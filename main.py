#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PERPETUAL DIRECIONAL — BTC / ETH / HYPE
=======================================

Identidade do robô:
  - Nome operacional: ASTER_PERPETUAL_DIRECIONAL
  - Estratégia ativa: PYRAMID 1% independente por símbolo e lado.
  - RANGE está permanentemente aposentado neste robô.

PYRAMID:
  - Seis estratégias independentes: BTC/ETH/HYPE x LONG/SHORT.
  - Uma única escada por símbolo/lado, sem subgrid cruzado com outra conta.
  - Anchor persistente por símbolo/lado.
  - Primeira entrada com notional configurado (padrão US$100).
  - Adições usam margem física livre real da conta, não crescimento do bankroll lógico.
  - Bankroll lógico padrão US$10 por estratégia.
  - Limite lógico de perda padrão US$10 por estratégia.
  - Leverage alvo padrão 10x e teto efetivo padrão 10x para novas adições.
    Posições antigas com leverage herdada maior continuam gerenciadas/protegidas,
    mas não recebem novas adições até o símbolo ficar flat e poder ser resetado.

Proteção:
  - STOP_MARKET nativa por estratégia/lado.
  - O stop efetivo considera tanto MAX_LOSS_USD quanto buffer antes da liquidação.
  - Proteção nativa é verificada em modo fail-closed.
  - Se uma nova adição não puder receber proteção confirmada, a cesta daquela
    estratégia é encerrada para não permanecer exposição aumentada e desprotegida.

Execução e consistência:
  - Hedge Mode obrigatório, margem ISOLATED, Single-Asset.
  - FillLedger SQLite + state.json + posição física da Aster são reconciliados em conjunto.
  - HTTP 503/timeout de transporte em envio de ordem é tratado como UNKNOWN e
    reconciliado por clientOrderId, sem reenvio cego.
  - RANGE legado existe apenas como compatibilidade de migração/retirada e nunca
    volta a abrir novas posições.

Persistência:
  - BOT_DIR/state.json
  - BOT_DIR/fill_ledger.sqlite3
  - BOT_DIR/trades.jsonl
  - BOT_DIR/order_journal.jsonl

Segurança:
  - LIVE_TRADING=0 por padrão.
  - SOFT kill bloqueia novas entradas e continua gerenciando posições.
  - HARD kill cancela ordens e tenta encerrar posições do robô.
  - Notícias de alto impacto podem bloquear entradas.
  - Nunca use seed phrase/chave privada da carteira principal; use somente a API Wallet.
"""


from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import queue
import re
import signal
import sqlite3
import uuid
import sys
import threading
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_DOWN, ROUND_UP, getcontext
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode

import requests
from eth_account import Account
from eth_account.messages import encode_typed_data

try:
    import websocket
except Exception:
    websocket = None

try:
    from bs4 import BeautifulSoup
except Exception:
    BeautifulSoup = None

getcontext().prec = 28
D = Decimal
UTC = timezone.utc

# -----------------------------------------------------------------------------
# CONFIG
# -----------------------------------------------------------------------------

VERSION = "7.9.0-v31-identity-normalized"
BOT_NAME = "ASTER_PERPETUAL_DIRECIONAL"
BASE_URL = os.getenv("ASTER_BASE_URL", "https://fapi.asterdex.com").rstrip("/")
WS_BASE = os.getenv("ASTER_WS_BASE", "wss://fstream.asterdex.com").rstrip("/")
USER_ADDRESS = os.getenv("ASTER_USER_ADDRESS", "").strip()
SIGNER_ADDRESS = os.getenv("ASTER_API_WALLET_ADDRESS", "").strip()
SIGNER_PRIVATE_KEY = os.getenv("ASTER_API_WALLET_PRIVATE_KEY", "").strip()
LIVE_TRADING = os.getenv("LIVE_TRADING", "0") == "1"
VALIDATE_API_ONLY = os.getenv("VALIDATE_API_ONLY", "0") == "1"
EMERGENCY_CLOSE_ALL_AND_RESET = os.getenv("EMERGENCY_CLOSE_ALL_AND_RESET", "0") == "1"
EMERGENCY_RESET_ID = os.getenv("EMERGENCY_RESET_ID", "reset-20260830-01").strip()
BOT_DIR = Path(os.getenv("BOT_DIR", "/data"))
BOT_DIR.mkdir(parents=True, exist_ok=True)
STATE_FILE = BOT_DIR / "state.json"
TRADES_FILE = BOT_DIR / "trades.jsonl"
NEWS_CACHE_FILE = BOT_DIR / "news_calendar_cache.json"
LOG_FILE = BOT_DIR / "aster_bot.log"
LEDGER_FILE = BOT_DIR / "fill_ledger.sqlite3"
ORDER_JOURNAL_FILE = BOT_DIR / "order_journal.jsonl"

SYMBOLS = tuple(s.strip().upper() for s in os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT,HYPEUSDT").split(",") if s.strip())
if not SYMBOLS:
    raise RuntimeError("SYMBOLS vazio ou inválido")

INITIAL_BANKROLL_USD = D(os.getenv("INITIAL_BANKROLL_USD", "10"))
BTC_INITIAL_BANKROLL_USD = D(os.getenv("BTC_INITIAL_BANKROLL_USD", "20"))
INITIAL_OPERATION_NOTIONAL_USD = D(os.getenv(
    "INITIAL_OPERATION_NOTIONAL_USD",
    os.getenv("INITIAL_OPERATION_MARGIN_USD", "10"),
))
BTC_INITIAL_OPERATION_NOTIONAL_USD = D(os.getenv("BTC_INITIAL_OPERATION_NOTIONAL_USD", "100"))
MAX_INITIAL_NOTIONAL_OVERSHOOT_PCT = D(os.getenv("MAX_INITIAL_NOTIONAL_OVERSHOOT_PCT", "0.05"))
RECOVERY_MULTIPLIER = D(os.getenv("RECOVERY_MULTIPLIER", "4"))
MAX_RECOVERY_FAILURES = int(os.getenv("MAX_RECOVERY_FAILURES", "2"))

MAX_REQUESTED_LEVERAGE = int(os.getenv("MAX_REQUESTED_LEVERAGE", "35"))
API_HARD_MAX_LEVERAGE = 125
BOT_HARD_MAX_LEVERAGE = 35
MIN_LEVERAGE = int(os.getenv("MIN_LEVERAGE", "1"))
LEVERAGE_HEADROOM = D(os.getenv("LEVERAGE_HEADROOM", "0.95"))
LIQUIDATION_BUFFER_PCT = D(os.getenv("LIQUIDATION_BUFFER_PCT", "0.005"))
ADVERSE_MOVE_SAFETY_MULTIPLIER = D(os.getenv("ADVERSE_MOVE_SAFETY_MULTIPLIER", "1.25"))
MIN_FREE_WALLET_BUFFER_USD = D(os.getenv("MIN_FREE_WALLET_BUFFER_USD", "1.00"))
MAX_MARGIN_FRACTION_PER_STRATEGY = D(os.getenv("MAX_MARGIN_FRACTION_PER_STRATEGY", "1.0"))

RANGE_SIGNAL_MODE = "VOLATILITY_ONLY"
RANGE_TRIGGER_PCT = D(os.getenv("RANGE_TRIGGER_PCT", "0.01"))
RANGE_TAKE_PROFIT_PCT = D(os.getenv("RANGE_TAKE_PROFIT_PCT", "0.01"))
RANGE_HARD_STOP_PCT = D(os.getenv("RANGE_HARD_STOP_PCT", "0.02"))
RANGE_REARM_PCT = D(os.getenv("RANGE_REARM_PCT", "0.03"))
RANGE_ENGINE_ENABLED = False  # RANGE definitivamente aposentado neste robô.

PROTECTIVE_WATCHDOG_SECONDS = float(os.getenv("PROTECTIVE_WATCHDOG_SECONDS", "5"))

# PYRAMID 1% engine: 2 robos independentes por ativo (LONG e SHORT).
PYRAMID_ENGINE_ENABLED = os.getenv("PYRAMID_ENGINE_ENABLED", "1") == "1"
RETIRE_LEGACY_RANGE_ON_STARTUP = os.getenv("RETIRE_LEGACY_RANGE_ON_STARTUP", "1") == "1"
PYRAMID_BANKROLL_USD = D(os.getenv("PYRAMID_BANKROLL_USD", "10"))
PYRAMID_INITIAL_NOTIONAL_USD = D(os.getenv("PYRAMID_INITIAL_NOTIONAL_USD", "100"))
PYRAMID_STEP_PCT = D(os.getenv("PYRAMID_STEP_PCT", "0.01"))
# The Directional robot is independent. Pyramid keeps its own operational anchor;
# it does not try to complete or coordinate the RANGE grid of another account.
PYRAMID_GRID_PHASES = tuple(
    D(x.strip()) for x in os.getenv("PYRAMID_GRID_PHASES", "0").split(",") if x.strip()
)
PYRAMID_GRID_COUNT = max(1, len(PYRAMID_GRID_PHASES))
PYRAMID_GRID_CAPITAL_SHARE = D(1) / D(PYRAMID_GRID_COUNT)
# Adicionais PYRAMID usam margem física livre da conta, não bankroll/equity virtual.
# Compatibilidade: se PYRAMID_ADD_FREE_MARGIN_PCT não existir, aceita o antigo
# PYRAMID_ADD_BANKROLL_PCT como fallback de configuração.
PYRAMID_ADD_FREE_MARGIN_PCT = D(os.getenv(
    "PYRAMID_ADD_FREE_MARGIN_PCT",
    os.getenv("PYRAMID_ADD_BANKROLL_PCT", "0.05"),
))
PYRAMID_ADD_BANKROLL_PCT = PYRAMID_ADD_FREE_MARGIN_PCT  # alias legado
PYRAMID_BTC_MIN_ADD_NOTIONAL_USD = D(os.getenv("PYRAMID_BTC_MIN_ADD_NOTIONAL_USD", "100"))
PYRAMID_LEVERAGE = int(os.getenv("PYRAMID_LEVERAGE", "10"))
PYRAMID_MAX_EFFECTIVE_LEVERAGE = int(os.getenv("PYRAMID_MAX_EFFECTIVE_LEVERAGE", str(PYRAMID_LEVERAGE)))
PYRAMID_MAX_LOSS_USD = D(os.getenv("PYRAMID_MAX_LOSS_USD", "10"))
PYRAMID_NATIVE_RISK_STOP = os.getenv("PYRAMID_NATIVE_RISK_STOP", "1") == "1"
PYRAMID_NATIVE_STOP_REFRESH_SECONDS = float(os.getenv("PYRAMID_NATIVE_STOP_REFRESH_SECONDS", "5"))
PYRAMID_MAX_LEVELS_PER_TICK = int(os.getenv("PYRAMID_MAX_LEVELS_PER_TICK", "20"))
PYRAMID_APPLY_NEWS_FILTER = os.getenv("PYRAMID_APPLY_NEWS_FILTER", "1") == "1"
PYRAMID_STOP_AFTER_MAX_LOSS = os.getenv("PYRAMID_STOP_AFTER_MAX_LOSS", "1") == "1"

RECV_WINDOW = int(os.getenv("RECV_WINDOW", "5000"))
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "10"))
ORDER_FILL_WAIT_SECONDS = float(os.getenv("ORDER_FILL_WAIT_SECONDS", "8"))
ORDER_POLL_SECONDS = float(os.getenv("ORDER_POLL_SECONDS", "0.4"))
MAIN_LOOP_SECONDS = float(os.getenv("MAIN_LOOP_SECONDS", "0.5"))
REST_PRICE_FALLBACK_SECONDS = float(os.getenv("REST_PRICE_FALLBACK_SECONDS", "5"))
HEARTBEAT_SECONDS = float(os.getenv("HEARTBEAT_SECONDS", "30"))
ACCOUNT_SYNC_SECONDS = float(os.getenv("ACCOUNT_SYNC_SECONDS", "10"))

ALLOW_MULTI_STRATEGY_SAME_SYMBOL = os.getenv("ALLOW_MULTI_STRATEGY_SAME_SYMBOL", "1") == "1"
NATIVE_PROTECTIVE_ORDERS = os.getenv("NATIVE_PROTECTIVE_ORDERS", "1") == "1"
PROTECTIVE_WORKING_TYPE = os.getenv("PROTECTIVE_WORKING_TYPE", "MARK_PRICE").strip().upper()
PROTECTIVE_PRICE_PROTECT = os.getenv("PROTECTIVE_PRICE_PROTECT", "0") == "1"

NEWS_FILTER_ENABLED = os.getenv("NEWS_FILTER_ENABLED", "1") == "1"
NEWS_FAIL_CLOSED = os.getenv("NEWS_FAIL_CLOSED", "1") == "1"
NEWS_WINDOW_BEFORE_MIN = int(os.getenv("NEWS_WINDOW_BEFORE_MIN", "15"))
NEWS_WINDOW_AFTER_MIN = int(os.getenv("NEWS_WINDOW_AFTER_MIN", "15"))
NEWS_REFRESH_SECONDS = int(os.getenv("NEWS_REFRESH_SECONDS", "900"))
NEWS_MAX_STALE_SECONDS = int(os.getenv("NEWS_MAX_STALE_SECONDS", "3600"))
NEWS_LOOKAHEAD_DAYS = int(os.getenv("NEWS_LOOKAHEAD_DAYS", "7"))
NEWS_MANUAL_EVENTS_UTC = os.getenv("NEWS_MANUAL_EVENTS_UTC", "").strip()

KILL_SWITCH_ON_API_ERRORS = int(os.getenv("KILL_SWITCH_ON_API_ERRORS", "8"))
HARD_KILL_ON_POSITION_MISMATCH = os.getenv("HARD_KILL_ON_POSITION_MISMATCH", "0") == "1"

MAX_RECOVERY_NOTIONAL_USD = D(os.getenv("MAX_RECOVERY_NOTIONAL_USD", "160"))
BTC_MAX_RECOVERY_NOTIONAL_USD = D(os.getenv("BTC_MAX_RECOVERY_NOTIONAL_USD", "1600"))
MAX_TOTAL_SYMBOL_NOTIONAL_USD = D(os.getenv("MAX_TOTAL_SYMBOL_NOTIONAL_USD", "300"))
BTC_MAX_TOTAL_SYMBOL_NOTIONAL_USD = D(os.getenv("BTC_MAX_TOTAL_SYMBOL_NOTIONAL_USD", "2500"))
MAX_PRICE_AGE_FOR_ENTRY_SECONDS = float(os.getenv("MAX_PRICE_AGE_FOR_ENTRY_SECONDS", "4"))
RECONCILE_INTERVAL_SECONDS = float(os.getenv("RECONCILE_INTERVAL_SECONDS", "10"))
STATE_LEDGER_MISMATCH_CONFIRMATIONS = int(os.getenv("STATE_LEDGER_MISMATCH_CONFIRMATIONS", "2"))
UNKNOWN_ORDER_QUERY_ATTEMPTS = int(os.getenv("UNKNOWN_ORDER_QUERY_ATTEMPTS", "12"))
UNKNOWN_ORDER_QUERY_DELAY_SECONDS = float(os.getenv("UNKNOWN_ORDER_QUERY_DELAY_SECONDS", "0.5"))
LEDGER_RECONCILE_ON_STARTUP = os.getenv("LEDGER_RECONCILE_ON_STARTUP", "1") == "1"
SELF_TEST_ON_STARTUP = os.getenv("SELF_TEST_ON_STARTUP", "1") == "1"

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# -----------------------------------------------------------------------------
# LOGGING
# -----------------------------------------------------------------------------

logger = logging.getLogger(BOT_NAME)
logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
sh = logging.StreamHandler(sys.stdout)
sh.setFormatter(formatter)
logger.addHandler(sh)
try:
    fh = logging.FileHandler(LOG_FILE)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
except Exception:
    pass

# -----------------------------------------------------------------------------
# HELPERS
# -----------------------------------------------------------------------------

def now_ms() -> int:
    return int(time.time() * 1000)

def now_iso() -> str:
    return datetime.now(UTC).isoformat()

def dec(x: Any, default: str = "0") -> Decimal:
    try:
        return D(str(x))
    except Exception:
        return D(default)

def dstr(x: Decimal, places: int = 8) -> str:
    q = D(10) ** -places
    s = format(x.quantize(q), "f")
    return s.rstrip("0").rstrip(".") if "." in s else s

def atomic_json_write(path: Path, data: Dict[str, Any]) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, path)

def jsonl_append(path: Path, obj: Dict[str, Any]) -> None:
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")

def floor_step(value: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        return value
    return (value / step).to_integral_value(rounding=ROUND_DOWN) * step

def ceil_step(value: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        return value
    return (value / step).to_integral_value(rounding=ROUND_UP) * step

def pct_change(a: Decimal, b: Decimal) -> Decimal:
    if a == 0:
        return D(0)
    return (b / a) - D(1)

def ema(values: List[Decimal], period: int) -> List[Decimal]:
    if len(values) < period:
        return []
    k = D(2) / D(period + 1)
    out = [sum(values[:period]) / D(period)]
    for v in values[period:]:
        out.append(v * k + out[-1] * (D(1) - k))
    return out


# -----------------------------------------------------------------------------
# ASTER REST CLIENT
# -----------------------------------------------------------------------------

class AsterAPIError(RuntimeError):
    def __init__(self, message: str, code: Optional[int] = None, payload: Any = None):
        super().__init__(message)
        self.code = code
        self.payload = payload

class AsterClient:
    def __init__(self, user_address: str, signer_address: str, signer_private_key: str):
        self.user_address = user_address
        self.signer_address = signer_address
        self.signer_private_key = signer_private_key
        if self.signer_private_key:
            derived = Account.from_key(self.signer_private_key).address
            if self.signer_address and derived.lower() != self.signer_address.lower():
                raise AsterAPIError(
                    f"ASTER_API_WALLET_PRIVATE_KEY nao corresponde a ASTER_API_WALLET_ADDRESS "
                    f"(derivado={derived})"
                )
        self.s = requests.Session()
        self.s.headers.update({"User-Agent": f"{BOT_NAME}/{VERSION}"})
        self.time_offset_ms = 0
        self.api_error_streak = 0
        self._lock = threading.Lock()
        self._last_nonce = 0

    def _ts(self) -> int:
        return now_ms() + self.time_offset_ms

    def _nonce(self) -> int:
        with self._lock:
            candidate = self._ts() * 1000
            self._last_nonce = max(candidate, self._last_nonce + 1)
            return self._last_nonce

    def sync_time(self) -> None:
        t0 = now_ms()
        r = self.s.get(BASE_URL + "/fapi/v3/time", timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        server = int(r.json()["serverTime"])
        t1 = now_ms()
        midpoint = (t0 + t1) // 2
        self.time_offset_ms = server - midpoint
        logger.info(f"TIME SYNC | offset_ms={self.time_offset_ms}")

    def _request(self, method: str, path: str, params: Optional[Dict[str, Any]] = None,
                 signed: bool = False, api_key_only: bool = False, retry_unknown: bool = False) -> Any:
        params = dict(params or {})
        if signed:
            if not self.user_address or not self.signer_address or not self.signer_private_key:
                raise AsterAPIError("Credenciais da API Wallet V3 ausentes")
            params["nonce"] = self._nonce()
            params["signer"] = self.signer_address
            qs = urlencode([(k, str(v).lower() if isinstance(v, bool) else str(v)) for k, v in params.items()])
            typed_data = {"types": {"EIP712Domain": [{"name": "name", "type": "string"}, {"name": "version", "type": "string"}, {"name": "chainId", "type": "uint256"}, {"name": "verifyingContract", "type": "address"}], "Message": [{"name": "msg", "type": "string"}]}, "primaryType": "Message", "domain": {"name": "AsterSignTransaction", "version": "1", "chainId": 1666, "verifyingContract": "0x0000000000000000000000000000000000000000"}, "message": {"msg": qs}}
            signable = encode_typed_data(full_message=typed_data)
            params["signature"] = Account.sign_message(signable, private_key=self.signer_private_key).signature.hex()
        url = BASE_URL + path
        try:
            r = self.s.request(method, url, params=params, timeout=HTTP_TIMEOUT)
            if r.status_code == 503 and not retry_unknown:
                raise AsterAPIError("HTTP 503: status de execucao desconhecido; reconciliar por clientOrderId", 503, r.text)
            if r.status_code >= 400:
                try:
                    body = r.json()
                    code = body.get("code") if isinstance(body, dict) else None
                    msg = body.get("msg", r.text) if isinstance(body, dict) else r.text
                except Exception:
                    code, msg, body = None, r.text, r.text
                raise AsterAPIError(f"HTTP {r.status_code} | {msg}", code, body)
            self.api_error_streak = 0
            return r.json() if r.text else {}
        except AsterAPIError:
            self.api_error_streak += 1
            raise
        except (requests.Timeout, requests.ConnectionError) as e:
            self.api_error_streak += 1
            if method.upper() == "POST" and path == "/fapi/v3/order":
                raise AsterAPIError(
                    f"TRANSPORT UNKNOWN EXECUTION | {type(e).__name__}: {e}",
                    503,
                    {"path": path, "method": method},
                ) from e
            raise AsterAPIError(str(e)) from e
        except Exception as e:
            self.api_error_streak += 1
            raise AsterAPIError(str(e)) from e

    def exchange_info(self) -> Dict[str, Any]:
        return self._request("GET", "/fapi/v3/exchangeInfo")

    def price(self, symbol: str) -> Decimal:
        x = self._request("GET", "/fapi/v3/ticker/price", {"symbol": symbol})
        return dec(x.get("price"))

    def mark(self, symbol: str) -> Decimal:
        x = self._request("GET", "/fapi/v3/premiumIndex", {"symbol": symbol})
        return dec(x.get("markPrice") or x.get("price"))

    def klines(self, symbol: str, interval: str, limit: int = 100) -> List[List[Any]]:
        return self._request("GET", "/fapi/v3/klines", {"symbol": symbol, "interval": interval, "limit": limit})

    def position_mode(self) -> bool:
        x = self._request("GET", "/fapi/v3/positionSide/dual", signed=True)
        return bool(x.get("dualSidePosition"))

    def multi_assets_mode(self) -> bool:
        x = self._request("GET", "/fapi/v3/multiAssetsMargin", signed=True)
        return bool(x.get("multiAssetsMargin"))

    def set_single_asset_mode(self) -> None:
        if not self.multi_assets_mode():
            return
        try:
            self._request("POST", "/fapi/v3/multiAssetsMargin",
                          {"multiAssetsMargin": "false"}, signed=True)
        except AsterAPIError as e:
            raise RuntimeError(
                "Aster esta em Multi-Assets Mode e nao permitiu mudar automaticamente para "
                "Single-Asset Mode. Cancele ordens e feche posicoes manuais na conta/subconta, "
                "desative Multi-Assets Mode na interface Aster e faca novo deploy. "
                f"Erro original: {e}"
            ) from e
        if self.multi_assets_mode():
            raise RuntimeError("Aster continuou em Multi-Assets Mode apos a solicitacao de desativacao")

    def set_hedge_mode(self) -> None:
        try:
            self._request("POST", "/fapi/v3/positionSide/dual", {"dualSidePosition": "true"}, signed=True)
        except AsterAPIError as e:
            if e.code not in (-4059,):
                raise

    def set_margin_type(self, symbol: str, isolated: bool = True) -> None:
        try:
            self._request("POST", "/fapi/v3/marginType",
                          {"symbol": symbol, "marginType": "ISOLATED" if isolated else "CROSSED"}, signed=True)
        except AsterAPIError as e:
            if e.code not in (-4046,):
                raise

    def set_leverage(self, symbol: str, leverage: int) -> Dict[str, Any]:
        return self._request("POST", "/fapi/v3/leverage", {"symbol": symbol, "leverage": int(leverage)}, signed=True)

    def leverage_bracket(self, symbol: str) -> Any:
        return self._request("GET", "/fapi/v3/leverageBracket", {"symbol": symbol}, signed=True)

    def balance(self) -> Any:
        return self._request("GET", "/fapi/v3/balance", signed=True)

    def account(self) -> Any:
        return self._request("GET", "/fapi/v3/accountWithJoinMargin", signed=True)

    def positions(self, symbol: Optional[str] = None) -> Any:
        p = {"symbol": symbol} if symbol else {}
        return self._request("GET", "/fapi/v3/positionRisk", p, signed=True)

    def open_orders(self, symbol: Optional[str] = None) -> Any:
        p = {"symbol": symbol} if symbol else {}
        return self._request("GET", "/fapi/v3/openOrders", p, signed=True)

    def query_order(self, symbol: str, client_id: str) -> Dict[str, Any]:
        return self._request("GET", "/fapi/v3/order", {"symbol": symbol, "origClientOrderId": client_id}, signed=True)

    def order(self, symbol: str, side: str, position_side: str, quantity: Decimal,
              client_id: str, order_type: str = "MARKET") -> Dict[str, Any]:
        p = {
            "symbol": symbol,
            "side": side,
            "positionSide": position_side,
            "type": order_type,
            "quantity": dstr(quantity, 12),
            "newClientOrderId": client_id[:36],
            "newOrderRespType": "RESULT",
        }
        try:
            return self._request("POST", "/fapi/v3/order", p, signed=True)
        except AsterAPIError as e:
            if e.code == 503:
                for _ in range(max(1, UNKNOWN_ORDER_QUERY_ATTEMPTS)):
                    time.sleep(max(0.05, UNKNOWN_ORDER_QUERY_DELAY_SECONDS))
                    try:
                        return self.query_order(symbol, client_id)
                    except Exception:
                        continue
            raise

    def conditional_order(self, symbol: str, side: str, position_side: str, quantity: Decimal,
                          stop_price: Decimal, client_id: str, order_type: str,
                          working_type: str = "MARK_PRICE", price_protect: bool = False) -> Dict[str, Any]:
        if order_type not in ("STOP_MARKET", "TAKE_PROFIT_MARKET"):
            raise ValueError(f"Tipo condicional invalido: {order_type}")
        p = {
            "symbol": symbol,
            "side": side,
            "positionSide": position_side,
            "type": order_type,
            "quantity": dstr(quantity, 12),
            "stopPrice": dstr(stop_price, 12),
            "workingType": working_type,
            "priceProtect": "TRUE" if price_protect else "FALSE",
            "newClientOrderId": client_id[:36],
            "newOrderRespType": "RESULT",
        }
        try:
            return self._request("POST", "/fapi/v3/order", p, signed=True)
        except AsterAPIError as e:
            if e.code == 503:
                for _ in range(max(1, UNKNOWN_ORDER_QUERY_ATTEMPTS)):
                    time.sleep(max(0.05, UNKNOWN_ORDER_QUERY_DELAY_SECONDS))
                    try:
                        return self.query_order(symbol, client_id)
                    except Exception:
                        continue
            raise

    def cancel_order(self, symbol: str, client_id: str) -> Any:
        try:
            return self._request("DELETE", "/fapi/v3/order",
                                 {"symbol": symbol, "origClientOrderId": client_id}, signed=True)
        except AsterAPIError as e:
            if e.code in (-2011, -2013):
                return {"status": "UNKNOWN_OR_GONE", "clientOrderId": client_id}
            raise

    def cancel_all(self, symbol: str) -> Any:
        return self._request("DELETE", "/fapi/v3/allOpenOrders", {"symbol": symbol}, signed=True)

    def income(self, symbol: Optional[str] = None, start_ms: Optional[int] = None, limit: int = 1000) -> Any:
        p: Dict[str, Any] = {"limit": limit}
        if symbol:
            p["symbol"] = symbol
        if start_ms:
            p["startTime"] = start_ms
        return self._request("GET", "/fapi/v3/income", p, signed=True)

    def user_trades(self, symbol: str, start_ms: Optional[int] = None, end_ms: Optional[int] = None,
                    limit: int = 1000) -> Any:
        p: Dict[str, Any] = {"symbol": symbol, "limit": limit}
        if start_ms is not None:
            p["startTime"] = int(start_ms)
        if end_ms is not None:
            p["endTime"] = int(end_ms)
        return self._request("GET", "/fapi/v3/userTrades", p, signed=True)

# -----------------------------------------------------------------------------
# EXCHANGE SYMBOL RULES
# -----------------------------------------------------------------------------

@dataclass
class SymbolRules:
    symbol: str
    tick_size: Decimal
    step_size: Decimal
    min_qty: Decimal
    max_qty: Decimal
    min_notional: Decimal

class RulesBook:
    def __init__(self, client: AsterClient):
        self.client = client
        self.rules: Dict[str, SymbolRules] = {}

    def refresh(self) -> None:
        info = self.client.exchange_info()
        out: Dict[str, SymbolRules] = {}
        for s in info.get("symbols", []):
            sym = str(s.get("symbol", "")).upper()
            if sym not in SYMBOLS:
                continue
            tick = step = min_qty = min_notional = D(0)
            max_qty = D("1e50")
            for f in s.get("filters", []):
                ft = f.get("filterType")
                if ft == "PRICE_FILTER":
                    tick = dec(f.get("tickSize"))
                elif ft in ("LOT_SIZE", "MARKET_LOT_SIZE"):
                    st = dec(f.get("stepSize"))
                    mn = dec(f.get("minQty"))
                    mx = dec(f.get("maxQty"), "1e50")
                    if st > step:
                        step = st
                    if mn > min_qty:
                        min_qty = mn
                    if mx < max_qty:
                        max_qty = mx
                elif ft in ("MIN_NOTIONAL", "NOTIONAL"):
                    min_notional = max(min_notional, dec(f.get("notional") or f.get("minNotional")))
            out[sym] = SymbolRules(sym, tick or D("0.00000001"), step or D("0.00000001"),
                                   min_qty, max_qty, min_notional)
        missing = [s for s in SYMBOLS if s not in out]
        if missing:
            raise RuntimeError(f"Simbolos nao disponiveis na Aster: {missing}")
        self.rules = out
        for r in out.values():
            logger.info(f"RULES | {r.symbol} | tick={r.tick_size} step={r.step_size} min_qty={r.min_qty} min_notional={r.min_notional}")

    def qty(self, symbol: str, raw: Decimal, price: Decimal) -> Decimal:
        r = self.rules[symbol]
        q = floor_step(raw, r.step_size)
        if q < r.min_qty:
            q = ceil_step(r.min_qty, r.step_size)
        if r.min_notional > 0 and q * price < r.min_notional:
            q = ceil_step(r.min_notional / price, r.step_size)
        if q > r.max_qty:
            raise RuntimeError(f"Quantidade acima maxQty {symbol}: {q}>{r.max_qty}")
        return q

    def trigger_price(self, symbol: str, raw: Decimal, direction: str) -> Decimal:
        r = self.rules[symbol]
        if direction == "UP":
            return ceil_step(raw, r.tick_size)
        if direction == "DOWN":
            return floor_step(raw, r.tick_size)
        return floor_step(raw, r.tick_size)

# -----------------------------------------------------------------------------
# MARKET DATA WEBSOCKET + REST FALLBACK
# -----------------------------------------------------------------------------

class MarketData:
    def __init__(self, client: AsterClient):
        self.client = client
        self.prices: Dict[str, Decimal] = {}
        self.price_ts: Dict[str, float] = {}
        self._lock = threading.Lock()
        self.stop = threading.Event()
        self.ws_thread: Optional[threading.Thread] = None
        self.rest_thread: Optional[threading.Thread] = None

    def start(self) -> None:
        if websocket is not None:
            self.ws_thread = threading.Thread(target=self._ws_loop, name="market-ws", daemon=True)
            self.ws_thread.start()
        else:
            logger.warning("websocket-client ausente; usando REST fallback")
        self.rest_thread = threading.Thread(target=self._rest_loop, name="market-rest", daemon=True)
        self.rest_thread.start()

    def age(self, symbol: str) -> float:
        with self._lock:
            ts = self.price_ts.get(symbol, 0)
        return max(0.0, time.time() - ts) if ts else float("inf")

    def is_fresh(self, symbol: str, max_age: float = MAX_PRICE_AGE_FOR_ENTRY_SECONDS) -> bool:
        return self.age(symbol) <= max_age

    def get(self, symbol: str, max_age: float = 4.0) -> Optional[Decimal]:
        with self._lock:
            p = self.prices.get(symbol)
            ts = self.price_ts.get(symbol, 0)
        if p is not None and time.time() - ts <= max_age:
            return p
        try:
            fresh = self.client.price(symbol)
            self._set(symbol, fresh)
            return fresh
        except Exception as e:
            age = max(0.0, time.time() - ts) if ts else float("inf")
            logger.warning(f"PRICE FALLBACK FAIL | {symbol} | age_s={age:.3f} | {e}")
            return None if age > max_age else p

    def _set(self, symbol: str, price: Decimal) -> None:
        if price <= 0:
            return
        with self._lock:
            self.prices[symbol] = price
            self.price_ts[symbol] = time.time()

    def _ws_loop(self) -> None:
        streams = "/".join(f"{s.lower()}@miniTicker" for s in SYMBOLS)
        url = f"{WS_BASE}/stream?streams={streams}"
        while not self.stop.is_set():
            try:
                def on_message(ws, message):
                    try:
                        j = json.loads(message)
                        data = j.get("data", j)
                        sym = str(data.get("s", "")).upper()
                        p = dec(data.get("c"))
                        if sym in SYMBOLS and p > 0:
                            self._set(sym, p)
                    except Exception:
                        pass

                def on_open(ws):
                    logger.info(f"MARKET WS | CONECTADO | {url}")

                def on_error(ws, error):
                    logger.warning(f"MARKET WS | erro={error}")

                def on_close(ws, code, msg):
                    logger.warning(f"MARKET WS | fechado code={code} msg={msg}")

                app = websocket.WebSocketApp(url, on_open=on_open, on_message=on_message,
                                             on_error=on_error, on_close=on_close)
                app.run_forever(ping_interval=120, ping_timeout=30)
            except Exception as e:
                logger.warning(f"MARKET WS LOOP | {e}")
            if self.stop.is_set():
                break
            self.stop.wait(3)

    def _rest_loop(self) -> None:
        while not self.stop.wait(REST_PRICE_FALLBACK_SECONDS):
            if self.stop.is_set():
                break
            for sym in SYMBOLS:
                with self._lock:
                    age = time.time() - self.price_ts.get(sym, 0)
                if age < REST_PRICE_FALLBACK_SECONDS:
                    continue
                try:
                    self._set(sym, self.client.price(sym))
                except Exception as e:
                    logger.warning(f"REST PRICE | {sym} | {e}")

# -----------------------------------------------------------------------------
# NEWS FILTER
# -----------------------------------------------------------------------------

class NewsFilter:
    def __init__(self):
        self.events: List[Dict[str, Any]] = []
        self.last_refresh = 0.0
        self.last_success = 0.0
        self.last_source = "NONE"
        self.stop = threading.Event()
        self.thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        self._load_cache()
        self._load_manual()

    def _load_cache(self) -> None:
        try:
            j = json.loads(NEWS_CACHE_FILE.read_text(encoding="utf-8"))
            self.events = j.get("events", [])
            self.last_success = float(j.get("last_success", 0))
            self.last_source = str(j.get("source", "CACHE"))
        except Exception:
            pass

    def _load_manual(self) -> None:
        if not NEWS_MANUAL_EVENTS_UTC:
            return
        manual = []
        for item in NEWS_MANUAL_EVENTS_UTC.split(";"):
            if not item.strip():
                continue
            parts = item.split("|", 1)
            try:
                dt = datetime.fromisoformat(parts[0].replace("Z", "+00:00")).astimezone(UTC)
                manual.append({"ts": dt.timestamp(), "title": parts[1] if len(parts) > 1 else "MANUAL", "source": "MANUAL"})
            except Exception:
                continue
        if manual:
            self.events.extend(manual)

    def start(self) -> None:
        if not NEWS_FILTER_ENABLED:
            return
        self.thread = threading.Thread(target=self._loop, name="news", daemon=True)
        self.thread.start()

    def _loop(self) -> None:
        while not self.stop.is_set():
            try:
                self.refresh()
            except Exception as e:
                logger.warning(f"NEWS | refresh falhou | {e}")
            self.stop.wait(NEWS_REFRESH_SECONDS)

    def refresh(self) -> None:
        self.last_refresh = time.time()
        source = "INVESTING_3STAR"
        try:
            events = self._fetch_investing()
        except Exception as investing_error:
            logger.warning(f"NEWS | Investing indisponivel | {investing_error} | tentando ForexFactory")
            events = self._fetch_forexfactory()
            source = "FOREXFACTORY_HIGH"
        with self._lock:
            manual = [e for e in self.events if e.get("source") == "MANUAL"]
            self.events = events + manual
            self.last_success = time.time()
            self.last_source = source
            atomic_json_write(NEWS_CACHE_FILE, {
                "last_success": self.last_success,
                "source": self.last_source,
                "events": self.events,
            })
        logger.info(f"NEWS | cache atualizado | fonte={source} | eventos_high={len(events)}")

    def _parse_investing_rows(self, html_text: str) -> List[Dict[str, Any]]:
        if BeautifulSoup is None:
            raise RuntimeError("beautifulsoup4 ausente")
        soup = BeautifulSoup(html_text or "", "html.parser")
        events: List[Dict[str, Any]] = []
        now = datetime.now(UTC)
        horizon = now + timedelta(days=NEWS_LOOKAHEAD_DAYS)
        rows = soup.find_all("tr", attrs={"data-event-datetime": True})
        for row in rows:
            txt = " ".join(row.stripped_strings)
            row_html = str(row)[:8000]
            high = bool(re.search(r"bull3|High Volatility Expected|sentiment[-_ ]?3|importance[^>]*3", row_html, re.I))
            if not high:
                continue
            raw_dt = row.get("data-event-datetime")
            if not raw_dt:
                continue
            dt = self._parse_investing_dt(str(raw_dt))
            if not dt or dt < now - timedelta(hours=2) or dt > horizon:
                continue
            event_cell = row.find("td", class_=lambda c: c and "event" in (c if isinstance(c, list) else str(c)).split())
            title = " ".join(event_cell.stripped_strings)[:240] if event_cell else txt[:240]
            events.append({"ts": dt.timestamp(), "title": title, "source": "INVESTING_3STAR"})
        unique = {(round(float(e["ts"])), e["title"][:80]): e for e in events}
        return sorted(unique.values(), key=lambda x: x["ts"])

    def _fetch_investing(self) -> List[Dict[str, Any]]:
        if BeautifulSoup is None:
            raise RuntimeError("beautifulsoup4 ausente")
        base = "https://www.investing.com"
        calendar_url = base + "/economic-calendar/"
        service_url = base + "/economic-calendar/Service/getCalendarFilteredData"
        ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/152.0.0.0 Safari/537.36"
        common = {
            "User-Agent": ua,
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Connection": "keep-alive",
        }
        ajax = dict(common)
        ajax.update({
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "X-Requested-With": "XMLHttpRequest",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Referer": calendar_url,
            "Origin": base,
        })

        today = datetime.now(UTC).date()
        end = today + timedelta(days=NEWS_LOOKAHEAD_DAYS)
        form = [
            ("importance[]", "3"),
            ("timeZone", "55"),
            ("timeFilter", "timeOnly"),
            ("currentTab", "custom"),
            ("limit_from", "0"),
            ("dateFrom", today.isoformat()),
            ("dateTo", end.isoformat()),
        ]
        errors: List[str] = []

        with requests.Session() as sess:
            sess.headers.update(common)
            try:
                warm = sess.get(calendar_url, timeout=20, allow_redirects=True)
                warm.raise_for_status()
                direct_events = self._parse_investing_rows(warm.text)
            except Exception as e:
                direct_events = []
                errors.append(f"warmup={type(e).__name__}:{e}")

            for attempt in range(1, 4):
                try:
                    r = sess.post(service_url, headers=ajax, data=form, timeout=25, allow_redirects=True)
                    if r.status_code in (403, 429) or r.status_code >= 500:
                        raise RuntimeError(f"HTTP {r.status_code}")
                    r.raise_for_status()
                    payload = r.json()
                    if not isinstance(payload, dict) or "data" not in payload:
                        raise RuntimeError("JSON sem campo data")
                    events = self._parse_investing_rows(str(payload.get("data", "")))
                    if events:
                        logger.info(f"NEWS INVESTING | service OK | tentativa={attempt} | eventos_3star={len(events)}")
                        return events
                    if direct_events:
                        logger.info(f"NEWS INVESTING | service vazio, usando pagina direta | eventos_3star={len(direct_events)}")
                        return direct_events
                    logger.info("NEWS INVESTING | service OK | nenhum evento 3-star no horizonte")
                    return []
                except Exception as e:
                    errors.append(f"service#{attempt}={type(e).__name__}:{e}")
                    if attempt < 3:
                        time.sleep(1.5 * attempt)
                        try:
                            sess.get(calendar_url, timeout=15, allow_redirects=True)
                        except Exception:
                            pass

            if direct_events:
                logger.warning(f"NEWS INVESTING | service falhou, pagina direta OK | eventos_3star={len(direct_events)}")
                return direct_events

        raise RuntimeError("Investing indisponivel apos retries: " + " | ".join(errors[-5:]))

    def _fetch_forexfactory(self) -> List[Dict[str, Any]]:
        url = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
        headers = {"User-Agent": f"{BOT_NAME}/{VERSION}", "Accept": "application/json"}
        r = requests.get(url, headers=headers, timeout=20)
        r.raise_for_status()
        payload = r.json()
        if not isinstance(payload, list):
            raise RuntimeError("resposta inesperada do calendario ForexFactory")
        now = datetime.now(UTC)
        horizon = now + timedelta(days=NEWS_LOOKAHEAD_DAYS)
        events: List[Dict[str, Any]] = []
        for item in payload:
            if str(item.get("impact", "")).strip().lower() != "high":
                continue
            try:
                dt = datetime.fromisoformat(str(item.get("date", "")).replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=UTC)
                dt = dt.astimezone(UTC)
            except Exception:
                continue
            if dt < now - timedelta(hours=2) or dt > horizon:
                continue
            title = f"{item.get('country', '')} | {item.get('title', 'High-impact event')}"[:240]
            events.append({"ts": dt.timestamp(), "title": title, "source": "FOREXFACTORY_HIGH"})
        unique = {(round(float(e["ts"])), e["title"][:80]): e for e in events}
        return sorted(unique.values(), key=lambda x: x["ts"])

    @staticmethod
    def _parse_investing_dt(raw: str) -> Optional[datetime]:
        raw = raw.strip()
        fmts = ["%Y/%m/%d %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S%z"]
        for fmt in fmts:
            try:
                dt = datetime.strptime(raw, fmt)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=UTC)
                return dt.astimezone(UTC)
            except Exception:
                pass
        try:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=UTC)
            return dt.astimezone(UTC)
        except Exception:
            return None

    def blocked(self, when: Optional[datetime] = None) -> Tuple[bool, Optional[str]]:
        if not NEWS_FILTER_ENABLED:
            return False, None
        when = when or datetime.now(UTC)
        with self._lock:
            events = list(self.events)
            last_success = self.last_success
        stale = (time.time() - last_success) > NEWS_MAX_STALE_SECONDS if last_success else True
        if stale and NEWS_FAIL_CLOSED:
            return True, "NEWS_CACHE_STALE_FAIL_CLOSED"
        ts = when.timestamp()
        before = NEWS_WINDOW_BEFORE_MIN * 60
        after = NEWS_WINDOW_AFTER_MIN * 60
        for e in events:
            et = float(e.get("ts", 0))
            if et - before <= ts <= et + after:
                return True, f"{e.get('source')} | {e.get('title')}"
        return False, None

# -----------------------------------------------------------------------------
# STATE
# -----------------------------------------------------------------------------

def configured_bankroll(symbol: str) -> Decimal:
    return BTC_INITIAL_BANKROLL_USD if symbol.upper() == "BTCUSDT" else INITIAL_BANKROLL_USD

def configured_initial_notional(symbol: str) -> Decimal:
    return BTC_INITIAL_OPERATION_NOTIONAL_USD if symbol.upper() == "BTCUSDT" else INITIAL_OPERATION_NOTIONAL_USD

def configured_max_recovery_notional(symbol: str) -> Decimal:
    return BTC_MAX_RECOVERY_NOTIONAL_USD if symbol.upper() == "BTCUSDT" else MAX_RECOVERY_NOTIONAL_USD

def configured_max_total_symbol_notional(symbol: str) -> Decimal:
    return BTC_MAX_TOTAL_SYMBOL_NOTIONAL_USD if symbol.upper() == "BTCUSDT" else MAX_TOTAL_SYMBOL_NOTIONAL_USD

def empty_range_state(symbol: str) -> Dict[str, Any]:
    bankroll = configured_bankroll(symbol)
    return {
        "strategy": f"RANGE:{symbol}",
        "symbol": symbol,
        "equity": str(bankroll),
        "bankroll_config_base": str(bankroll),
        "anchor": None,
        "status": "IDLE",
        "basket": None,
        "recovery_deficit": "0",
        "failures": 0,
        "protect_anchor": None,
        "wins": 0,
        "losses": 0,
        "realized_pnl": "0",
        "last_result": "NONE",
        "last_update": now_iso(),
    }


def empty_pyramid_state(symbol: str, side: str) -> Dict[str, Any]:
    side = str(side).upper()
    return {
        "strategy": f"PYRAMID:{symbol}:{side}",
        "symbol": symbol,
        "side": side,
        "grid_id": "LEGACY",
        "grid_phase": "0",
        "capital_share": "1",
        "bankroll": str(PYRAMID_BANKROLL_USD),
        "equity": str(PYRAMID_BANKROLL_USD),
        "anchor": None,
        "next_level": 1,
        "legs": [],
        "stopped": False,
        "stop_reason": None,
        "realized_pnl": "0",
        "last_unrealized": "0",
        "last_net_pnl": "0",
        "levels_filled": 0,
        "last_trigger_price": None,
        "native_risk_stop": None,
        "last_update": now_iso(),
    }

def empty_pyramid_grid_state(symbol: str, side: str, grid_id: str, phase: Decimal) -> Dict[str, Any]:
    side = str(side).upper()
    share = PYRAMID_GRID_CAPITAL_SHARE
    bankroll = PYRAMID_BANKROLL_USD * share
    return {
        "strategy": f"PYRAMID:{symbol}:{side}:{grid_id}",
        "symbol": symbol,
        "side": side,
        "grid_id": grid_id,
        "grid_phase": str(phase),
        "capital_share": str(share),
        "bankroll": str(bankroll),
        "equity": str(bankroll),
        "anchor": None,
        "next_level": 1,
        "legs": [],
        "stopped": False,
        "stop_reason": None,
        "realized_pnl": "0",
        "last_unrealized": "0",
        "last_net_pnl": "0",
        "levels_filled": 0,
        "last_trigger_price": None,
        "native_risk_stop": None,
        "last_update": now_iso(),
    }


def fresh_state() -> Dict[str, Any]:
    return {
        "version": VERSION,
        "created_at": now_iso(),
        "updated_at": now_iso(),
        "kill_switch": {"mode": "OFF", "reason": None, "at": None},
        "trade_gate": {"open_allowed": True, "reason": None, "at": now_iso()},
        "protection_blocks": {},
        "range": {s: empty_range_state(s) for s in SYMBOLS},
        "pyramid": {f"{s}:{side}": empty_pyramid_state(s, side) for s in SYMBOLS for side in ("LONG", "SHORT")},
        # Bucket mantido apenas para compatibilidade com estados antigos; novos G0 não são criados.
        "pyramid_grids": {},
        "symbol_owner": {s: None for s in SYMBOLS},
        "last_wallet": {},
        "maintenance": {"completed_emergency_actions": []},
    }

class StateStore:
    def __init__(self):
        self.lock = threading.RLock()
        self.state = self._load()

    def _load(self) -> Dict[str, Any]:
        try:
            st = json.loads(STATE_FILE.read_text(encoding="utf-8"))
            logger.info(f"STATE | carregado | {STATE_FILE}")
        except Exception:
            st = fresh_state()
            logger.info("STATE | novo")
        st.setdefault("kill_switch", {"mode": "OFF", "reason": None, "at": None})
        st.setdefault("trade_gate", {"open_allowed": True, "reason": None, "at": now_iso()})
        st.setdefault("protection_blocks", {})
        st.setdefault("range", {})
        st.setdefault("pyramid", {})
        st.setdefault("pyramid_grids", {})
        st.setdefault("symbol_owner", {})
        st.setdefault("last_wallet", {})
        st.setdefault("maintenance", {"completed_emergency_actions": []})
        for s in SYMBOLS:
            st["range"].setdefault(s, empty_range_state(s))
            st["symbol_owner"].setdefault(s, None)
            for side in ("LONG", "SHORT"):
                pkey = f"{s}:{side}"
                st["pyramid"].setdefault(pkey, empty_pyramid_state(s, side))
                # Não cria novos pyramid_grids. Estados G0 antigos, se existirem,
                # são tratados de forma segura no startup.
        st["version"] = VERSION
        return st

    def save(self) -> None:
        with self.lock:
            self.state["updated_at"] = now_iso()
            atomic_json_write(STATE_FILE, self.state)

    def kill(self, mode: str, reason: str) -> None:
        with self.lock:
            self.state["kill_switch"] = {"mode": mode, "reason": reason, "at": now_iso()}
            self.save()
        logger.error(f"KILL SWITCH | mode={mode} | reason={reason}")

    def killed(self) -> str:
        with self.lock:
            return self.state.get("kill_switch", {}).get("mode", "OFF")

    def set_trade_gate(self, allowed: bool, reason: Optional[str] = None) -> None:
        with self.lock:
            self.state["trade_gate"] = {"open_allowed": bool(allowed), "reason": reason, "at": now_iso()}
            self.save()

    def entry_allowed(self) -> Tuple[bool, Optional[str]]:
        with self.lock:
            blocks = self.state.get("protection_blocks", {}) or {}
            if blocks:
                first_key = sorted(blocks)[0]
                return False, f"PROTECTION_BLOCK:{first_key}:{blocks[first_key]}"
            g = self.state.get("trade_gate", {}) or {}
            return bool(g.get("open_allowed", True)), g.get("reason")

    def set_protection_block(self, strategy_id: str, reason: Optional[str]) -> None:
        with self.lock:
            blocks = self.state.setdefault("protection_blocks", {})
            if reason:
                blocks[strategy_id] = str(reason)
            else:
                blocks.pop(strategy_id, None)
            self.save()
        logger.warning("PROTECTION BLOCK | strategy=%s | active=%s | reason=%s", strategy_id, bool(reason), reason)

    def clear_soft_position_mismatch(self) -> bool:
        with self.lock:
            ks = self.state.get("kill_switch", {}) or {}
            if str(ks.get("mode")) != "SOFT":
                return False
            if not str(ks.get("reason") or "").startswith("POSITION_MISMATCH"):
                return False
            self.state["kill_switch"] = {"mode": "OFF", "reason": None, "at": now_iso()}
            self.save()
        logger.warning("KILL SWITCH AUTO-CLEAR | POSITION_MISMATCH reconciliado | novas entradas liberadas")
        return True

# -----------------------------------------------------------------------------
# DURABLE FILL LEDGER + EXCHANGE SNAPSHOT + ORDER STATE MACHINE
# -----------------------------------------------------------------------------

@dataclass
class ExchangeSnapshot:
    captured_ms: int
    positions: Dict[Tuple[str, str], Decimal]
    entry_prices: Dict[Tuple[str, str], Decimal]
    open_orders: List[Dict[str, Any]]

class FillLedger:
    def __init__(self, path: Path):
        self.path = path
        self.lock = threading.RLock()
        self.db = sqlite3.connect(str(path), check_same_thread=False, timeout=30)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript("""
        CREATE TABLE IF NOT EXISTS orders (
            client_id TEXT PRIMARY KEY,
            strategy_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            position_side TEXT NOT NULL,
            action TEXT NOT NULL,
            order_type TEXT NOT NULL,
            requested_qty TEXT NOT NULL,
            order_id TEXT,
            status TEXT NOT NULL,
            executed_qty TEXT NOT NULL DEFAULT '0',
            avg_price TEXT NOT NULL DEFAULT '0',
            commission TEXT NOT NULL DEFAULT '0',
            realized_pnl TEXT NOT NULL DEFAULT '0',
            reason TEXT,
            created_ms INTEGER NOT NULL,
            updated_ms INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS lots (
            leg_id TEXT PRIMARY KEY,
            strategy_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            position_side TEXT NOT NULL,
            opened_qty TEXT NOT NULL,
            open_qty TEXT NOT NULL,
            entry_price TEXT NOT NULL,
            open_client_id TEXT,
            opened_ms INTEGER NOT NULL,
            closed_ms INTEGER,
            source TEXT NOT NULL DEFAULT 'BOT'
        );
        CREATE INDEX IF NOT EXISTS idx_lots_open ON lots(symbol, position_side, open_qty);
        CREATE INDEX IF NOT EXISTS idx_orders_oid ON orders(order_id);
        """)
        self.db.commit()

    def close(self) -> None:
        with self.lock:
            self.db.commit(); self.db.close()

    def reset(self) -> None:
        with self.lock:
            self.db.execute("DELETE FROM lots")
            self.db.execute("DELETE FROM orders")
            self.db.commit()
        logger.warning("LEDGER RESET | durable fill ledger cleared after confirmed emergency reset")

    def order_state(self, client_id: str, strategy_id: str, symbol: str, position_side: str,
                    action: str, order_type: str, requested_qty: Decimal, status: str,
                    order_id: Any = None, executed_qty: Decimal = D(0), avg_price: Decimal = D(0),
                    commission: Decimal = D(0), realized_pnl: Decimal = D(0), reason: str = "") -> None:
        with self.lock:
            t = now_ms()
            self.db.execute("""
                INSERT INTO orders(client_id,strategy_id,symbol,position_side,action,order_type,requested_qty,
                    order_id,status,executed_qty,avg_price,commission,realized_pnl,reason,created_ms,updated_ms)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(client_id) DO UPDATE SET
                    order_id=excluded.order_id,status=excluded.status,executed_qty=excluded.executed_qty,
                    avg_price=excluded.avg_price,commission=excluded.commission,realized_pnl=excluded.realized_pnl,
                    reason=excluded.reason,updated_ms=excluded.updated_ms
            """, (client_id, strategy_id, symbol, position_side, action, order_type, str(requested_qty),
                  str(order_id or ""), status, str(executed_qty), str(avg_price), str(commission),
                  str(realized_pnl), reason, t, t))
            self.db.commit()
        jsonl_append(ORDER_JOURNAL_FILE, {"client_id": client_id, "strategy": strategy_id, "symbol": symbol,
            "position_side": position_side, "action": action, "order_type": order_type, "status": status,
            "executed_qty": str(executed_qty), "avg_price": str(avg_price), "commission": str(commission),
            "realized_pnl": str(realized_pnl), "at": now_iso()})

    def record_open_lot(self, leg_id: str, strategy_id: str, symbol: str, position_side: str,
                        qty: Decimal, entry_price: Decimal, client_id: str, source: str = "BOT") -> None:
        with self.lock:
            self.db.execute("""
                INSERT INTO lots(leg_id,strategy_id,symbol,position_side,opened_qty,open_qty,entry_price,
                                 open_client_id,opened_ms,source)
                VALUES(?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(leg_id) DO UPDATE SET strategy_id=excluded.strategy_id,symbol=excluded.symbol,
                    position_side=excluded.position_side,opened_qty=excluded.opened_qty,
                    open_qty=CASE WHEN CAST(lots.open_qty AS REAL)>0 THEN lots.open_qty ELSE excluded.open_qty END,
                    entry_price=excluded.entry_price,open_client_id=excluded.open_client_id,source=excluded.source
            """, (leg_id, strategy_id, symbol, position_side, str(qty), str(qty), str(entry_price), client_id, now_ms(), source))
            self.db.commit()

    def record_close_lot(self, leg_id: str, qty: Decimal) -> None:
        with self.lock:
            row = self.db.execute("SELECT open_qty FROM lots WHERE leg_id=?", (leg_id,)).fetchone()
            if not row:
                return
            remaining = max(D(0), dec(row[0]) - qty)
            self.db.execute("UPDATE lots SET open_qty=?, closed_ms=? WHERE leg_id=?",
                            (str(remaining), now_ms() if remaining <= 0 else None, leg_id))
            self.db.commit()

    def open_by_symbol_side(self) -> Dict[Tuple[str, str], Decimal]:
        out: Dict[Tuple[str, str], Decimal] = {}
        with self.lock:
            rows = self.db.execute("SELECT symbol,position_side,open_qty FROM lots WHERE CAST(open_qty AS REAL)>0").fetchall()
        for sym, side, q in rows:
            k = (str(sym).upper(), str(side).upper())
            out[k] = out.get(k, D(0)) + dec(q)
        return out

    def open_strategy_qty(self, strategy_id: str, symbol: str, side: str) -> Decimal:
        with self.lock:
            row = self.db.execute("SELECT COALESCE(SUM(CAST(open_qty AS REAL)),0) FROM lots WHERE strategy_id=? AND symbol=? AND position_side=? AND CAST(open_qty AS REAL)>0",
                                  (strategy_id, symbol, side)).fetchone()
        return dec(row[0] if row else 0)

    def zero_open_strategy_side(self, strategy_id: str, symbol: str, side: str, reason: str = "AUTO_REPAIR") -> Decimal:
        """Zera apenas lots virtuais de uma estratégia/lado confirmados como fantasmas.

        Nunca altera posição física. O Reconciler só chama este método quando a posição
        física é explicada exatamente pelas OUTRAS estratégias do mesmo símbolo/lado.
        """
        with self.lock:
            rows = self.db.execute(
                "SELECT leg_id,open_qty FROM lots WHERE strategy_id=? AND symbol=? AND position_side=? AND CAST(open_qty AS REAL)>0",
                (strategy_id, symbol, side),
            ).fetchall()
            removed = sum((dec(q) for _, q in rows), D(0))
            if removed <= 0:
                return D(0)
            t = now_ms()
            self.db.execute(
                "UPDATE lots SET open_qty='0', closed_ms=? WHERE strategy_id=? AND symbol=? AND position_side=? AND CAST(open_qty AS REAL)>0",
                (t, strategy_id, symbol, side),
            )
            self.db.commit()
        logger.warning(f"LEDGER AUTO-REPAIR | strategy={strategy_id} {symbol} {side} removed_ghost_qty={removed} reason={reason}")
        return removed

    def open_non_range_qty(self, symbol: str, side: str) -> Decimal:
        with self.lock:
            row = self.db.execute(
                "SELECT COALESCE(SUM(CAST(open_qty AS REAL)),0) FROM lots WHERE symbol=? AND position_side=? AND strategy_id NOT LIKE 'RANGE:%' AND CAST(open_qty AS REAL)>0",
                (symbol, side),
            ).fetchone()
        return dec(row[0] if row else 0)

    def rename_strategy(self, old_strategy_id: str, new_strategy_id: str,
                        symbol: Optional[str] = None, side: Optional[str] = None) -> int:
        """Renomeia ownership lógico no ledger sem tocar na posição física."""
        if old_strategy_id == new_strategy_id:
            return 0
        with self.lock:
            where = ["strategy_id=?"]
            args: List[Any] = [old_strategy_id]
            if symbol:
                where.append("symbol=?"); args.append(symbol)
            if side:
                where.append("position_side=?"); args.append(side)
            clause = " AND ".join(where)
            row = self.db.execute(
                f"SELECT COUNT(*) FROM lots WHERE {clause}", tuple(args)
            ).fetchone()
            count = int(row[0] if row else 0)
            self.db.execute(
                f"UPDATE lots SET strategy_id=? WHERE {clause}",
                tuple([new_strategy_id] + args),
            )
            self.db.execute(
                "UPDATE orders SET strategy_id=? WHERE strategy_id=?",
                (new_strategy_id, old_strategy_id),
            )
            self.db.commit()
        if count:
            logger.warning(
                f"LEDGER STRATEGY MIGRATION | {old_strategy_id} -> {new_strategy_id} "
                f"| symbol={symbol or '*'} side={side or '*'} lots={count}"
            )
        return count

    def bootstrap_from_state(self, store: 'StateStore') -> int:
        with self.lock:
            existing = self.db.execute("SELECT COUNT(*) FROM lots").fetchone()[0]
        if existing:
            return 0
        seeded = 0
        with store.lock:
            for sym, st in store.state.get("range", {}).items():
                b = (st or {}).get("basket") or {}
                for leg in b.get("legs", []) or []:
                    q = dec(leg.get("qty")); ep = dec(leg.get("entry_price")); lid = str(leg.get("id") or uuid.uuid4().hex)
                    if q > 0 and ep > 0:
                        self.record_open_lot(lid, f"RANGE:{sym}", sym, str(leg.get("side")), q, ep, lid, "STATE_BOOTSTRAP"); seeded += 1
            for bucket in ("pyramid", "pyramid_grids"):
                for st in store.state.get(bucket, {}).values():
                    st = st or {}
                    for leg in st.get("legs", []) or []:
                        q = dec(leg.get("qty")); ep = dec(leg.get("entry_price")); lid = str(leg.get("id") or uuid.uuid4().hex)
                        if q > 0 and ep > 0:
                            self.record_open_lot(lid, str(st.get("strategy")), str(st.get("symbol")), str(leg.get("side")), q, ep, lid, "STATE_BOOTSTRAP"); seeded += 1
        if seeded:
            logger.warning(f"LEDGER BOOTSTRAP | lots_seeded={seeded} from state.json")
        return seeded

class OrderManager:
    TERMINAL = {"FILLED", "CANCELED", "REJECTED", "EXPIRED"}
    def __init__(self, client: AsterClient, ledger: FillLedger):
        self.client = client
        self.ledger = ledger
        self.lock = threading.RLock()

    def submit_market(self, strategy_id: str, symbol: str, position_side: str, side: str,
                      qty: Decimal, client_id: str, reason: str) -> Dict[str, Any]:
        action = "OPEN" if side == ("BUY" if position_side == "LONG" else "SELL") else "CLOSE"
        self.ledger.order_state(client_id, strategy_id, symbol, position_side, action,
                                "MARKET", qty, "CREATED", reason=reason)
        try:
            resp = self.client.order(symbol, side, position_side, qty, client_id, "MARKET")
            self.ledger.order_state(client_id, strategy_id, symbol, position_side, action, "MARKET", qty,
                                    str(resp.get("status") or "SUBMITTED"), resp.get("orderId"),
                                    dec(resp.get("executedQty")), dec(resp.get("avgPrice")), reason=reason)
            return resp
        except AsterAPIError as e:
            status = "UNKNOWN" if e.code == 503 else "REJECTED"
            self.ledger.order_state(client_id, strategy_id, symbol, position_side, action, "MARKET", qty,
                                    status, reason=reason)
            if status == "UNKNOWN":
                logger.critical(
                    "ORDER EXECUTION UNKNOWN | strategy=%s symbol=%s side=%s posSide=%s qty=%s cid=%s",
                    strategy_id, symbol, side, position_side, qty, client_id,
                )
            raise
        except Exception:
            self.ledger.order_state(client_id, strategy_id, symbol, position_side, action, "MARKET", qty,
                                    "REJECTED", reason=reason)
            raise

    def submit_conditional(self, strategy_id: str, symbol: str, position_side: str, side: str,
                           qty: Decimal, stop_price: Decimal, client_id: str, order_type: str,
                           working_type: str, price_protect: bool, reason: str) -> Dict[str, Any]:
        self.ledger.order_state(client_id, strategy_id, symbol, position_side, "PROTECT", order_type,
                                qty, "CREATED", reason=reason)
        try:
            resp = self.client.conditional_order(symbol, side, position_side, qty, stop_price,
                                                client_id, order_type, working_type, price_protect)
            self.ledger.order_state(client_id, strategy_id, symbol, position_side, "PROTECT", order_type, qty,
                                    str(resp.get("status") or "SUBMITTED"), resp.get("orderId"),
                                    dec(resp.get("executedQty")), dec(resp.get("avgPrice")), reason=reason)
            return resp
        except AsterAPIError as e:
            status = "UNKNOWN" if e.code == 503 else "REJECTED"
            self.ledger.order_state(client_id, strategy_id, symbol, position_side, "PROTECT", order_type, qty,
                                    status, reason=reason)
            if status == "UNKNOWN":
                logger.critical(
                    "PROTECTIVE ORDER EXECUTION UNKNOWN | strategy=%s symbol=%s posSide=%s qty=%s cid=%s",
                    strategy_id, symbol, position_side, qty, client_id,
                )
            raise
        except Exception:
            self.ledger.order_state(client_id, strategy_id, symbol, position_side, "PROTECT", order_type, qty,
                                    "REJECTED", reason=reason)
            raise

# -----------------------------------------------------------------------------
# ACCOUNT / LEVERAGE / EXECUTION
# -----------------------------------------------------------------------------

class AccountManager:
    def __init__(self, client: AsterClient, rules: RulesBook, store: StateStore):
        self.client = client
        self.rules = rules
        self.store = store
        self.wallet_balance = D(0)
        self.available_balance = D(0)
        self.unrealized = D(0)
        self.last_sync = 0.0
        self.commission: Dict[str, Decimal] = {}
        self._lock = threading.RLock()

    def sync(self, force: bool = False) -> None:
        if not LIVE_TRADING and not VALIDATE_API_ONLY:
            strategy_count = (
                (len(SYMBOLS) if RANGE_ENGINE_ENABLED else 0)
                + (len(SYMBOLS) * 2 if PYRAMID_ENGINE_ENABLED else 0)
            )
            simulated_total = D(0)
            if RANGE_ENGINE_ENABLED:
                simulated_total += sum((configured_bankroll(s) for s in SYMBOLS), D(0))
            if PYRAMID_ENGINE_ENABLED:
                simulated_total += PYRAMID_BANKROLL_USD * D(len(SYMBOLS) * 2)
            if strategy_count == 0:
                simulated_total = INITIAL_BANKROLL_USD
            self.wallet_balance = simulated_total
            self.available_balance = simulated_total
            self.unrealized = D(0)
            self.last_sync = time.time()
            return
        if not force and time.time() - self.last_sync < ACCOUNT_SYNC_SECONDS:
            return
        with self._lock:
            acct = self.client.account()
            self.wallet_balance = dec(acct.get("totalWalletBalance") or acct.get("totalMarginBalance") or 0)
            self.available_balance = dec(acct.get("availableBalance") or 0)
            self.unrealized = dec(acct.get("totalUnrealizedProfit") or 0)
            self.last_sync = time.time()
            with self.store.lock:
                self.store.state["last_wallet"] = {
                    "wallet": str(self.wallet_balance),
                    "available": str(self.available_balance),
                    "unrealized": str(self.unrealized),
                    "at": now_iso(),
                }
            self.store.save()

    def free_margin(self) -> Decimal:
        self.sync()
        return max(D(0), self.available_balance - MIN_FREE_WALLET_BUFFER_USD)

    def ensure_modes(self) -> None:
        if not LIVE_TRADING:
            logger.info("MODES | simulacao: nao altera Hedge/Isolated")
            return
        self.client.set_single_asset_mode()
        logger.info("MODES | Single-Asset Mode confirmado")
        self.client.set_hedge_mode()
        if not self.client.position_mode():
            raise RuntimeError("Conta nao esta em Hedge Mode")
        for s in SYMBOLS:
            self.client.set_margin_type(s, True)
        logger.info(f"MODES | Hedge Mode confirmado | ISOLATED solicitado em {','.join(SYMBOLS)}")

    def get_brackets(self, symbol: str) -> List[Dict[str, Any]]:
        if not LIVE_TRADING:
            return []
        try:
            x = self.client.leverage_bracket(symbol)
            if isinstance(x, list):
                if x and "brackets" in x[0]:
                    return x[0].get("brackets", [])
                return x
            if isinstance(x, dict):
                return x.get("brackets", [])
        except Exception as e:
            logger.warning(f"LEVERAGE BRACKET FAIL | {symbol} | {e}")
        return []

    def max_exchange_leverage(self, symbol: str, notional: Decimal) -> Tuple[int, Decimal]:
        brackets = self.get_brackets(symbol)
        max_lev = API_HARD_MAX_LEVERAGE
        mmr = D("0.005")
        if brackets:
            chosen = None
            for b in brackets:
                floor = dec(b.get("notionalFloor"))
                cap = dec(b.get("notionalCap"), "1e50")
                if floor <= notional < cap:
                    chosen = b
                    break
            if chosen is None:
                chosen = brackets[-1]
            max_lev = int(chosen.get("initialLeverage", max_lev))
            mmr = dec(chosen.get("maintMarginRatio"), "0.005")
        return max(1, min(max_lev, API_HARD_MAX_LEVERAGE, BOT_HARD_MAX_LEVERAGE,
                          MAX_REQUESTED_LEVERAGE)), mmr

    def safe_leverage_cap(self, symbol: str, notional: Decimal, adverse_distance_pct: Decimal) -> Tuple[int, Dict[str, Any]]:
        exch_max, mmr = self.max_exchange_leverage(symbol, notional)
        protected_move = adverse_distance_pct * ADVERSE_MOVE_SAFETY_MULTIPLIER
        denom = protected_move + mmr + LIQUIDATION_BUFFER_PCT
        liq_safe = int((D(1) / denom).to_integral_value(rounding=ROUND_DOWN)) if denom > 0 else exch_max
        # Aster/Binance-style futures leverage is configured per SYMBOL, not per virtual
        # strategy and not independently per LONG/SHORT leg.  When PYRAMID is enabled
        # it requires PYRAMID_LEVERAGE as its hard safety ceiling, therefore every
        # strategy sharing the symbol must respect the same ceiling.  Otherwise a
        # RANGE order could raise the symbol from 10x to e.g. 31x and silently move
        # the liquidation price of an already-open PYRAMID position.
        shared_symbol_cap = PYRAMID_LEVERAGE if PYRAMID_ENGINE_ENABLED else MAX_REQUESTED_LEVERAGE
        cap = max(MIN_LEVERAGE, min(exch_max, liq_safe, API_HARD_MAX_LEVERAGE,
                                    BOT_HARD_MAX_LEVERAGE, MAX_REQUESTED_LEVERAGE,
                                    shared_symbol_cap))
        return cap, {"exchange_max": exch_max, "bot_hard_max": BOT_HARD_MAX_LEVERAGE,
                     "shared_symbol_cap": shared_symbol_cap,
                     "mmr": str(mmr), "protected_move": str(protected_move),
                     "liq_safe_max": liq_safe, "denom": str(denom)}

    def current_symbol_notional(self, symbol: str) -> Decimal:
        if not LIVE_TRADING:
            return D(0)
        total = D(0)
        try:
            for p in self.client.positions(symbol):
                q = abs(dec(p.get("positionAmt"))); mark = dec(p.get("markPrice") or p.get("entryPrice"))
                if q > 0 and mark > 0:
                    total += q * mark
        except Exception as e:
            logger.warning(f"SYMBOL NOTIONAL SNAPSHOT FAIL | {symbol} | {e}")
        return total

    def base_margin_budget(self, strategy_state: Dict[str, Any]) -> Decimal:
        eq = dec(strategy_state.get("equity"), str(INITIAL_BANKROLL_USD))
        desired = eq
        desired = min(desired, eq * MAX_MARGIN_FRACTION_PER_STRATEGY)
        return max(D(0), desired)

    def sizing_for_profit_target(self, symbol: str, price: Decimal, strategy_state: Dict[str, Any],
                                 target_profit: Optional[Decimal], target_move_pct: Decimal,
                                 adverse_distance_pct: Decimal, recovery_level: int = 0,
                                 desired_notional_override: Optional[Decimal] = None,
                                 recovery_multiplier: Optional[Decimal] = None) -> Optional[Dict[str, Any]]:
        self.sync()
        active = bool(strategy_state.get("position") or strategy_state.get("basket"))
        configured_base = configured_bankroll(symbol)
        previous_base = dec(strategy_state.get("bankroll_config_base"), str(INITIAL_BANKROLL_USD))
        if not active and configured_base != previous_base:
            previous_equity = dec(strategy_state.get("equity"), str(previous_base))
            strategy_state["equity"] = str(previous_equity + configured_base - previous_base)
            strategy_state["bankroll_config_base"] = str(configured_base)
            strategy_state["last_update"] = now_iso()
            self.store.save()
            logger.info(
                f"BANKROLL MIGRATION | {strategy_state.get('strategy', symbol)} | base {previous_base}->{configured_base} | equity {previous_equity}->{strategy_state['equity']}"
            )
        logical_eq = dec(strategy_state.get("equity"), str(INITIAL_BANKROLL_USD))
        physical_free = self.free_margin()
        if logical_eq <= 0 or physical_free <= 0:
            return None

        base_budget = min(self.base_margin_budget(strategy_state), logical_eq, physical_free)
        if base_budget <= 0:
            return None

        recovery_level = max(0, min(int(recovery_level), MAX_RECOVERY_FAILURES))
        if recovery_multiplier is None:
            recovery_multiplier = RECOVERY_MULTIPLIER

        base_notional = max(configured_initial_notional(symbol), logical_eq)
        if recovery_level == 0:
            desired_notional = base_notional
            cap, meta = self.safe_leverage_cap(symbol, desired_notional, adverse_distance_pct)
            lev = cap
            margin = desired_notional / D(lev)
            if margin > base_budget:
                return None
        else:
            classic_notional = base_notional * (recovery_multiplier ** recovery_level)
            desired_notional = max(classic_notional, dec(desired_notional_override)) \
                if desired_notional_override is not None else classic_notional
            recovery_cap = configured_max_recovery_notional(symbol)
            if desired_notional > recovery_cap:
                logger.warning(f"RECOVERY CAP | {symbol} | requested={desired_notional} capped={recovery_cap} level={recovery_level}")
                desired_notional = recovery_cap
            cap, meta = self.safe_leverage_cap(symbol, desired_notional, adverse_distance_pct)
            lev = cap
            margin = desired_notional / D(lev)

        lev = max(MIN_LEVERAGE, min(int(lev), MAX_REQUESTED_LEVERAGE,
                                    BOT_HARD_MAX_LEVERAGE, API_HARD_MAX_LEVERAGE))
        fresh_operation = recovery_level == 0
        notional = desired_notional
        qty = self.rules.qty(symbol, notional / price, price)
        actual_notional = qty * price
        actual_margin = actual_notional / D(lev)
        current_symbol_notional = self.current_symbol_notional(symbol)
        symbol_cap = configured_max_total_symbol_notional(symbol)
        if current_symbol_notional + actual_notional > symbol_cap:
            logger.warning(f"SYMBOL EXPOSURE CAP | {symbol} | current={current_symbol_notional} new={actual_notional} cap={symbol_cap}")
            return None
        estimated_adverse_loss = actual_notional * adverse_distance_pct
        if fresh_operation:
            max_allowed = desired_notional * (D(1) + MAX_INITIAL_NOTIONAL_OVERSHOOT_PCT)
            if actual_notional > max_allowed:
                rule = self.rules.rules[symbol]
                logger.warning(
                    f"SIZING BLOCK | {symbol} | entrada_inicial={desired_notional} notional_minimo_real={actual_notional} "
                    f"limite_com_tolerancia={max_allowed} min_qty={rule.min_qty} step={rule.step_size} price={price}"
                )
                return None
        if estimated_adverse_loss > logical_eq * MAX_MARGIN_FRACTION_PER_STRATEGY:
            logger.warning(
                f"SIZING RISK BLOCK | {symbol} | notional={actual_notional} perda_estimada_stop={estimated_adverse_loss} caixa_logico={logical_eq} level={recovery_level}"
            )
            return None
        if actual_margin > physical_free:
            logger.warning(
                f"SIZING MARGIN BLOCK | {symbol} | margin_necessaria={actual_margin} margem_livre={physical_free} notional={actual_notional} lev={lev}x level={recovery_level}"
            )
            return None
        return {
            "leverage": lev,
            "qty": qty,
            "price": price,
            "notional": actual_notional,
            "margin": actual_margin,
            "estimated_adverse_loss": estimated_adverse_loss,
            "target_profit": target_profit or D(0),
            "recovery_level": recovery_level,
            "desired_notional_override": desired_notional_override,
            "recovery_multiplier": str(recovery_multiplier),
            "meta": meta,
        }

    def active_symbol_leverage(self, symbol: str) -> Tuple[Optional[int], int]:
        """Return (exchange_leverage, open_position_count) for a symbol.

        In Hedge Mode the leverage setting is still symbol-wide.  Both LONG and SHORT
        rows therefore share the same leverage configuration.
        """
        if not LIVE_TRADING:
            return None, 0
        rows = self.client.positions(symbol)
        if isinstance(rows, dict):
            rows = [rows]
        open_rows = [p for p in (rows or []) if abs(dec(p.get("positionAmt"))) > 0]
        if not open_rows:
            return None, 0
        leverages = []
        for p in open_rows:
            try:
                lv = int(dec(p.get("leverage")))
                if lv > 0:
                    leverages.append(lv)
            except Exception:
                pass
        if not leverages:
            logger.error(f"LEVERAGE SNAPSHOT INVALID | {symbol} | posicao aberta sem campo leverage")
            return None, len(open_rows)
        # A symbol should expose one leverage value across hedge-side rows.  If the API
        # ever reports a disagreement, use the highest value as the conservative risk view.
        current = max(leverages)
        if any(x != current for x in leverages):
            logger.warning(f"LEVERAGE SNAPSHOT DIVERGENTE | {symbol} | values={leverages} | usando={current}x")
        return current, len(open_rows)

    def prepare_leverage_for_open(self, symbol: str, requested: int) -> Optional[int]:
        """Safely resolve the leverage that an opening order may use.

        Aster rejects leverage reduction in ISOLATED mode while a symbol has an open
        position. In Hedge Mode the leverage is symbol-wide, so independent strategies
        cannot own independent leverage values. Therefore leverage is changed only while
        the symbol is flat. With an open position every strategy adopts the leverage that
        is already active on the exchange; no leverage-change request is sent.
        """
        requested = max(MIN_LEVERAGE, min(int(requested), MAX_REQUESTED_LEVERAGE,
                                          BOT_HARD_MAX_LEVERAGE, API_HARD_MAX_LEVERAGE))
        if not LIVE_TRADING:
            logger.info(f"LEVERAGE | {symbol} | {requested}x | SIM")
            return requested

        try:
            current, open_count = self.active_symbol_leverage(symbol)
        except Exception as e:
            logger.warning(f"LEVERAGE PRECHECK FAIL | {symbol} | requested={requested}x | {e}")
            return None

        if open_count == 0:
            try:
                self.client.set_leverage(symbol, requested)
                logger.info(f"LEVERAGE SET | {symbol} | {requested}x | symbol_flat=True")
                return requested
            except AsterAPIError as e:
                logger.warning(f"LEVERAGE SET BLOCK | {symbol} | requested={requested}x | {e}")
                return None

        if current is None:
            logger.warning(f"LEVERAGE ENTRY BLOCK | {symbol} | requested={requested}x | posicoes_abertas={open_count} current=UNKNOWN")
            return None
        # Em ISOLATED/HEDGE a alavancagem pertence ao SIMBOLO, nao à estrategia nem
        # ao lado LONG/SHORT. Com qualquer posicao aberta, nunca tentamos alterar a
        # alavancagem: adotamos exatamente o valor ja ativo na exchange. Isso permite
        # varias estrategias no mesmo simbolo sem provocar HTTP 400 por reducao de
        # leverage nem bloquear indefinidamente uma segunda estrategia.
        #
        # O sizing continua sendo feito pelo notional da estrategia; leverage nao muda
        # PnL por unidade de movimento, apenas margem/liquidacao. open_leg() recalcula
        # a margem efetiva com o leverage compartilhado antes de enviar a ordem.
        absolute_cap = min(MAX_REQUESTED_LEVERAGE, BOT_HARD_MAX_LEVERAGE, API_HARD_MAX_LEVERAGE)
        if current < MIN_LEVERAGE or current > absolute_cap:
            logger.warning(
                f"LEVERAGE ENTRY BLOCK | {symbol} | current={current}x requested={requested}x "
                f"posicoes_abertas={open_count} | leverage ativo fora dos limites absolutos"
            )
            return None

        # Direcional is intentionally a 10x Pyramid architecture by default.
        # Existing inherited leverage above this ceiling is managed/protected, but
        # no new level is allowed until the symbol becomes flat and can be reset.
        effective_cap = max(MIN_LEVERAGE, min(PYRAMID_MAX_EFFECTIVE_LEVERAGE, absolute_cap))
        if current > effective_cap:
            logger.critical(
                f"LEVERAGE ADD BLOCK | {symbol} | current={current}x cap={effective_cap}x "
                f"requested={requested}x positions={open_count} | existing positions remain managed"
            )
            return None

        if current != requested:
            logger.warning(
                f"LEVERAGE ADOPT | {symbol} | current={current}x requested={requested}x "
                f"posicoes_abertas={open_count} | usando leverage compartilhado ja ativo; sem alterar exchange"
            )
        else:
            logger.info(
                f"LEVERAGE REUSE | {symbol} | current={current}x requested={requested}x "
                f"posicoes_abertas={open_count}"
            )
        return current

    def set_leverage(self, symbol: str, leverage: int) -> None:
        # Kept for compatibility with older call sites.  New opening orders must use
        # prepare_leverage_for_open(), which understands symbol-wide isolated leverage.
        effective = self.prepare_leverage_for_open(symbol, leverage)
        if effective is None:
            raise RuntimeError(f"Leverage indisponivel para {symbol}: solicitado={leverage}x")

# -----------------------------------------------------------------------------
# EXECUTION + VIRTUAL LOT BOOK
# -----------------------------------------------------------------------------

class ExecutionEngine:
    PREFIX = "a3"

    def __init__(self, client: AsterClient, account: AccountManager, rules: RulesBook, store: StateStore,
                 ledger: FillLedger):
        self.client = client
        self.account = account
        self.rules = rules
        self.store = store
        self.ledger = ledger
        self.orders = OrderManager(client, ledger)
        self.seq = 0
        self.lock = threading.RLock()

    def client_id(self, strategy_id: str, action: str) -> str:
        self.seq = (self.seq + 1) % 9999
        digest = hashlib.sha1(strategy_id.encode()).hexdigest()[:6]
        return f"{self.PREFIX}-{digest}-{action[:5]}-{int(time.time())%1000000}-{self.seq}"[:36]

    @staticmethod
    def order_side(position_side: str, opening: bool) -> str:
        if position_side == "LONG":
            return "BUY" if opening else "SELL"
        return "SELL" if opening else "BUY"

    def _fill_from_response(self, symbol: str, resp: Dict[str, Any], client_id: str,
                            fallback_price: Decimal) -> Tuple[Decimal, Decimal]:
        status = str(resp.get("status", ""))
        qty = dec(resp.get("executedQty"))
        avg = dec(resp.get("avgPrice"))
        end = time.time() + ORDER_FILL_WAIT_SECONDS
        while (status not in ("FILLED", "PARTIALLY_FILLED") or qty <= 0 or avg <= 0) and time.time() < end:
            try:
                q = self.client.query_order(symbol, client_id)
                status = str(q.get("status", status))
                qty = dec(q.get("executedQty") or qty)
                avg = dec(q.get("avgPrice") or avg)
                resp = q
                if status == "FILLED" and qty > 0 and avg > 0:
                    break
            except Exception:
                pass
            time.sleep(ORDER_POLL_SECONDS)
        if qty <= 0:
            raise RuntimeError(f"Ordem sem fill confirmado: {symbol} client_id={client_id} status={status}")
        if avg <= 0:
            avg = fallback_price
            logger.warning(f"FILL AVG AUSENTE | {symbol} | cid={client_id} | usando ref_price={fallback_price}")
        return qty, avg

    def _actual_trade_costs(self, symbol: str, order_id: Any, around_ms: int) -> Tuple[Decimal, Decimal]:
        if not LIVE_TRADING or not order_id:
            return D(0), D(0)
        try:
            rows = self.client.user_trades(symbol, max(0, around_ms-120000), around_ms+120000, 1000)
            matched = [x for x in (rows if isinstance(rows, list) else []) if str(x.get("orderId")) == str(order_id)]
            commission = sum((abs(dec(x.get("commission"))) for x in matched), D(0))
            realized = sum((dec(x.get("realizedPnl")) for x in matched), D(0))
            return commission, realized
        except Exception as e:
            logger.warning(f"ACTUAL FEE/P&L LOOKUP FAIL | {symbol} order_id={order_id} | {e}")
            return D(0), D(0)

    def market(self, strategy_id: str, symbol: str, position_side: str, qty: Decimal,
               opening: bool, ref_price: Decimal) -> Dict[str, Any]:
        with self.lock:
            side = self.order_side(position_side, opening)
            cid = self.client_id(strategy_id, "open" if opening else "close")
            if not LIVE_TRADING:
                logger.info(f"SIM ORDER | {strategy_id} | {'OPEN' if opening else 'CLOSE'} {side} posSide={position_side} qty={qty} px~{ref_price} cid={cid}")
                return {"qty": qty, "price": ref_price, "client_id": cid,
                        "order_id": f"SIM-{cid}", "status": "FILLED", "time": now_ms(),
                        "price_source": "SIM_REF"}
            submitted_ms = now_ms()
            resp = self.orders.submit_market(strategy_id, symbol, position_side, side, qty, cid,
                                             "OPEN" if opening else "CLOSE")
            filled, avg = self._fill_from_response(symbol, resp, cid, ref_price)
            order_id = resp.get("orderId")
            if not order_id:
                try:
                    order_id = self.client.query_order(symbol, cid).get("orderId")
                except Exception:
                    pass
            commission, realized = self._actual_trade_costs(symbol, order_id, now_ms())
            self.ledger.order_state(cid, strategy_id, symbol, position_side,
                                    "OPEN" if opening else "CLOSE", "MARKET", qty,
                                    "FILLED", order_id, filled, avg, commission, realized,
                                    "MARKET_EXECUTION")
            logger.info(f"ORDER FILLED | {strategy_id} | {'OPEN' if opening else 'CLOSE'} {side} posSide={position_side} requested_qty={qty} filled_qty={filled} avg={avg} commission={commission} realized={realized} cid={cid}")
            return {"qty": filled, "price": avg, "client_id": cid, "order_id": order_id,
                    "status": "FILLED", "time": submitted_ms, "price_source": "EXCHANGE_AVG",
                    "commission_actual": commission, "realized_pnl_exchange": realized}

    def open_leg(self, strategy_id: str, symbol: str, position_side: str, sizing: Dict[str, Any],
                 reason: str) -> Optional[Dict[str, Any]]:
        requested_leverage = int(sizing["leverage"])
        effective_leverage = self.account.prepare_leverage_for_open(symbol, requested_leverage)
        if effective_leverage is None:
            logger.warning(
                f"OPEN BLOCK LEVERAGE | {strategy_id} | {symbol} {position_side} | "
                f"requested={requested_leverage}x reason={reason}"
            )
            return None

        # If another live strategy has already fixed a LOWER symbol leverage, margin usage
        # is higher than the original sizing estimate.  Revalidate against current free
        # margin before submitting the order.
        actual_notional = dec(sizing.get("notional"))
        if actual_notional <= 0:
            actual_notional = dec(sizing["qty"]) * dec(sizing["price"])
        effective_margin = actual_notional / D(effective_leverage)
        free_margin = self.account.free_margin()
        if effective_margin > free_margin:
            logger.warning(
                f"OPEN BLOCK MARGIN | {strategy_id} | {symbol} | notional={actual_notional} "
                f"effective_lev={effective_leverage}x margin={effective_margin} free={free_margin}"
            )
            return None

        fill = self.market(strategy_id, symbol, position_side, sizing["qty"], True, sizing["price"])
        leg = {
            "id": fill["client_id"],
            "side": position_side,
            "qty": str(fill["qty"]),
            "entry_price": str(fill["price"]),
            "signal_price": str(sizing["price"]),
            "price_source": fill.get("price_source", "UNKNOWN"),
            "leverage": effective_leverage,
            "requested_leverage": requested_leverage,
            "notional": str(fill["qty"] * fill["price"]),
            "margin_est": str((fill["qty"] * fill["price"]) / D(effective_leverage)),
            "opened_at": now_iso(),
            "reason": reason,
        }
        self.ledger.record_open_lot(leg["id"], strategy_id, symbol, position_side, fill["qty"], fill["price"], fill["client_id"])
        jsonl_append(TRADES_FILE, {"event": "OPEN", "strategy": strategy_id, "symbol": symbol,
                                  "leg": leg, "at": now_iso()})
        return leg

    def _close_record(self, strategy_id: str, symbol: str, leg: Dict[str, Any],
                      closed_qty: Decimal, exitp: Decimal, reason: str,
                      close_client_id: str, exit_source: str) -> Dict[str, Any]:
        entry = dec(leg["entry_price"])
        gross = (exitp - entry) * closed_qty if leg["side"] == "LONG" else (entry - exitp) * closed_qty
        fee_rate = D(os.getenv("TAKER_FEE_RATE", "0.00035"))
        entry_fee_est = entry * closed_qty * fee_rate
        exit_fee_est = exitp * closed_qty * fee_rate
        fees_est = entry_fee_est + exit_fee_est
        entry_commission_actual = D(0); exit_commission_actual = D(0); exchange_realized = D(0)
        if LIVE_TRADING:
            try:
                open_row = self.ledger.db.execute("SELECT commission FROM orders WHERE client_id=?", (str(leg.get("id")),)).fetchone()
                if open_row:
                    full_open_fee = dec(open_row[0])
                    opened_qty = max(dec(leg.get("qty")), closed_qty)
                    if full_open_fee > 0 and opened_qty > 0:
                        entry_commission_actual = full_open_fee * (closed_qty / opened_qty)

                row = self.ledger.db.execute("SELECT order_id,commission,realized_pnl FROM orders WHERE client_id=?", (close_client_id,)).fetchone()
                order_id = row[0] if row and row[0] else None
                if row:
                    exit_commission_actual = dec(row[1]); exchange_realized = dec(row[2])
                if exit_commission_actual <= 0:
                    if not order_id:
                        try:
                            order_id = self.client.query_order(symbol, close_client_id).get("orderId")
                        except Exception:
                            order_id = None
                    if order_id:
                        exit_commission_actual, exchange_realized = self._actual_trade_costs(symbol, order_id, now_ms())
                        self.ledger.order_state(close_client_id, strategy_id, symbol, str(leg.get("side")), "CLOSE", "EXCHANGE_FILL", closed_qty,
                                                "FILLED", order_id, closed_qty, exitp, exit_commission_actual, exchange_realized, reason)
            except Exception as e:
                logger.warning(f"CLOSE ACTUAL COST RECONCILE FAIL | {close_client_id} | {e}")
        entry_fee_used = entry_commission_actual if entry_commission_actual > 0 else entry_fee_est
        exit_fee_used = exit_commission_actual if exit_commission_actual > 0 else exit_fee_est
        fees_actual = entry_commission_actual + exit_commission_actual
        fees_used = entry_fee_used + exit_fee_used
        pnl = gross - fees_used
        rec = {
            "leg_id": leg["id"], "side": leg["side"], "qty": str(closed_qty),
            "entry_price": str(entry), "exit_price": str(exitp), "gross": str(gross),
            "fees_est": str(fees_est), "entry_fee_actual": str(entry_commission_actual),
            "exit_fee_actual": str(exit_commission_actual), "fees_actual": str(fees_actual),
            "fees_used": str(fees_used), "exchange_realized_pnl": str(exchange_realized),
            "pnl_est": str(pnl), "reason": reason,
            "closed_at": now_iso(), "close_client_id": close_client_id,
            "exit_source": exit_source,
        }
        self.ledger.record_close_lot(str(leg.get("id")), closed_qty)
        jsonl_append(TRADES_FILE, {"event": "CLOSE", "strategy": strategy_id, "symbol": symbol,
                                  "close": rec, "at": now_iso()})
        return rec

    def physical_position_qty(self, symbol: str, position_side: str) -> Decimal:
        if not LIVE_TRADING:
            return D("1e50")
        rows = self.client.positions()
        for p in (rows if isinstance(rows, list) else []):
            if str(p.get("symbol", "")).upper() != str(symbol).upper():
                continue
            if str(p.get("positionSide", "")).upper() != str(position_side).upper():
                continue
            return abs(dec(p.get("positionAmt")))
        return D(0)

    def close_leg(self, strategy_id: str, symbol: str, leg: Dict[str, Any],
                  ref_price: Decimal, reason: str,
                  max_physical_qty: Optional[Decimal] = None) -> Optional[Dict[str, Any]]:
        wanted = dec(leg["qty"])
        qty = wanted
        if LIVE_TRADING:
            physical = self.physical_position_qty(symbol, str(leg["side"]))
            if max_physical_qty is not None:
                physical = min(physical, max(D(0), dec(max_physical_qty)))
            qty = min(wanted, physical)
            step = self.rules.rules[symbol].step_size
            qty = floor_step(qty, step)
            if qty <= 0:
                logger.warning(
                    f"CLOSE LEG SKIP | {strategy_id} | {symbol} {leg.get('side')} | wanted={wanted} physical_available={physical} | "
                    "motivo=POSICAO_FISICA_JA_ENCERRADA_OU_RESERVADA_PARA_OUTRA_ESTRATEGIA"
                )
                return None
        fill = self.market(strategy_id, symbol, leg["side"], qty, False, ref_price)
        closed_qty = min(qty, fill["qty"])
        return self._close_record(strategy_id, symbol, leg, closed_qty, fill["price"], reason,
                                  fill["client_id"], fill.get("price_source", "MARKET"))

    def close_legs(self, strategy_id: str, symbol: str, legs: List[Dict[str, Any]],
                   ref_price: Decimal, reason: str) -> Tuple[Decimal, List[Dict[str, Any]]]:
        closes = []
        total = D(0)
        for leg in list(legs):
            try:
                c = self.close_leg(strategy_id, symbol, leg, ref_price, reason)
                if c is None:
                    continue
                closes.append(c)
                total += dec(c["pnl_est"])
            except Exception as e:
                logger.exception(f"CLOSE LEG FAIL | {strategy_id} | leg={leg.get('id')} | {e}")
                raise
        return total, closes

    def install_bracket(self, strategy_id: str, symbol: str, leg: Dict[str, Any],
                        tp_price: Decimal, stop_price: Decimal) -> Optional[Dict[str, Any]]:
        if not NATIVE_PROTECTIVE_ORDERS:
            return None
        side = leg["side"]
        qty = dec(leg["qty"])
        if qty <= 0:
            return None
        if side == "LONG":
            tp = self.rules.trigger_price(symbol, tp_price, "UP")
            sl = self.rules.trigger_price(symbol, stop_price, "DOWN")
        else:
            tp = self.rules.trigger_price(symbol, tp_price, "DOWN")
            sl = self.rules.trigger_price(symbol, stop_price, "UP")
        close_side = self.order_side(side, False)
        tp_cid = self.client_id(strategy_id, "tp")
        sl_cid = self.client_id(strategy_id, "stop")
        if not LIVE_TRADING:
            logger.info(f"SIM BRACKET | {strategy_id} | {symbol} {side} qty={qty} TP={tp} SL={sl}")
            return {
                "tp": {"client_id": tp_cid, "stop_price": str(tp), "type": "TAKE_PROFIT_MARKET", "status": "NEW"},
                "sl": {"client_id": sl_cid, "stop_price": str(sl), "type": "STOP_MARKET", "status": "NEW"},
                "working_type": PROTECTIVE_WORKING_TYPE,
                "installed_at": now_iso(),
            }
        tp_resp = self.orders.submit_conditional(
            strategy_id, symbol, side, close_side, qty, tp, tp_cid, "TAKE_PROFIT_MARKET",
            PROTECTIVE_WORKING_TYPE, PROTECTIVE_PRICE_PROTECT, "TAKE_PROFIT",
        )
        try:
            sl_resp = self.orders.submit_conditional(
                strategy_id, symbol, side, close_side, qty, sl, sl_cid, "STOP_MARKET",
                PROTECTIVE_WORKING_TYPE, PROTECTIVE_PRICE_PROTECT, "STOP_LOSS",
            )
        except Exception:
            try:
                self.client.cancel_order(symbol, tp_cid)
            except Exception:
                pass
            raise
        bracket = {
            "tp": {"client_id": tp_cid, "order_id": tp_resp.get("orderId"), "stop_price": str(tp),
                   "type": "TAKE_PROFIT_MARKET", "status": tp_resp.get("status", "NEW")},
            "sl": {"client_id": sl_cid, "order_id": sl_resp.get("orderId"), "stop_price": str(sl),
                   "type": "STOP_MARKET", "status": sl_resp.get("status", "NEW")},
            "working_type": PROTECTIVE_WORKING_TYPE,
            "installed_at": now_iso(),
        }
        logger.info(f"NATIVE BRACKET | {strategy_id} | {symbol} {side} qty={qty} | TP={tp} cid={tp_cid} | SL={sl} cid={sl_cid}")
        return bracket


    def cancel_bracket(self, symbol: str, bracket: Optional[Dict[str, Any]]) -> None:
        if not bracket or not LIVE_TRADING:
            return
        for key in ("tp", "sl"):
            cid = str((bracket.get(key) or {}).get("client_id") or "")
            if not cid:
                continue
            try:
                self.client.cancel_order(symbol, cid)
            except Exception as e:
                logger.warning(f"CANCEL PROTECTION FAIL | {symbol} | {cid} | {e}")

    def consume_bracket_fill(self, strategy_id: str, symbol: str, leg: Dict[str, Any],
                             bracket: Optional[Dict[str, Any]], ref_price: Decimal) -> Optional[Dict[str, Any]]:
        if not LIVE_TRADING or not bracket:
            return None
        triggered_key = None
        triggered = None
        for key in ("tp", "sl"):
            meta = bracket.get(key) or {}
            cid = str(meta.get("client_id") or "")
            if not cid:
                continue
            try:
                q = self.client.query_order(symbol, cid)
            except AsterAPIError as e:
                if e.code in (-2011, -2013):
                    continue
                raise
            status = str(q.get("status", ""))
            meta["status"] = status
            if status in ("FILLED", "PARTIALLY_FILLED") and dec(q.get("executedQty")) > 0:
                triggered_key = key
                triggered = q
                break
        if not triggered:
            return None

        sibling = "sl" if triggered_key == "tp" else "tp"
        sibling_cid = str((bracket.get(sibling) or {}).get("client_id") or "")
        if sibling_cid:
            try:
                self.client.cancel_order(symbol, sibling_cid)
            except Exception as e:
                logger.warning(f"CANCEL SIBLING FAIL | {strategy_id} | {sibling_cid} | {e}")

        requested_qty = dec(leg["qty"])
        filled_qty = min(requested_qty, dec(triggered.get("executedQty")))
        avg = dec(triggered.get("avgPrice"))
        if avg <= 0:
            avg = ref_price
            logger.warning(f"NATIVE EXIT AVG AUSENTE | {strategy_id} | cid={triggered.get('clientOrderId')} | ref={ref_price}")

        if filled_qty < requested_qty:
            remaining = requested_qty - filled_qty
            fallback = self.market(strategy_id, symbol, leg["side"], remaining, False, ref_price)
            total_qty = filled_qty + fallback["qty"]
            if total_qty > 0:
                avg = (avg * filled_qty + fallback["price"] * fallback["qty"]) / total_qty
                filled_qty = total_qty

        reason = "NATIVE_TAKE_PROFIT" if triggered_key == "tp" else "NATIVE_STOP_LOSS"
        logger.warning(f"NATIVE EXIT FILLED | {strategy_id} | {reason} | qty={filled_qty} avg={avg}")
        return self._close_record(
            strategy_id, symbol, leg, min(requested_qty, filled_qty), avg, reason,
            str(triggered.get("clientOrderId") or (bracket.get(triggered_key) or {}).get("client_id") or ""),
            "ASTER_CONDITIONAL",
        )

    def install_basket_exit(self, strategy_id: str, symbol: str, legs: List[Dict[str, Any]],
                            target_price: Decimal, ref_price: Decimal) -> Optional[Dict[str, Any]]:
        if not NATIVE_PROTECTIVE_ORDERS or not legs:
            return None
        target_direction = "UP" if target_price >= ref_price else "DOWN"
        trigger = self.rules.trigger_price(symbol, target_price, target_direction)
        grouped: Dict[str, Decimal] = {}
        for leg in legs:
            side = str(leg["side"])
            grouped[side] = grouped.get(side, D(0)) + dec(leg["qty"])
        orders: List[Dict[str, Any]] = []
        placed: List[str] = []
        try:
            for position_side, qty in grouped.items():
                close_side = self.order_side(position_side, False)
                if target_direction == "UP":
                    order_type = "TAKE_PROFIT_MARKET" if position_side == "LONG" else "STOP_MARKET"
                else:
                    order_type = "STOP_MARKET" if position_side == "LONG" else "TAKE_PROFIT_MARKET"
                cid = self.client_id(strategy_id, f"bx{position_side[0].lower()}")
                if not LIVE_TRADING:
                    resp = {"orderId": f"SIM-{cid}", "status": "NEW"}
                else:
                    resp = self.orders.submit_conditional(
                        strategy_id, symbol, position_side, close_side, qty, trigger, cid, order_type,
                        PROTECTIVE_WORKING_TYPE, PROTECTIVE_PRICE_PROTECT, "BASKET_EXIT",
                    )
                    placed.append(cid)
                orders.append({
                    "position_side": position_side,
                    "qty": str(qty),
                    "client_id": cid,
                    "order_id": resp.get("orderId"),
                    "type": order_type,
                    "stop_price": str(trigger),
                    "status": resp.get("status", "NEW"),
                })
        except Exception:
            if LIVE_TRADING:
                for cid in placed:
                    try:
                        self.client.cancel_order(symbol, cid)
                    except Exception:
                        pass
            raise
        logger.info(f"NATIVE BASKET EXIT | {strategy_id} | {symbol} target={trigger} orders={[(x['position_side'], x['qty'], x['type'], x['client_id']) for x in orders]}")
        return {"target_price": str(trigger), "orders": orders, "installed_at": now_iso()}

    def cancel_basket_exit(self, symbol: str, native_exit: Optional[Dict[str, Any]]) -> None:
        if not native_exit or not LIVE_TRADING:
            return
        for meta in native_exit.get("orders", []):
            cid = str(meta.get("client_id") or "")
            if not cid:
                continue
            try:
                self.client.cancel_order(symbol, cid)
            except Exception as e:
                logger.warning(f"CANCEL BASKET EXIT FAIL | {symbol} | {cid} | {e}")

    def basket_exit_health(self, symbol: str, native_exit: Optional[Dict[str, Any]]) -> str:
        if not native_exit:
            return "MISSING"
        if not LIVE_TRADING:
            return "LIVE"
        expected = [str(x.get("client_id") or "") for x in native_exit.get("orders", []) if x.get("client_id")]
        if not expected:
            return "MISSING"
        try:
            rows = self.client.open_orders(symbol)
            if not isinstance(rows, list):
                return "UNKNOWN"
            live_cids = {
                str(r.get("clientOrderId") or r.get("origClientOrderId") or "")
                for r in rows
                if str(r.get("status", "NEW")) in ("NEW", "PARTIALLY_FILLED")
            }
            missing = [cid for cid in expected if cid not in live_cids]
            if missing:
                logger.warning("NATIVE BASKET EXIT AUSENTE | %s | missing=%s esperado=%s", symbol, missing, expected)
                return "MISSING"
            return "LIVE"
        except Exception as e:
            logger.warning("NATIVE BASKET EXIT VERIFY UNKNOWN | %s | %s", symbol, e)
            return "UNKNOWN"

    def basket_exit_is_live(self, symbol: str, native_exit: Optional[Dict[str, Any]]) -> bool:
        return self.basket_exit_health(symbol, native_exit) == "LIVE"

    def consume_basket_exit(self, strategy_id: str, symbol: str, legs: List[Dict[str, Any]],
                            native_exit: Optional[Dict[str, Any]], ref_price: Decimal,
                            close_reason: str = "NATIVE_RANGE_BASKET_TAKE_PROFIT"
                            ) -> Optional[Tuple[Decimal, List[Dict[str, Any]]]]:
        if not LIVE_TRADING or not native_exit:
            return None
        orders = native_exit.get("orders", [])
        snapshots: Dict[str, Dict[str, Any]] = {}
        any_fill = False
        for meta in orders:
            cid = str(meta.get("client_id") or "")
            if not cid:
                continue
            try:
                q = self.client.query_order(symbol, cid)
            except AsterAPIError as e:
                if e.code in (-2011, -2013):
                    continue
                raise
            snapshots[str(meta["position_side"])] = q
            meta["status"] = str(q.get("status", ""))
            if meta["status"] in ("FILLED", "PARTIALLY_FILLED") and dec(q.get("executedQty")) > 0:
                any_fill = True
        if not any_fill:
            return None

        deadline = time.time() + min(2.0, ORDER_FILL_WAIT_SECONDS)
        while time.time() < deadline:
            pending = False
            for meta in orders:
                ps = str(meta["position_side"])
                q = snapshots.get(ps, {})
                if str(q.get("status", "")) == "FILLED":
                    continue
                cid = str(meta.get("client_id") or "")
                try:
                    q = self.client.query_order(symbol, cid)
                    snapshots[ps] = q
                    meta["status"] = str(q.get("status", ""))
                except Exception:
                    pass
                if str((snapshots.get(ps) or {}).get("status", "")) != "FILLED":
                    pending = True
            if not pending:
                break
            time.sleep(ORDER_POLL_SECONDS)

        for meta in orders:
            ps = str(meta["position_side"])
            q = snapshots.get(ps, {})
            if str(q.get("status", "")) != "FILLED":
                cid = str(meta.get("client_id") or "")
                if cid:
                    try:
                        self.client.cancel_order(symbol, cid)
                    except Exception:
                        pass

        side_avg: Dict[str, Decimal] = {}
        side_qty: Dict[str, Decimal] = {}
        for meta in orders:
            ps = str(meta["position_side"])
            wanted = dec(meta.get("qty"))
            q = snapshots.get(ps, {})
            filled = min(wanted, dec(q.get("executedQty")))
            avg = dec(q.get("avgPrice"))
            if filled > 0 and avg <= 0:
                avg = ref_price
            remaining = max(D(0), wanted - filled)
            if remaining > 0:
                fallback = self.market(strategy_id, symbol, ps, remaining, False, ref_price)
                totalq = filled + fallback["qty"]
                avg = ((avg * filled) + (fallback["price"] * fallback["qty"])) / totalq if totalq > 0 else ref_price
                filled = totalq
            side_avg[ps] = avg if avg > 0 else ref_price
            side_qty[ps] = filled

        closes: List[Dict[str, Any]] = []
        total = D(0)
        remaining_by_side = dict(side_qty)
        for leg in legs:
            ps = str(leg["side"])
            leg_qty = dec(leg["qty"])
            alloc = min(leg_qty, remaining_by_side.get(ps, D(0)))
            if alloc <= 0:
                raise RuntimeError(f"Native basket exit sem quantidade suficiente para {strategy_id} {ps}")
            cid = str((snapshots.get(ps) or {}).get("clientOrderId") or "NATIVE_BASKET")
            rec = self._close_record(strategy_id, symbol, leg, alloc, side_avg[ps],
                                     close_reason, cid, "ASTER_CONDITIONAL_BASKET")
            closes.append(rec)
            total += dec(rec["pnl_est"])
            remaining_by_side[ps] = remaining_by_side.get(ps, D(0)) - alloc
        logger.warning(f"NATIVE BASKET EXIT FILLED | {strategy_id} | pnl={total} | target={native_exit.get('target_price')}")
        return total, closes

# -----------------------------------------------------------------------------
# OWNERSHIP / INTERFERENCE CONTROL
# -----------------------------------------------------------------------------

def acquire_owner(store: StateStore, symbol: str, strategy_id: str) -> bool:
    if ALLOW_MULTI_STRATEGY_SAME_SYMBOL:
        return True
    with store.lock:
        owner = store.state["symbol_owner"].get(symbol)
        if owner in (None, strategy_id):
            store.state["symbol_owner"][symbol] = strategy_id
            store.save()
            return True
        return False

def release_owner(store: StateStore, symbol: str, strategy_id: str) -> None:
    if ALLOW_MULTI_STRATEGY_SAME_SYMBOL:
        return
    with store.lock:
        if store.state["symbol_owner"].get(symbol) == strategy_id:
            store.state["symbol_owner"][symbol] = None
            store.save()

# -----------------------------------------------------------------------------
# UTILITY: apply realized PnL to strategy state
# -----------------------------------------------------------------------------

def _apply_realized_pnl_to_state(st: Dict[str, Any], pnl: Decimal, exit_price: Decimal,
                                 close_reason: str) -> None:
    before = dec(st.get("equity"))
    after = before + pnl
    st["equity"] = str(after)
    st["realized_pnl"] = str(dec(st.get("realized_pnl")) + pnl)
    rd_before = dec(st.get("recovery_deficit"))
    if pnl < 0:
        rd_after = rd_before + (-pnl)
        st["losses"] = int(st.get("losses", 0)) + 1
        st["last_result"] = "LOSS"
    elif pnl > 0:
        rd_after = max(D(0), rd_before - pnl)
        st["wins"] = int(st.get("wins", 0)) + 1
        st["last_result"] = "WIN"
    else:
        rd_after = rd_before
        st["last_result"] = "FLAT"
    st["recovery_deficit"] = str(rd_after)
    st["last_update"] = now_iso()

# -----------------------------------------------------------------------------
# RANGE ENGINE
# -----------------------------------------------------------------------------

class RangeEngine:
    def __init__(self, symbol: str, client: AsterClient, md: MarketData, news: NewsFilter,
                 account: AccountManager, exe: ExecutionEngine, store: StateStore,
                 allow_new_entries: bool = True):
        self.symbol = symbol
        self.allow_new_entries = allow_new_entries
        self.id = f"RANGE:{symbol}"
        self.client = client
        self.md = md
        self.news = news
        self.account = account
        self.exe = exe
        self.store = store

    def st(self) -> Dict[str, Any]:
        return self.store.state["range"][self.symbol]

    def _other_strategy_reserved_qty(self, position_side: str) -> Decimal:
        total = D(0)
        wanted = str(position_side).upper()
        with self.store.lock:
            for bucket in ("pyramid", "pyramid_grids"):
                for pst in self.store.state.get(bucket, {}).values():
                    pst = pst or {}
                    if str(pst.get("symbol", "")).upper() != self.symbol:
                        continue
                    if str(pst.get("side", "")).upper() != wanted:
                        continue
                    for leg in pst.get("legs", []) or []:
                        total += dec(leg.get("qty"))
        return total

    def _range_physical_capacity(self, position_side: str) -> Decimal:
        actual = self.exe.physical_position_qty(self.symbol, position_side)
        reserved = self._other_strategy_reserved_qty(position_side)
        return max(D(0), actual - reserved)

    def _reconcile_range_ghost_legs(self, b: Dict[str, Any], price: Decimal) -> bool:
        if not LIVE_TRADING:
            return False
        legs = list(b.get("legs") or [])
        if not legs:
            return False
        changed = False
        rebuilt: List[Dict[str, Any]] = []
        by_side_capacity = {
            "LONG": self._range_physical_capacity("LONG"),
            "SHORT": self._range_physical_capacity("SHORT"),
        }
        used = {"LONG": D(0), "SHORT": D(0)}
        for leg in legs:
            side = str(leg.get("side", "")).upper()
            qty = dec(leg.get("qty"))
            if side not in ("LONG", "SHORT") or qty <= 0:
                continue
            available = max(D(0), by_side_capacity[side] - used[side])
            keep = min(qty, available)
            step = self.exe.rules.rules[self.symbol].step_size
            keep = floor_step(keep, step)
            if keep <= 0:
                changed = True
                logger.warning(
                    f"RANGE GHOST LEG REMOVIDA | {self.symbol} | side={side} leg={leg.get('id')} virtual_qty={qty} "
                    f"physical_capacity={by_side_capacity[side]} reserved_other={self._other_strategy_reserved_qty(side)}"
                )
                continue
            if keep < qty:
                changed = True
                new_leg = dict(leg)
                new_leg["qty"] = str(keep)
                new_leg["notional"] = str(keep * dec(new_leg.get("entry_price")))
                leg = new_leg
                logger.warning(
                    f"RANGE GHOST LEG REDUZIDA | {self.symbol} | side={side} leg={leg.get('id')} old_qty={qty} new_qty={keep}"
                )
            rebuilt.append(leg)
            used[side] += keep
        if not changed:
            return False
        self.exe.cancel_basket_exit(self.symbol, b.get("native_basket_exit"))
        self.exe.cancel_basket_exit(self.symbol, b.get("native_basket_stop"))
        if b.get("native_bracket"):
            self.exe.cancel_bracket(self.symbol, b.get("native_bracket"))
        b["native_basket_exit"] = None
        b["native_basket_stop"] = None
        b["native_bracket"] = None
        b["legs"] = rebuilt
        st = self.st()
        if not rebuilt:
            st["basket"] = None
            st["status"] = "PROTECT" if dec(st.get("recovery_deficit")) > 0 else "IDLE"
            st["anchor"] = str(price)
            st["protect_anchor"] = str(price) if st["status"] == "PROTECT" else None
            st["last_result"] = "RECONCILED_ALREADY_CLOSED"
            st["last_update"] = now_iso()
            self.store.save()
            release_owner(self.store, self.symbol, self.id)
            logger.warning(
                f"RANGE BASKET RECONCILIADO | {self.symbol} | nenhuma quantidade RANGE restante na Aster | "
                f"status={st['status']} equity_preservada={st.get('equity')} RD_preservado={st.get('recovery_deficit')}"
            )
            return True
        b["active_side"] = str(rebuilt[-1].get("side"))
        st["last_update"] = now_iso()
        self.store.save()
        return False

    def _new_anchor(self, price: Decimal) -> None:
        st = self.st()
        st["anchor"] = str(price)
        st["status"] = "IDLE"
        st["basket"] = None
        st["failures"] = 0
        st["protect_anchor"] = None
        st["last_update"] = now_iso()
        self.store.save()
        release_owner(self.store, self.symbol, self.id)
        logger.info(f"RANGE ANCHOR | {self.symbol} | anchor={price}")

    def _target_recovery_profit(self, st: Dict[str, Any], basket: Optional[Dict[str, Any]] = None) -> Decimal:
        rd = dec(st.get("recovery_deficit"))
        if basket:
            pass
        return rd * RECOVERY_MULTIPLIER if rd > 0 else D(0)

    @staticmethod
    def unrealized(legs: List[Dict[str, Any]], price: Decimal) -> Decimal:
        total = D(0)
        for leg in legs:
            q = dec(leg["qty"]); ep = dec(leg["entry_price"])
            total += (price - ep) * q if leg["side"] == "LONG" else (ep - price) * q
        return total

    @staticmethod
    def estimated_net_pnl(legs: List[Dict[str, Any]], exit_price: Decimal) -> Decimal:
        fee_rate = D(os.getenv("TAKER_FEE_RATE", "0.00035"))
        total = D(0)
        for leg in legs:
            qty = dec(leg["qty"])
            entry = dec(leg["entry_price"])
            gross = (exit_price - entry) * qty if leg["side"] == "LONG" else (entry - exit_price) * qty
            fees = (entry * qty + exit_price * qty) * fee_rate
            total += gross - fees
        return total

    def dynamic_recovery_notional(self, st: Dict[str, Any], basket: Dict[str, Any],
                                  new_side: str, entry_price: Decimal,
                                  recovery_level: int) -> Tuple[Decimal, Decimal, Decimal]:
        tp_price = entry_price * (D(1) + RANGE_TAKE_PROFIT_PCT) \
            if new_side == "LONG" else entry_price * (D(1) - RANGE_TAKE_PROFIT_PCT)
        existing_at_tp = self.estimated_net_pnl(basket.get("legs", []), tp_price)
        base_notional = max(configured_initial_notional(self.symbol), dec(st.get("equity")))
        desired_basket_profit = dec(st.get("recovery_deficit")) + base_notional * RANGE_TAKE_PROFIT_PCT
        fee_rate = D(os.getenv("TAKER_FEE_RATE", "0.00035"))
        move_yield = abs(tp_price - entry_price) / entry_price
        round_trip_fee_yield = fee_rate * (D(1) + tp_price / entry_price)
        net_yield = move_yield - round_trip_fee_yield
        if net_yield <= 0:
            raise RuntimeError("RANGE recovery sem rendimento liquido positivo no TP")
        dynamic_notional = max(D(0), (desired_basket_profit - existing_at_tp) / net_yield)
        classic_floor = base_notional * (RECOVERY_MULTIPLIER ** recovery_level)
        requested = max(dynamic_notional, classic_floor)
        capped = min(requested, configured_max_recovery_notional(self.symbol))
        if capped < requested:
            logger.warning(f"RANGE DYNAMIC RECOVERY CAPPED | {self.symbol} | requested={requested} cap={capped} level={recovery_level}")
        return capped, tp_price, existing_at_tp

    def _open(self, side: str, price: Decimal, target_profit: Optional[Decimal], reason: str,
              recovery_level: int = 0,
              desired_notional_override: Optional[Decimal] = None) -> Optional[Dict[str, Any]]:
        st = self.st()
        blocked, why = self.news.blocked()
        if blocked:
            logger.info(f"RANGE BLOQUEADO NEWS | {self.symbol} | {why}")
            return None
        if self.store.killed() != "OFF":
            return None
        gate_ok, gate_reason = self.store.entry_allowed()
        if not gate_ok:
            logger.warning(f"RANGE ENTRY GATE | {self.symbol} | {gate_reason}")
            return None
        if not self.md.is_fresh(self.symbol):
            logger.warning(f"RANGE ENTRY STALE PRICE | {self.symbol} | age_s={self.md.age(self.symbol):.3f}")
            return None
        if not acquire_owner(self.store, self.symbol, self.id):
            logger.info(f"RANGE BLOQUEADO OWNER | {self.symbol} | owner={self.store.state['symbol_owner'].get(self.symbol)}")
            return None
        sizing = self.account.sizing_for_profit_target(
            self.symbol, price, st, target_profit, RANGE_TAKE_PROFIT_PCT, RANGE_HARD_STOP_PCT,
            recovery_level=recovery_level,
            desired_notional_override=desired_notional_override,
            recovery_multiplier=RECOVERY_MULTIPLIER,
        )
        if not sizing:
            release_owner(self.store, self.symbol, self.id)
            logger.warning(f"RANGE SIZING NAO CABE | {self.symbol} | target={target_profit}")
            return None
        logger.info(f"RANGE SIZING | {self.symbol} | side={side} target={target_profit} lev={sizing['leverage']}x notional={sizing['notional']} margin={sizing['margin']} qty={sizing['qty']} meta={sizing['meta']}")
        leg = self.exe.open_leg(self.id, self.symbol, side, sizing, reason)
        if not leg:
            release_owner(self.store, self.symbol, self.id)
            return None
        return leg

    def _start_basket(self, side: str, price: Decimal) -> None:
        st = self.st()
        rd = dec(st.get("recovery_deficit"))
        target = rd * RECOVERY_MULTIPLIER if rd > 0 else None
        leg = self._open(side, price, target, "RANGE_INITIAL" if rd == 0 else "RANGE_REARM_RECOVERY",
                         recovery_level=1 if rd > 0 else 0)
        if not leg:
            return
        anchor = dec(st["anchor"])
        entry = dec(leg["entry_price"])
        tp_price = entry * (D(1) + RANGE_TAKE_PROFIT_PCT) if side == "LONG" else entry * (D(1) - RANGE_TAKE_PROFIT_PCT)
        hard_stop_price = entry * (D(1) - RANGE_HARD_STOP_PCT) if side == "LONG" else entry * (D(1) + RANGE_HARD_STOP_PCT)
        native_bracket = self.exe.install_bracket(self.id, self.symbol, leg, tp_price, hard_stop_price)
        st["status"] = "BASKET"
        st["basket"] = {
            "origin_anchor": str(anchor),
            "initial_side": side,
            "signal_entry": str(price),
            "initial_entry": str(entry),
            "legs": [leg],
            "active_side": side,
            "alternations": 0,
            "next_reverse_price": str(anchor),
            "tp_price": str(tp_price),
            "hard_stop_price": str(hard_stop_price),
            "native_bracket": native_bracket,
            "native_basket_exit": None,
            "native_basket_stop": None,
            "started_at": now_iso(),
        }
        st["last_update"] = now_iso()
        self.store.save()
        logger.info(f"RANGE BASKET START | {self.symbol} | {side} signal={price} fill={entry} | anchor={anchor} TP={tp_price} SL={hard_stop_price}")

    def _close_basket(self, price: Decimal, reason: str, protect_after: bool = False) -> None:
        st = self.st(); b = st.get("basket")
        if not b:
            return
        rd_before = dec(st.get("recovery_deficit"))
        recovery_attempt = int(b.get("alternations", 0)) > 0 or rd_before > 0
        recovery_tp_hit = reason == "RECOVERY_LEG_TP_1PCT_CLOSE_ALL"

        self.exe.cancel_bracket(self.symbol, b.get("native_bracket"))
        self.exe.cancel_basket_exit(self.symbol, b.get("native_basket_exit"))
        self.exe.cancel_basket_exit(self.symbol, b.get("native_basket_stop"))
        closes = []
        pnl = D(0)
        reserved_used = {"LONG": D(0), "SHORT": D(0)}
        for _leg in list(b.get("legs", [])):
            _side = str(_leg.get("side", "")).upper()
            _reserved_other = self._other_strategy_reserved_qty(_side)
            _actual = self.exe.physical_position_qty(self.symbol, _side)
            _available_range = max(D(0), _actual - _reserved_other - reserved_used.get(_side, D(0)))
            _c = self.exe.close_leg(
                self.id, self.symbol, _leg, price, reason,
                max_physical_qty=_available_range,
            )
            if _c is not None:
                closes.append(_c)
                pnl += dec(_c.get("pnl_est"))
                reserved_used[_side] = reserved_used.get(_side, D(0)) + dec(_c.get("qty"))

        _apply_realized_pnl_to_state(st, pnl, price, reason)
        rd_after = dec(st.get("recovery_deficit"))

        # Um martingale RANGE legado só é considerado ACERTO quando o PnL REALIZADO
        # cobriu integralmente o déficit anterior. Nesse caso o estado operacional
        # é zerado explicitamente; não dependemos de estado legado/migração.
        recovery_success = recovery_attempt and recovery_tp_hit and rd_before > 0 and pnl >= rd_before
        if recovery_success:
            st["recovery_deficit"] = "0"
            rd_after = D(0)
            st["failures"] = 0
            st["status"] = "IDLE"
            st["anchor"] = str(price)
            st["protect_anchor"] = None
            st["last_result"] = "RECOVERY_WIN_RESET"
            protect_after = False
            logger.warning(
                f"RANGE RECOVERY RESET | {self.symbol} | pnl_realizado={pnl} "
                f"rd_before={rd_before} -> RD=0 failures=0 status=IDLE anchor={price}"
            )
        else:
            st["failures"] = 0
            # Se o preço tocou o TP, mas taxas/slippage impediram recuperar todo RD,
            # não marcamos falso acerto. Preserva-se o déficit e entra em proteção.
            if recovery_attempt and recovery_tp_hit and rd_after > 0:
                protect_after = True
                st["last_result"] = "RECOVERY_PARTIAL"
                logger.warning(
                    f"RANGE RECOVERY PARTIAL | {self.symbol} | pnl_realizado={pnl} "
                    f"rd_before={rd_before} rd_restante={rd_after} | mantendo recovery"
                )
            if protect_after:
                st["status"] = "PROTECT"
                st["protect_anchor"] = str(price)
                st["anchor"] = str(price)
            else:
                st["status"] = "IDLE"
                st["anchor"] = str(price)
                st["protect_anchor"] = None

        st["basket"] = None
        st["last_update"] = now_iso()
        self.store.save()
        release_owner(self.store, self.symbol, self.id)
        logger.info(
            f"RANGE CLOSE | {self.symbol} | reason={reason} pnl={pnl} equity={st['equity']} "
            f"RD={st['recovery_deficit']} recovery_success={recovery_success} protect={protect_after}"
        )

    def retire_legacy_range(self, price: Decimal) -> None:
        """Close only legacy RANGE exposure and permanently retire its operational state.

        PYRAMID quantities on the same symbol/side are reserved by _close_basket(), so an
        aggregated Aster position is reduced only by the virtual RANGE quantity.
        """
        st = self.st()
        b = st.get("basket")
        if b:
            logger.warning(
                f"RANGE RETIRE | {self.symbol} | fechando somente lotes RANGE legados; "
                f"legs={len(b.get('legs', []) or [])}"
            )
            self._close_basket(price, "MIGRATION_RETIRE_RANGE_V28", protect_after=False)
            st = self.st()
        # Operational recovery is intentionally discarded because this strategy is retired.
        # Historical PnL/equity remain recorded in the state/trade ledger.
        st["basket"] = None
        st["status"] = "RETIRED"
        st["anchor"] = None
        st["protect_anchor"] = None
        st["failures"] = 0
        st["recovery_deficit"] = "0"
        st["last_result"] = "RETIRED_V28"
        st["last_update"] = now_iso()
        self.store.save()
        release_owner(self.store, self.symbol, self.id)
        logger.warning(f"RANGE RETIRED | {self.symbol} | novas_entradas=DESABILITADAS_PERMANENTEMENTE")

    def _reverse(self, price: Decimal) -> None:
        st = self.st(); b = st.get("basket")
        if not b:
            return
        if int(b.get("alternations", 0)) >= MAX_RECOVERY_FAILURES:
            self._close_basket(price, "MAX_RECOVERY_FAILURES_AFTER_FULL_ATTEMPTS", protect_after=True)
            return
        current = b["active_side"]
        new_side = "SHORT" if current == "LONG" else "LONG"
        mtm = self.unrealized(b["legs"], price)
        accumulated_loss = dec(st.get("recovery_deficit")) + max(D(0), -mtm)
        target = accumulated_loss * RECOVERY_MULTIPLIER
        if target <= 0:
            target = None
        recovery_level = min(int(b.get("alternations", 0)) + 1, MAX_RECOVERY_FAILURES)
        self.exe.cancel_bracket(self.symbol, b.get("native_bracket"))
        self.exe.cancel_basket_exit(self.symbol, b.get("native_basket_exit"))
        self.exe.cancel_basket_exit(self.symbol, b.get("native_basket_stop"))
        b["native_bracket"] = None
        b["native_basket_exit"] = None
        b["native_basket_stop"] = None
        desired_notional, recovery_tp_signal, existing_at_tp = self.dynamic_recovery_notional(
            st, b, new_side, price, recovery_level
        )
        leg = self._open(new_side, price, target, "RANGE_ALTERNATING_RECOVERY",
                         recovery_level=recovery_level,
                         desired_notional_override=desired_notional)
        if not leg:
            if len(b.get("legs", [])) == 1:
                original_leg = b["legs"][0]
                b["native_bracket"] = self.exe.install_bracket(
                    self.id, self.symbol, original_leg,
                    dec(b.get("tp_price")), dec(b.get("hard_stop_price")),
                )
                self.store.save()
            return
        recovery_entry = dec(leg["entry_price"])
        recovery_tp = recovery_entry * (D(1) + RANGE_TAKE_PROFIT_PCT) \
            if new_side == "LONG" else recovery_entry * (D(1) - RANGE_TAKE_PROFIT_PCT)
        b["legs"].append(leg)
        b["active_side"] = new_side
        b["alternations"] = int(b.get("alternations", 0)) + 1
        st["failures"] = b["alternations"]
        if new_side == b["initial_side"]:
            b["next_reverse_price"] = b["origin_anchor"]
        else:
            b["next_reverse_price"] = b["initial_entry"]
        b["recovery_tp_price"] = str(recovery_tp)
        recovery_stop = recovery_entry * (D(1) - RANGE_HARD_STOP_PCT) if new_side == "LONG" else recovery_entry * (D(1) + RANGE_HARD_STOP_PCT)
        b["recovery_stop_price"] = str(recovery_stop)
        try:
            b["native_basket_exit"] = self.exe.install_basket_exit(
                self.id + ":TP", self.symbol, b["legs"], recovery_tp, recovery_entry
            )
        except Exception as e:
            b["native_basket_exit"] = None
            logger.exception(f"RANGE NATIVE BASKET TP FAIL | {self.symbol} | {e}")
        try:
            b["native_basket_stop"] = self.exe.install_basket_exit(
                self.id + ":SL", self.symbol, b["legs"], recovery_stop, recovery_entry
            )
        except Exception as e:
            b["native_basket_stop"] = None
            logger.exception(f"RANGE NATIVE BASKET SL FAIL | {self.symbol} | {e}")
        logger.warning(f"RANGE RECOVERY PROTECTION | {self.symbol} | TP={recovery_tp} SL={recovery_stop} | native_tp={bool(b.get('native_basket_exit'))} native_sl={bool(b.get('native_basket_stop'))}")
        st["last_update"] = now_iso()
        self.store.save()
        logger.warning(f"RANGE REVERSE 4X DINAMICO | {self.symbol} | new={new_side} @{recovery_entry} | mtm={mtm} existing_at_tp={existing_at_tp} desired_notional={desired_notional} recovery_tp={recovery_tp} failures={st['failures']}")

    def tick(self, price: Decimal) -> None:
        with self.store.lock:
            st = self.st()
            if st.get("anchor") is None:
                if not self.allow_new_entries:
                    return
                self._new_anchor(price)
                return
            status = st.get("status", "IDLE")
            anchor = dec(st["anchor"])
            if status == "PROTECT":
                pa = dec(st.get("protect_anchor") or anchor)
                move = abs(pct_change(pa, price))
                if move >= RANGE_REARM_PCT:
                    st["status"] = "IDLE"
                    st["anchor"] = str(price)
                    st["protect_anchor"] = None
                    st["failures"] = 0
                    self.store.save()
                    logger.info(f"RANGE PROTECT LIBERADO | {self.symbol} | move={move} | new_anchor={price} | RD={st['recovery_deficit']}")
                return
            if status == "IDLE":
                if not self.allow_new_entries:
                    return
                up = anchor * (D(1) + RANGE_TRIGGER_PCT)
                dn = anchor * (D(1) - RANGE_TRIGGER_PCT)
                if price >= up:
                    self._start_basket("LONG", price)
                elif price <= dn:
                    self._start_basket("SHORT", price)
                return
            b = st.get("basket")
            if not b:
                st["status"] = "IDLE"; self.store.save(); return
            active = b["active_side"]
            alternations = int(b.get("alternations", 0))
            if alternations == 0 and not b.get("native_bracket") and NATIVE_PROTECTIVE_ORDERS:
                try:
                    b["native_bracket"] = self.exe.install_bracket(
                        self.id, self.symbol, b["legs"][0],
                        dec(b.get("tp_price")), dec(b.get("hard_stop_price")),
                    )
                    self.store.save()
                except Exception as e:
                    logger.exception(f"RANGE BRACKET INSTALL FAIL | {self.symbol} | {e}")
            if alternations == 0 and b.get("native_bracket"):
                native_close = self.exe.consume_bracket_fill(
                    self.id, self.symbol, b["legs"][0], b.get("native_bracket"), price
                )
                if native_close:
                    pnl = dec(native_close["pnl_est"])
                    _apply_realized_pnl_to_state(st, pnl, dec(native_close["exit_price"]), native_close["reason"])
                    protect_after = pnl < 0
                    st["basket"] = None
                    st["failures"] = 0
                    st["status"] = "PROTECT" if protect_after else "IDLE"
                    st["protect_anchor"] = str(dec(native_close["exit_price"])) if protect_after else None
                    st["anchor"] = str(dec(native_close["exit_price"]))
                    self.store.save()
                    release_owner(self.store, self.symbol, self.id)
                    logger.info(f"RANGE NATIVE CLOSE | {self.symbol} | pnl={pnl} equity={st['equity']} RD={st['recovery_deficit']} protect={protect_after}")
                    return
            if alternations == 0:
                tp = dec(b["tp_price"])
                hard = dec(b["hard_stop_price"])
                if (active == "LONG" and price >= tp) or (active == "SHORT" and price <= tp):
                    self._close_basket(price, "INITIAL_TP_1PCT", protect_after=False)
                    return
                if (b["initial_side"] == "LONG" and price <= hard) or (b["initial_side"] == "SHORT" and price >= hard):
                    self._close_basket(price, "INITIAL_HARD_STOP_2PCT", protect_after=True)
                    return
            else:
                active_recovery_side = str(b.get("active_side") or "")
                active_recovery_leg = None
                for _leg in reversed(b.get("legs", [])):
                    if str(_leg.get("side") or "") == active_recovery_side:
                        active_recovery_leg = _leg
                        break
                if active_recovery_leg:
                    _re = dec(active_recovery_leg.get("entry_price"))
                    if _re > 0:
                        expected_rtp = _re * (D(1) + RANGE_TAKE_PROFIT_PCT) if active_recovery_side == "LONG" else _re * (D(1) - RANGE_TAKE_PROFIT_PCT)
                        expected_rsl = _re * (D(1) - RANGE_HARD_STOP_PCT) if active_recovery_side == "LONG" else _re * (D(1) + RANGE_HARD_STOP_PCT)
                        stored_rtp = dec(b.get("recovery_tp_price"))
                        stored_rsl = dec(b.get("recovery_stop_price"))
                        tick = dec(self.exe.rules.rules[self.symbol].tick_size)
                        tol = max(tick * D(2), _re * D("0.000001"))
                        if abs(stored_rtp - expected_rtp) > tol or abs(stored_rsl - expected_rsl) > tol:
                            logger.warning(
                                f"RANGE RECOVERY PRICE MIGRATION | {self.symbol} | side={active_recovery_side} entry={_re} | old_tp={stored_rtp} old_sl={stored_rsl} -> new_tp={expected_rtp} new_sl={expected_rsl}"
                            )
                            self.exe.cancel_basket_exit(self.symbol, b.get("native_basket_exit"))
                            self.exe.cancel_basket_exit(self.symbol, b.get("native_basket_stop"))
                            b["native_basket_exit"] = None
                            b["native_basket_stop"] = None
                            b["recovery_tp_price"] = str(expected_rtp)
                            b["recovery_stop_price"] = str(expected_rsl)
                            self.store.save()
                rtp = dec(b.get("recovery_tp_price"))
                rsl = dec(b.get("recovery_stop_price"))
                native_result = None
                if b.get("native_basket_exit"):
                    native_result = self.exe.consume_basket_exit(
                        self.id, self.symbol, b.get("legs", []),
                        b.get("native_basket_exit"), price,
                        "NATIVE_RANGE_BASKET_TAKE_PROFIT",
                    )
                if native_result:
                    self.exe.cancel_basket_exit(self.symbol, b.get("native_basket_stop"))
                    pnl, closes = native_result
                    _apply_realized_pnl_to_state(st, pnl, price, "NATIVE_RANGE_BASKET_TAKE_PROFIT")
                    st["basket"] = None
                    st["failures"] = 0
                    st["status"] = "IDLE"
                    st["anchor"] = str(price)
                    st["protect_anchor"] = None
                    self.store.save()
                    release_owner(self.store, self.symbol, self.id)
                    logger.info(f"RANGE NATIVE BASKET TP CLOSE | {self.symbol} | pnl={pnl} equity={st['equity']} RD={st['recovery_deficit']}")
                    return
                native_stop = None
                if b.get("native_basket_stop"):
                    native_stop = self.exe.consume_basket_exit(
                        self.id, self.symbol, b.get("legs", []),
                        b.get("native_basket_stop"), price,
                        "NATIVE_RANGE_BASKET_STOP_LOSS",
                    )
                if native_stop:
                    self.exe.cancel_basket_exit(self.symbol, b.get("native_basket_exit"))
                    pnl, closes = native_stop
                    _apply_realized_pnl_to_state(st, pnl, price, "NATIVE_RANGE_BASKET_STOP_LOSS")
                    st["basket"] = None
                    st["status"] = "PROTECT"
                    st["protect_anchor"] = str(price)
                    st["anchor"] = str(price)
                    self.store.save()
                    release_owner(self.store, self.symbol, self.id)
                    logger.warning(f"RANGE NATIVE BASKET SL CLOSE | {self.symbol} | pnl={pnl} equity={st['equity']} RD={st['recovery_deficit']}")
                    return
                if self._reconcile_range_ghost_legs(b, price):
                    return
                b = st.get("basket")
                if not b:
                    return
                active = b["active_side"]
                rtp = dec(b.get("recovery_tp_price"))
                rsl = dec(b.get("recovery_stop_price"))
                if NATIVE_PROTECTIVE_ORDERS and b.get("native_basket_exit") and not self.exe.basket_exit_is_live(self.symbol, b.get("native_basket_exit")):
                    self.exe.cancel_basket_exit(self.symbol, b.get("native_basket_exit"))
                    b["native_basket_exit"] = None
                    self.store.save()
                if NATIVE_PROTECTIVE_ORDERS and b.get("native_basket_stop") and not self.exe.basket_exit_is_live(self.symbol, b.get("native_basket_stop")):
                    self.exe.cancel_basket_exit(self.symbol, b.get("native_basket_stop"))
                    b["native_basket_stop"] = None
                    self.store.save()
                if rtp > 0 and not b.get("native_basket_exit") and NATIVE_PROTECTIVE_ORDERS:
                    try:
                        b["native_basket_exit"] = self.exe.install_basket_exit(
                            self.id + ":TP", self.symbol, b.get("legs", []), rtp, price
                        )
                        self.store.save()
                        logger.warning(f"RANGE BASKET TP REINSTALADO | {self.symbol} | trigger={rtp}")
                    except Exception as e:
                        logger.exception(f"RANGE BASKET TP REINSTALL FAIL | {self.symbol} | {e}")
                if rsl > 0 and not b.get("native_basket_stop") and NATIVE_PROTECTIVE_ORDERS:
                    try:
                        b["native_basket_stop"] = self.exe.install_basket_exit(
                            self.id + ":SL", self.symbol, b.get("legs", []), rsl, price
                        )
                        self.store.save()
                        logger.warning(f"RANGE BASKET SL REINSTALADO | {self.symbol} | trigger={rsl}")
                    except Exception as e:
                        logger.exception(f"RANGE BASKET SL REINSTALL FAIL | {self.symbol} | {e}")
                if rtp > 0 and ((active == "LONG" and price >= rtp) or (active == "SHORT" and price <= rtp)):
                    self._close_basket(price, "RECOVERY_LEG_TP_1PCT_CLOSE_ALL", protect_after=False); return
                if rsl > 0 and ((active == "LONG" and price <= rsl) or (active == "SHORT" and price >= rsl)):
                    self._close_basket(price, "RECOVERY_HARD_STOP_2PCT_CLOSE_ALL", protect_after=True); return
            rev = dec(b["next_reverse_price"])
            if (active == "LONG" and price <= rev) or (active == "SHORT" and price >= rev):
                self._reverse(price)


# -----------------------------------------------------------------------------
# PYRAMID 1% ENGINE - LONG e SHORT independentes, sem indicador
# -----------------------------------------------------------------------------

class PyramidEngine:
    """Escada direcional persistente baseada exclusivamente em deslocamentos de 1% do anchor.

    LONG: +1%, +2%, +3% ...; SHORT: -1%, -2%, -3% ...
    A arquitetura atual separa contabilidade lógica de capacidade física:
      - nível 1 usa PYRAMID_INITIAL_NOTIONAL_USD;
      - níveis seguintes usam uma fração da MARGEM FÍSICA LIVRE REAL da conta
        (availableBalance - MIN_FREE_WALLET_BUFFER_USD), convertida em notional
        pela alavancagem PYRAMID;
      - BTC mantém PYRAMID_BTC_MIN_ADD_NOTIONAL_USD como piso;
      - bankroll/equity virtual permanece para PnL e limite de perda, mas não
        aumenta sozinho o tamanho das novas adições.
    As regras da exchange arredondam para step/min-notional quando necessário.
    Recuos nunca reduzem posição. O encerramento automático permanece pelo
    limite explícito PYRAMID_MAX_LOSS_USD.
    """
    def __init__(self, symbol: str, side: str, client: AsterClient, md: MarketData,
                 news: NewsFilter, account: AccountManager, exe: ExecutionEngine, store: StateStore,
                 state_bucket: str = "pyramid", state_key: Optional[str] = None,
                 grid_id: str = "LEGACY", grid_phase: Decimal = D(0),
                 allow_new_entries: bool = True):
        self.symbol = symbol
        self.side = side.upper()
        self.state_bucket = state_bucket
        self.state_key = state_key or f"{symbol}:{self.side}"
        self.grid_id = grid_id
        self.grid_phase = grid_phase
        self.allow_new_entries = allow_new_entries
        self.id = f"PYRAMID:{symbol}:{self.side}" if grid_id == "LEGACY" else f"PYRAMID:{symbol}:{self.side}:{grid_id}"
        self.client = client; self.md = md; self.news = news
        self.account = account; self.exe = exe; self.store = store
        self._last_native_stop_refresh = 0.0

    def st(self) -> Dict[str, Any]:
        return self.store.state[self.state_bucket][self.state_key]

    def _trigger_price(self, anchor: Decimal, level: int) -> Decimal:
        if self.side == "LONG":
            raw = anchor * (D(1) + PYRAMID_STEP_PCT * D(level))
            return self.exe.rules.trigger_price(self.symbol, raw, "UP")
        raw = anchor * (D(1) - PYRAMID_STEP_PCT * D(level))
        if raw <= 0:
            return D(0)
        return self.exe.rules.trigger_price(self.symbol, raw, "DOWN")

    def _crossed(self, price: Decimal, trigger: Decimal) -> bool:
        return price >= trigger if self.side == "LONG" else price <= trigger

    def _net_unrealized(self, price: Decimal) -> Decimal:
        st = self.st()
        fee_rate = D(os.getenv("TAKER_FEE_RATE", "0.00035"))
        total = D(0)
        for leg in st.get("legs", []) or []:
            q = dec(leg.get("qty")); ep = dec(leg.get("entry_price"))
            if q <= 0 or ep <= 0:
                continue
            gross = (price - ep) * q if self.side == "LONG" else (ep - price) * q
            # inclui fee de entrada + fee estimada de saída para disparar o limite de forma conservadora
            fees = (ep * q + price * q) * fee_rate
            total += gross - fees
        return total

    def _desired_notional(self, level: int, free_margin: Optional[Decimal] = None) -> Decimal:
        share = dec(self.st().get("capital_share", "1"))
        if level <= 1:
            return PYRAMID_INITIAL_NOTIONAL_USD * share

        if free_margin is None:
            free_margin = self.account.free_margin()
        free_margin = max(D(0), dec(free_margin))
        margin_budget = free_margin * PYRAMID_ADD_FREE_MARGIN_PCT * share
        desired = margin_budget * D(PYRAMID_LEVERAGE)

        if self.symbol == "BTCUSDT":
            desired = max(PYRAMID_BTC_MIN_ADD_NOTIONAL_USD * share, desired)
        return desired

    def _sizing(self, price: Decimal, level: int) -> Optional[Dict[str, Any]]:
        self.account.sync()
        free = self.account.free_margin()
        desired = self._desired_notional(level, free_margin=free)
        if desired <= 0 or price <= 0:
            return None
        lev = max(MIN_LEVERAGE, min(PYRAMID_LEVERAGE, MAX_REQUESTED_LEVERAGE,
                                    BOT_HARD_MAX_LEVERAGE, API_HARD_MAX_LEVERAGE))
        qty = self.exe.rules.qty(self.symbol, desired / price, price)
        actual_notional = qty * price
        margin = actual_notional / D(lev)
        if margin > free:
            logger.warning(f"PYRAMID MARGIN BLOCK | {self.id} | level={level} margin={margin} free={free}")
            return None
        current_symbol = self.account.current_symbol_notional(self.symbol)
        symbol_cap = configured_max_total_symbol_notional(self.symbol)
        if current_symbol + actual_notional > symbol_cap:
            logger.warning(f"PYRAMID SYMBOL CAP | {self.id} | current={current_symbol} add={actual_notional} cap={symbol_cap}")
            return None
        return {"leverage": lev, "qty": qty, "price": price, "notional": actual_notional,
                "margin": margin, "estimated_adverse_loss": D(0), "target_profit": D(0),
                "recovery_level": 0, "desired_notional_override": desired,
                "recovery_multiplier": "1", "meta": {
                    "engine": "PYRAMID_1PCT", "level": level,
                    "sizing_base": "INITIAL_FIXED" if level <= 1 else "REAL_FREE_MARGIN",
                    "free_margin_before": str(free),
                    "add_free_margin_pct": str(PYRAMID_ADD_FREE_MARGIN_PCT),
                }}

    def _entry_allowed(self) -> bool:
        if not self.allow_new_entries:
            return False
        if self.store.killed() != "OFF":
            return False
        gate_ok, gate_reason = self.store.entry_allowed()
        if not gate_ok:
            logger.warning(f"PYRAMID ENTRY GATE | {self.id} | {gate_reason}")
            return False
        if not self.md.is_fresh(self.symbol):
            logger.warning(f"PYRAMID STALE PRICE | {self.id} | age_s={self.md.age(self.symbol):.3f}")
            return False
        if PYRAMID_APPLY_NEWS_FILTER:
            blocked, why = self.news.blocked()
            if blocked:
                logger.info(f"PYRAMID NEWS BLOCK | {self.id} | {why}")
                return False
        return True

    def _physical_position_snapshot(self) -> Optional[Dict[str, Any]]:
        """Retorna a posição física deste symbol/side para cálculo de liquidação."""
        if not LIVE_TRADING:
            return None
        try:
            rows = self.client.positions(self.symbol)
            if not isinstance(rows, list):
                rows = [rows] if rows else []
            for p in rows:
                if (
                    str(p.get("symbol", "")).upper() == self.symbol
                    and str(p.get("positionSide", "")).upper() == self.side
                    and abs(dec(p.get("positionAmt"))) > 0
                ):
                    return p
        except Exception as exc:
            logger.warning(f"PYRAMID NATIVE STOP POSITION SNAPSHOT FAIL | {self.id} | {exc}")
        return None

    def _max_loss_stop_price(self) -> Optional[Decimal]:
        """Preço em que o PnL líquido estimado atinge -PYRAMID_MAX_LOSS_USD.

        Usa a mesma taxa de fee do _net_unrealized() para que a ordem nativa fique
        coerente com o limite lógico do PYRAMID.
        """
        st = self.st()
        legs = st.get("legs", []) or []
        if not legs:
            return None
        q_total = D(0)
        entry_value = D(0)
        for leg in legs:
            q = dec(leg.get("qty"))
            ep = dec(leg.get("entry_price"))
            if q > 0 and ep > 0:
                q_total += q
                entry_value += ep * q
        if q_total <= 0 or entry_value <= 0:
            return None

        share = dec(st.get("capital_share", "1"))
        max_loss = PYRAMID_MAX_LOSS_USD * share
        fee_rate = D(os.getenv("TAKER_FEE_RATE", "0.00035"))

        if self.side == "LONG":
            denom = q_total * (D(1) - fee_rate)
            if denom <= 0:
                return None
            raw = (entry_value * (D(1) + fee_rate) - max_loss) / denom
        else:
            denom = q_total * (D(1) + fee_rate)
            if denom <= 0:
                return None
            raw = (entry_value * (D(1) - fee_rate) + max_loss) / denom

        if raw <= 0:
            return None
        direction = "DOWN" if self.side == "LONG" else "UP"
        return self.exe.rules.trigger_price(self.symbol, raw, direction)

    def _effective_native_stop_price(self, mark: Decimal) -> Tuple[Optional[Decimal], Dict[str, Any]]:
        """Combina max-loss lógico com buffer antes da liquidação física.

        LONG: escolhe o stop MAIS ALTO (fecha antes).
        SHORT: escolhe o stop MAIS BAIXO (fecha antes).
        """
        loss_stop = self._max_loss_stop_price()
        physical = self._physical_position_snapshot()
        liq = dec((physical or {}).get("liquidationPrice"))
        liq_guard = None

        if liq > 0:
            if self.side == "LONG":
                raw = liq * (D(1) + LIQUIDATION_BUFFER_PCT)
                liq_guard = self.exe.rules.trigger_price(self.symbol, raw, "DOWN")
            else:
                raw = liq * (D(1) - LIQUIDATION_BUFFER_PCT)
                liq_guard = self.exe.rules.trigger_price(self.symbol, raw, "UP")

        candidates = [x for x in (loss_stop, liq_guard) if x is not None and x > 0]
        if not candidates:
            return None, {"loss_stop": loss_stop, "liq": liq, "liq_guard": liq_guard}

        target = max(candidates) if self.side == "LONG" else min(candidates)

        # A ordem deve estar no lado adverso do mark. Se já estivermos além do
        # preço seguro, o tick fará fechamento imediato em vez de instalar
        # uma condicional inválida.
        valid = target < mark if self.side == "LONG" else target > mark
        return (target if valid else None), {
            "loss_stop": loss_stop,
            "liq": liq,
            "liq_guard": liq_guard,
            "target": target,
            "mark": mark,
            "valid": valid,
        }

    def _clear_native_risk_stop(self) -> None:
        st = self.st()
        native = st.get("native_risk_stop")
        if native:
            self.exe.cancel_basket_exit(self.symbol, native)
        st["native_risk_stop"] = None

    def _stop_state_after_close(self, total: Decimal, net_before_close: Decimal,
                                reason: str, remaining: List[Dict[str, Any]]) -> None:
        st = self.st()
        st["legs"] = remaining
        st["realized_pnl"] = str(dec(st.get("realized_pnl")) + total)
        st["equity"] = str(
            PYRAMID_BANKROLL_USD * dec(st.get("capital_share", "1"))
            + dec(st.get("realized_pnl"))
        )
        st["last_unrealized"] = "0"
        st["last_net_pnl"] = str(total)
        st["native_risk_stop"] = None
        st["stopped"] = bool(PYRAMID_STOP_AFTER_MAX_LOSS)
        st["stop_reason"] = (
            f"{reason} net_before_close={net_before_close} realized_close={total}"
        )
        st["last_update"] = now_iso()
        self.store.save()

    def _consume_native_risk_stop(self, price: Decimal) -> bool:
        st = self.st()
        native = st.get("native_risk_stop")
        legs = list(st.get("legs", []) or [])
        if not native or not legs:
            return False
        try:
            consumed = self.exe.consume_basket_exit(
                self.id,
                self.symbol,
                legs,
                native,
                price,
                "NATIVE_PYRAMID_RISK_STOP_V29",
            )
        except Exception as exc:
            logger.exception(f"PYRAMID NATIVE STOP CONSUME FAIL | {self.id} | {exc}")
            return False

        if not consumed:
            return False

        total, closes = consumed
        closed_ids = {str(c.get("leg_id")) for c in closes}
        remaining = [leg for leg in legs if str(leg.get("id")) not in closed_ids]
        net_before = dec(st.get("last_net_pnl"))
        self._stop_state_after_close(
            total, net_before, "NATIVE_RISK_STOP_FILLED", remaining
        )
        logger.critical(
            f"PYRAMID NATIVE RISK STOP FILLED | {self.id} | "
            f"realized={total} remaining_legs={len(remaining)} stopped={self.st().get('stopped')}"
        )
        return True

    def _ensure_native_risk_stop(self, price: Decimal, force: bool = False) -> bool:
        """Mantém uma STOP_MARKET nativa para a cesta PYRAMID deste lado."""
        if not PYRAMID_NATIVE_RISK_STOP or not NATIVE_PROTECTIVE_ORDERS:
            return True

        st = self.st()
        legs = list(st.get("legs", []) or [])
        if not legs:
            if st.get("native_risk_stop"):
                self._clear_native_risk_stop()
                self.store.save()
            return True

        now_t = time.time()
        if (
            not force
            and now_t - self._last_native_stop_refresh < max(1.0, PYRAMID_NATIVE_STOP_REFRESH_SECONDS)
        ):
            return True
        self._last_native_stop_refresh = now_t

        target, meta = self._effective_native_stop_price(price)
        existing = st.get("native_risk_stop")

        # Se a proteção atual continua viva e o alvo não mudou materialmente,
        # não cancela/recria a ordem.
        if existing and target is not None:
            old_target = dec(existing.get("target_price"))
            tick = self.exe.rules.rules[self.symbol].tick_size
            health = self.exe.basket_exit_health(self.symbol, existing)
            if health == "UNKNOWN":
                self.store.set_protection_block(self.id, "PYRAMID_NATIVE_STOP_VERIFY_UNKNOWN")
                logger.error(
                    "PYRAMID NATIVE STOP VERIFY UNKNOWN | %s | mantendo ordem persistida e bloqueando novas entradas",
                    self.id,
                )
                return False
            if (
                old_target > 0
                and abs(old_target - target) <= max(tick, tick * D(2))
                and health == "LIVE"
            ):
                self.store.set_protection_block(self.id, None)
                return True

        # Se o preço já ultrapassou a faixa em que uma condicional seria válida,
        # fecha agora para não esperar liquidação.
        if target is None:
            candidate = dec(meta.get("target"))
            liq = dec(meta.get("liq"))
            unsafe = (
                candidate > 0
                and (
                    (self.side == "LONG" and price <= candidate)
                    or (self.side == "SHORT" and price >= candidate)
                )
            )
            if unsafe:
                logger.critical(
                    f"PYRAMID LIQUIDATION GUARD IMMEDIATE | {self.id} | "
                    f"mark={price} candidate_stop={candidate} liq={liq} meta={meta}"
                )
                self._clear_native_risk_stop()
                net = self._net_unrealized(price)
                self._stop_and_close(price, net, close_reason="PYRAMID_LIQUIDATION_GUARD_V29")
                return False
            logger.warning(
                f"PYRAMID NATIVE STOP TARGET UNAVAILABLE | {self.id} | meta={meta}"
            )
            return False

        if existing:
            self.exe.cancel_basket_exit(self.symbol, existing)
            st["native_risk_stop"] = None

        try:
            native = self.exe.install_basket_exit(
                self.id, self.symbol, legs, target, price
            )
        except Exception as exc:
            logger.exception(
                f"PYRAMID NATIVE STOP INSTALL FAIL | {self.id} | target={target} | {exc}"
            )
            self.store.set_protection_block(self.id, "PYRAMID_NATIVE_STOP_INSTALL_FAILED")
            return False

        if not native:
            logger.warning(
                f"PYRAMID NATIVE STOP NOT INSTALLED | {self.id} | target={target}"
            )
            self.store.set_protection_block(self.id, "PYRAMID_NATIVE_STOP_NOT_INSTALLED")
            return False

        native["risk_meta"] = {
            k: (str(v) if isinstance(v, Decimal) else v) for k, v in meta.items()
        }
        st["native_risk_stop"] = native
        st["last_update"] = now_iso()
        self.store.set_protection_block(self.id, None)
        self.store.save()
        logger.warning(
            f"PYRAMID NATIVE RISK STOP | {self.id} | side={self.side} "
            f"target={target} max_loss_stop={meta.get('loss_stop')} "
            f"liq={meta.get('liq')} liq_guard={meta.get('liq_guard')} "
            f"qty={sum((dec(x.get('qty')) for x in legs), D(0))}"
        )
        return True

    def _open_level(self, price: Decimal, level: int, trigger: Decimal) -> bool:
        if not self._entry_allowed():
            return False
        sizing = self._sizing(price, level)
        if not sizing:
            return False
        reason = "PYRAMID_INITIAL_1PCT" if level == 1 else f"PYRAMID_ADD_LEVEL_{level}"
        leg = self.exe.open_leg(self.id, self.symbol, self.side, sizing, reason)
        if not leg:
            return False
        st = self.st()
        st.setdefault("legs", []).append(leg)
        st["levels_filled"] = int(st.get("levels_filled", 0)) + 1
        st["next_level"] = level + 1
        st["last_trigger_price"] = str(trigger)
        st["last_update"] = now_iso()
        self.store.save()
        logger.warning(
            f"PYRAMID OPEN | {self.id} | level={level} trigger={trigger} fill={leg.get('entry_price')} "
            f"qty={leg.get('qty')} notional={leg.get('notional')} leverage={sizing['leverage']}x "
            f"anchor={st.get('anchor')} next_level={st['next_level']}"
        )
        # Recalcula a proteção porque quantidade/entry médio/liquidação mudaram.
        # Se a ordem protetiva não puder ser mantida, preserva o nível que acabou
        # de abrir, mas impede qualquer nova adição neste mesmo tick.
        protected = self._ensure_native_risk_stop(price, force=True)
        if not protected:
            logger.critical(
                f"PYRAMID PROTECTION INSTALL FAIL | {self.id} | level={level} "
                f"motivo=NATIVE_RISK_STOP_NOT_CONFIRMED | fechando cesta para nao manter exposicao desprotegida"
            )
            net = self._net_unrealized(price)
            self._stop_and_close(
                price,
                net,
                close_reason="PYRAMID_PROTECTION_INSTALL_FAILED",
            )
            return False
        return True

    def _stop_and_close(self, price: Decimal, net_before_close: Decimal, close_reason: str = "PYRAMID_MAX_LOSS") -> None:
        st = self.st()
        legs = list(st.get("legs", []) or [])
        self._clear_native_risk_stop()
        if not legs:
            st["stopped"] = True
            st["stop_reason"] = close_reason
            self.store.save()
            return
        total, closes = self.exe.close_legs(self.id, self.symbol, legs, price, close_reason)
        closed_ids = {str(c.get("leg_id")) for c in closes}
        remaining = [leg for leg in legs if str(leg.get("id")) not in closed_ids]
        self._stop_state_after_close(total, net_before_close, close_reason, remaining)
        logger.critical(
            f"PYRAMID STOP | {self.id} | reason={close_reason} "
            f"net_before_close={net_before_close} realized={total} "
            f"remaining_legs={len(remaining)} stopped={self.st().get('stopped')}"
        )

    def diagnostic(self, price: Optional[Decimal]) -> Dict[str, Any]:
        """Retorna o motivo operacional atual para não haver nova entrada."""
        st = self.st()
        if price is None or price <= 0:
            return {"status": "WAITING_PRICE", "reason": "NO_MARK_PRICE"}
        if st.get("stopped"):
            return {"status": "STOPPED", "reason": st.get("stop_reason") or "STOPPED"}
        anchor = dec(st.get("anchor"))
        if anchor <= 0:
            return {"status": "WAITING_ANCHOR", "reason": "ANCHOR_NOT_SET", "mark": price}
        level = max(1, int(st.get("next_level", 1)))
        trigger = self._trigger_price(anchor, level)
        free_margin = self.account.free_margin()
        desired = self._desired_notional(level, free_margin=free_margin)
        if trigger <= 0:
            return {"status": "INVALID_TRIGGER", "reason": "TRIGGER_LE_ZERO", "mark": price, "anchor": anchor, "level": level}
        crossed = self._crossed(price, trigger)
        if crossed:
            # O gatilho foi alcançado. A tentativa de entrada ocorre no tick; se não houver
            # posição, os logs específicos informarão gate/news/margem/cap/API.
            remain_pct = D(0)
            status = "TRIGGER_REACHED"
            reason = "ENTRY_ATTEMPT_EXPECTED"
        else:
            remain_pct = (abs(trigger - price) / price * D(100)) if price > 0 else D(0)
            status = "WAITING_TRIGGER"
            reason = "PRICE_NOT_REACHED"
        return {
            "status": status, "reason": reason, "mark": price, "anchor": anchor,
            "trigger": trigger, "remaining_pct": remain_pct, "level": level,
            "desired_notional": desired, "legs": len(st.get("legs", []) or []),
            "equity": dec(st.get("equity")), "net": dec(st.get("last_net_pnl")),
            "free_margin": free_margin,
            "sizing_base": "INITIAL_FIXED" if level <= 1 else "REAL_FREE_MARGIN",
        }

    def tick(self, price: Decimal) -> None:
        st = self.st()
        if st.get("stopped"):
            return
        if dec(st.get("anchor")) <= 0:
            if not self.allow_new_entries:
                return
            phased_anchor = price * (D(1) + self.grid_phase)
            st["anchor"] = str(phased_anchor)
            st["next_level"] = max(1, int(st.get("next_level", 1)))
            st["last_update"] = now_iso()
            self.store.save()
            logger.warning(f"PYRAMID ANCHOR | {self.id} | phase={self.grid_phase} anchor={st['anchor']} | first_trigger={self._trigger_price(dec(st['anchor']), 1)}")
            return

        legs = st.get("legs", []) or []
        if legs:
            # A ordem STOP_MARKET vive na exchange. No tick, primeiro consumimos
            # eventual fill e depois verificamos/recriamos a proteção se necessário.
            if self._consume_native_risk_stop(price):
                return
            if not self._ensure_native_risk_stop(price):
                # Se não foi possível manter a proteção nativa, não abre novos níveis
                # neste tick; a posição existente continua sob o watchdog lógico.
                return
            net = self._net_unrealized(price)
            st["last_unrealized"] = str(net)
            st["last_net_pnl"] = str(dec(st.get("realized_pnl")) + net)
            st["equity"] = str(PYRAMID_BANKROLL_USD * dec(st.get("capital_share", "1")) + dec(st["last_net_pnl"]))
            if net <= -(PYRAMID_MAX_LOSS_USD * dec(st.get("capital_share", "1"))):
                self._stop_and_close(price, net)
                return

        anchor = dec(st.get("anchor"))
        level = max(1, int(st.get("next_level", 1)))
        processed = 0
        while processed < max(1, PYRAMID_MAX_LEVELS_PER_TICK):
            trigger = self._trigger_price(anchor, level)
            if trigger <= 0 or not self._crossed(price, trigger):
                break
            if not self._open_level(price, level, trigger):
                break
            level += 1
            processed += 1

# -----------------------------------------------------------------------------
# STARTUP RECONCILIATION + KILL SWITCH
# -----------------------------------------------------------------------------

class Reconciler:
    def __init__(self, client: AsterClient, store: StateStore, ledger: FillLedger, rules: RulesBook):
        self.client = client; self.store = store; self.ledger = ledger; self.rules = rules
        self.last_snapshot: Optional[ExchangeSnapshot] = None
        self._state_ledger_mismatch_streak = 0

    def snapshot(self) -> ExchangeSnapshot:
        positions: Dict[Tuple[str, str], Decimal] = {}; entries: Dict[Tuple[str, str], Decimal] = {}
        for p in (self.client.positions() if LIVE_TRADING else []):
            sym = str(p.get("symbol", "")).upper(); side = str(p.get("positionSide", "")).upper()
            if sym not in SYMBOLS or side not in ("LONG", "SHORT"): continue
            q = abs(dec(p.get("positionAmt")))
            if q > 0:
                positions[(sym, side)] = q; entries[(sym, side)] = dec(p.get("entryPrice"))
        orders = self.client.open_orders() if LIVE_TRADING else []
        snap = ExchangeSnapshot(now_ms(), positions, entries, orders if isinstance(orders, list) else [])
        self.last_snapshot = snap
        return snap

    def expected_by_symbol_side(self) -> Dict[Tuple[str, str], Decimal]:
        return self.ledger.open_by_symbol_side()

    def expected_from_state_by_symbol_side(self) -> Dict[Tuple[str, str], Decimal]:
        out: Dict[Tuple[str, str], Decimal] = {}

        def add(symbol: str, side: str, qty: Any) -> None:
            sym = str(symbol).upper()
            ps = str(side).upper()
            q = dec(qty)
            if sym in SYMBOLS and ps in ("LONG", "SHORT") and q > 0:
                out[(sym, ps)] = out.get((sym, ps), D(0)) + q

        with self.store.lock:
            # Active single-PYRAMID states.
            for st in self.store.state.get("pyramid", {}).values():
                if not isinstance(st, dict):
                    continue
                sym = str(st.get("symbol") or "").upper()
                default_side = str(st.get("side") or "").upper()
                for leg in st.get("legs", []) or []:
                    add(sym, leg.get("side") or default_side, leg.get("qty"))

            # Compatibility/drain-only G0 states, only if they still carry live legs.
            for st in self.store.state.get("pyramid_grids", {}).values():
                if not isinstance(st, dict):
                    continue
                sym = str(st.get("symbol") or "").upper()
                default_side = str(st.get("side") or "").upper()
                for leg in st.get("legs", []) or []:
                    add(sym, leg.get("side") or default_side, leg.get("qty"))

        return out

    def state_ledger_mismatches(self, ledger_expected: Dict[Tuple[str, str], Decimal]
                                ) -> List[Tuple[Tuple[str, str], Decimal, Decimal]]:
        state_expected = self.expected_from_state_by_symbol_side()
        out: List[Tuple[Tuple[str, str], Decimal, Decimal]] = []
        for key in set(ledger_expected) | set(state_expected):
            lq = ledger_expected.get(key, D(0))
            sq = state_expected.get(key, D(0))
            step = self.rules.rules[key[0]].step_size if key[0] in self.rules.rules else D("0.00000001")
            if abs(lq - sq) >= step:
                out.append((key, lq, sq))
        return out

    def reconcile(self) -> bool:
        if not LIVE_TRADING:
            self.store.set_trade_gate(True, None); return True
        expected = self.expected_by_symbol_side(); snap = self.snapshot(); actual = snap.positions
        mismatches = []
        for k in set(expected) | set(actual):
            e = expected.get(k, D(0)); a = actual.get(k, D(0))
            step = self.rules.rules[k[0]].step_size if k[0] in self.rules.rules else D("0.00000001")
            if abs(e - a) >= step:
                mismatches.append((k, e, a))
        if mismatches:
            # AUTO-REPAIR conservador: corrige somente RANGE ghost lots quando a quantidade
            # física é explicada EXATAMENTE pelas demais estratégias (ex.: PYRAMID).
            # Assim não atribuímos posição externa/desconhecida ao robô e nunca tocamos
            # na posição física. Depois do reparo, recalculamos tudo antes de liberar o gate.
            repaired = []
            for k, e, a in list(mismatches):
                sym, side = k
                step = self.rules.rules[sym].step_size if sym in self.rules.rules else D("0.00000001")
                range_qty = self.ledger.open_strategy_qty(f"RANGE:{sym}", sym, side)
                other_qty = self.ledger.open_non_range_qty(sym, side)
                if range_qty > 0 and abs(other_qty - a) < step:
                    removed = self.ledger.zero_open_strategy_side(
                        f"RANGE:{sym}", sym, side,
                        reason=f"physical={a} fully_explained_by_non_range={other_qty}",
                    )
                    if removed > 0:
                        repaired.append((k, removed))
            if repaired:
                expected = self.expected_by_symbol_side()
                mismatches = []
                for k in set(expected) | set(actual):
                    e = expected.get(k, D(0)); a = actual.get(k, D(0))
                    step = self.rules.rules[k[0]].step_size if k[0] in self.rules.rules else D("0.00000001")
                    if abs(e - a) >= step:
                        mismatches.append((k, e, a))
                logger.warning(f"RECONCILE | AUTO_REPAIR_RANGE_GHOST | repaired={repaired} remaining={mismatches}")
                if not mismatches:
                    self.store.set_trade_gate(True, None)
                    cleared = self.store.clear_soft_position_mismatch()
                    logger.info(f"RECONCILE | OK APOS AUTO-REPAIR | ledger={expected} physical={actual} | soft_mismatch_cleared={cleared}")
                    return True

        if mismatches:
            reason = f"POSITION_MISMATCH_LEDGER expected_vs_actual={mismatches}"
            self.store.set_trade_gate(False, reason)
            current_ks = self.store.state.get("kill_switch", {}) or {}
            desired = "HARD" if HARD_KILL_ON_POSITION_MISMATCH else "SOFT"
            if str(current_ks.get("mode")) != desired or str(current_ks.get("reason")) != reason:
                self.store.kill(desired, reason)
            logger.error(f"RECONCILE | BLOQUEADO | {reason}")
            return False
        state_mismatches = self.state_ledger_mismatches(expected)
        if state_mismatches:
            self._state_ledger_mismatch_streak += 1
            logger.error(
                "RECONCILE | STATE_LEDGER_MISMATCH | streak=%s/%s | ledger_vs_state=%s | physical=%s",
                self._state_ledger_mismatch_streak,
                max(1, STATE_LEDGER_MISMATCH_CONFIRMATIONS),
                state_mismatches,
                actual,
            )
            if self._state_ledger_mismatch_streak >= max(1, STATE_LEDGER_MISMATCH_CONFIRMATIONS):
                self.store.set_trade_gate(False, f"STATE_LEDGER_MISMATCH:{state_mismatches}")
            return False

        self._state_ledger_mismatch_streak = 0
        self.store.set_trade_gate(True, None)
        cleared = self.store.clear_soft_position_mismatch()
        logger.info(
            "RECONCILE | OK | ledger=%s state=%s physical=%s | soft_mismatch_cleared=%s",
            expected, self.expected_from_state_by_symbol_side(), actual, cleared,
        )
        return True

# -----------------------------------------------------------------------------
# BUILT-IN REGRESSION CHECKS
# -----------------------------------------------------------------------------

def run_internal_regression_checks() -> None:
    assert STATE_LEDGER_MISMATCH_CONFIRMATIONS >= 1
    assert UNKNOWN_ORDER_QUERY_ATTEMPTS >= 1
    assert PYRAMID_MAX_EFFECTIVE_LEVERAGE >= MIN_LEVERAGE
    assert PYRAMID_MAX_EFFECTIVE_LEVERAGE <= PYRAMID_LEVERAGE
    assert RANGE_SIGNAL_MODE == "VOLATILITY_ONLY"
    assert RANGE_ENGINE_ENABLED is False
    assert PYRAMID_GRID_PHASES == (D("0"),)
    assert PYRAMID_GRID_COUNT == 1
    assert PYRAMID_GRID_CAPITAL_SHARE == D(1)
    assert RANGE_TRIGGER_PCT > 0 and RANGE_TAKE_PROFIT_PCT > 0 and RANGE_HARD_STOP_PCT > 0
    assert RECOVERY_MULTIPLIER >= D(1) and MAX_RECOVERY_FAILURES >= 0
    assert configured_max_recovery_notional("BTCUSDT") >= configured_initial_notional("BTCUSDT")
    assert configured_max_recovery_notional("ETHUSDT") >= configured_initial_notional("ETHUSDT")
    fake = object.__new__(RulesBook)
    fake.rules = {"X": SymbolRules("X", D("0.1"), D("0.001"), D("0.001"), D("100"), D("5"))}
    assert fake.trigger_price("X", D("100.01"), "UP") == D("100.1")
    assert fake.trigger_price("X", D("100.09"), "DOWN") == D("100.0")
    assert RECOVERY_MULTIPLIER ** 2 == RECOVERY_MULTIPLIER * RECOVERY_MULTIPLIER
    assert PYRAMID_BANKROLL_USD > 0 and PYRAMID_INITIAL_NOTIONAL_USD > 0
    assert PYRAMID_BTC_MIN_ADD_NOTIONAL_USD > 0
    assert PYRAMID_STEP_PCT > 0 and D(0) < PYRAMID_ADD_FREE_MARGIN_PCT <= D(1)
    assert PYRAMID_LEVERAGE >= 1 and PYRAMID_MAX_LOSS_USD > 0
    assert PYRAMID_NATIVE_STOP_REFRESH_SECONDS >= 1

    # Regression: realized PnL must clear RD exactly when it covers prior loss.
    _t = {"equity":"10", "realized_pnl":"0", "recovery_deficit":"1", "wins":0, "losses":0}
    _apply_realized_pnl_to_state(_t, D("1.25"), D("100"), "TEST_RECOVERY_WIN")
    assert dec(_t["recovery_deficit"]) == D(0)
    _t2 = {"equity":"10", "realized_pnl":"0", "recovery_deficit":"1", "wins":0, "losses":0}
    _apply_realized_pnl_to_state(_t2, D("0.75"), D("100"), "TEST_RECOVERY_PARTIAL")
    assert dec(_t2["recovery_deficit"]) == D("0.25")
    assert D("100") * D("0.05") * D("10") == D("50")
    logger.info("SELF TEST | PASS | single-pyramid/real-free-margin/native-risk-stop/range-retire/recovery/risk/tick invariants")

# -----------------------------------------------------------------------------
# BOT
# -----------------------------------------------------------------------------

class Bot:
    def __init__(self):
        self.stop = threading.Event()
        self.client = AsterClient(USER_ADDRESS, SIGNER_ADDRESS, SIGNER_PRIVATE_KEY)
        self.store = StateStore()
        self.rules = RulesBook(self.client)
        self.md = MarketData(self.client)
        self.news = NewsFilter()
        self.account = AccountManager(self.client, self.rules, self.store)
        self.ledger = FillLedger(LEDGER_FILE)
        self.exe = ExecutionEngine(self.client, self.account, self.rules, self.store, self.ledger)
        self._last_periodic_reconcile_ms = 0
        self.reconciler = Reconciler(self.client, self.store, self.ledger, self.rules)
        self.range_engines: List[RangeEngine] = []
        self.pyramid_engines: List[PyramidEngine] = []
        self.last_hb = 0.0

    def _retire_empty_pyramid_grid(self, st: Dict[str, Any], reason: str) -> None:
        st["anchor"] = None
        st["next_level"] = max(1, int(st.get("next_level", 1)))
        st["legs"] = []
        st["stopped"] = True
        st["stop_reason"] = reason
        st["last_update"] = now_iso()

    def _migrate_single_g0_to_legacy(self) -> None:
        """Migra G0 para o PYRAMID original quando isso é inequívoco."""
        maintenance = self.store.state.setdefault("maintenance", {"completed_emergency_actions": []})
        report = maintenance.setdefault("pyramid_v28_migration", {})
        grids = self.store.state.setdefault("pyramid_grids", {})

        for sym in SYMBOLS:
            for side in ("LONG", "SHORT"):
                legacy_key = f"{sym}:{side}"
                legacy = self.store.state["pyramid"].setdefault(legacy_key, empty_pyramid_state(sym, side))
                gkey = f"{sym}:{side}:G0"
                g0 = grids.get(gkey)
                if not isinstance(g0, dict):
                    report[legacy_key] = {"status": "NO_G0", "at": now_iso()}
                    continue

                legacy_legs = list(legacy.get("legs", []) or [])
                g0_legs = list(g0.get("legs", []) or [])
                old_id = str(g0.get("strategy") or f"PYRAMID:{sym}:{side}:G0")
                new_id = f"PYRAMID:{sym}:{side}"

                if g0_legs and not legacy_legs:
                    promoted = dict(g0)
                    promoted["strategy"] = new_id
                    promoted["grid_id"] = "LEGACY"
                    promoted["grid_phase"] = "0"
                    promoted["capital_share"] = "1"
                    promoted["bankroll"] = str(PYRAMID_BANKROLL_USD)
                    promoted["equity"] = str(PYRAMID_BANKROLL_USD + dec(promoted.get("last_net_pnl")))
                    promoted["last_update"] = now_iso()
                    self.store.state["pyramid"][legacy_key] = promoted
                    self.ledger.rename_strategy(old_id, new_id, sym, side)
                    self._retire_empty_pyramid_grid(g0, "MIGRATED_TO_LEGACY_V28")
                    g0["strategy"] = old_id
                    report[legacy_key] = {"status": "G0_PROMOTED", "at": now_iso()}
                    logger.warning(
                        f"PYRAMID MIGRATION | {sym} {side} | G0 -> LEGACY | "
                        f"legs={len(g0_legs)} anchor={promoted.get('anchor')}"
                    )
                elif g0_legs and legacy_legs:
                    report[legacy_key] = {"status": "DUAL_LIVE_G0_DRAIN_ONLY", "at": now_iso()}
                    logger.warning(
                        f"PYRAMID MIGRATION | {sym} {side} | LEGACY+G0 ambos possuem legs | "
                        f"LEGACY ativo, G0 DRAIN-ONLY; nenhuma nova entrada G0"
                    )
                else:
                    self._retire_empty_pyramid_grid(g0, "RETIRED_EMPTY_G0_V28")
                    report[legacy_key] = {"status": "G0_RETIRED_EMPTY", "at": now_iso()}

        self.store.save()

    def startup(self) -> None:
        logger.info("=" * 90)
        logger.info(f"{BOT_NAME} | version={VERSION} | LIVE_TRADING={LIVE_TRADING}")
        if PYRAMID_GRID_PHASES != (D("0"),):
            raise RuntimeError(
                f"O Perpetual Direcional usa uma única escada PYRAMID por símbolo/lado; PYRAMID_GRID_PHASES deve ser somente 0, atual={PYRAMID_GRID_PHASES}"
            )
        if len(set(PYRAMID_GRID_PHASES)) != len(PYRAMID_GRID_PHASES) or any(x < 0 or x >= PYRAMID_STEP_PCT for x in PYRAMID_GRID_PHASES):
            raise RuntimeError(f"PYRAMID_GRID_PHASES invalidas: {PYRAMID_GRID_PHASES}; use fases unicas entre 0 e PYRAMID_STEP_PCT")
        logger.info(f"SYMBOLS={SYMBOLS} | RANGE=False (legacy positions auto-close on startup) | PYRAMID_1PCT={PYRAMID_ENGINE_ENABLED} architecture=SINGLE_LONG_SHORT_PER_SYMBOL | INDICADORES=NONE")
        logger.info(f"MARGIN=ISOLATED | MODE=HEDGE | MAX_REQUESTED_LEV={MAX_REQUESTED_LEVERAGE} | BOT_HARD_CAP={BOT_HARD_MAX_LEVERAGE} | API_HARD_CAP={API_HARD_MAX_LEVERAGE}")
        logger.info(f"BASE ETH/HYPE: bankroll={INITIAL_BANKROLL_USD} notional={INITIAL_OPERATION_NOTIONAL_USD} | BASE BTC: bankroll={BTC_INITIAL_BANKROLL_USD} notional={BTC_INITIAL_OPERATION_NOTIONAL_USD} | RANGE_RECOVERY={RECOVERY_MULTIPLIER}x | MAX_FAIL={MAX_RECOVERY_FAILURES}")
        logger.info(f"EXITS | RANGE_TP={RANGE_TAKE_PROFIT_PCT} RANGE_STOP={RANGE_HARD_STOP_PCT} | PYRAMID_MAX_LOSS={PYRAMID_MAX_LOSS_USD}")
        logger.info(f"NEWS 3-STAR={NEWS_FILTER_ENABLED} | janela=-{NEWS_WINDOW_BEFORE_MIN}m/+{NEWS_WINDOW_AFTER_MIN}m | fail_closed={NEWS_FAIL_CLOSED}")
        logger.info(f"SAME_SYMBOL_MULTI_STRATEGY={ALLOW_MULTI_STRATEGY_SAME_SYMBOL} | NATIVE_PROTECTIVE_ORDERS={NATIVE_PROTECTIVE_ORDERS} workingType={PROTECTIVE_WORKING_TYPE}")
        logger.info(f"HARDENING | ledger={LEDGER_FILE} | news_stale_max={NEWS_MAX_STALE_SECONDS}s | entry_price_max_age={MAX_PRICE_AGE_FOR_ENTRY_SECONDS}s | reconcile={RECONCILE_INTERVAL_SECONDS}s")
        logger.info(f"RISK CAPS | ETH/HYPE recovery={MAX_RECOVERY_NOTIONAL_USD} total_symbol={MAX_TOTAL_SYMBOL_NOTIONAL_USD} | BTC recovery={BTC_MAX_RECOVERY_NOTIONAL_USD} total_symbol={BTC_MAX_TOTAL_SYMBOL_NOTIONAL_USD}")
        logger.info(f"PYRAMID | bankroll_logico={PYRAMID_BANKROLL_USD} initial_notional={PYRAMID_INITIAL_NOTIONAL_USD} step={PYRAMID_STEP_PCT} add_base=REAL_FREE_MARGIN add_pct={PYRAMID_ADD_FREE_MARGIN_PCT} leverage={PYRAMID_LEVERAGE}x max_loss={PYRAMID_MAX_LOSS_USD} native_risk_stop={PYRAMID_NATIVE_RISK_STOP} liq_buffer={LIQUIDATION_BUFFER_PCT} | BTC_add_floor={PYRAMID_BTC_MIN_ADD_NOTIONAL_USD} | 2 bots/symbol LONG+SHORT")
        logger.info("=" * 90)
        if (LIVE_TRADING or VALIDATE_API_ONLY) and (not USER_ADDRESS or not SIGNER_ADDRESS or not SIGNER_PRIVATE_KEY):
            raise RuntimeError("LIVE_TRADING=1 ou VALIDATE_API_ONLY=1 requer as tres credenciais da API Wallet V3")
        if SELF_TEST_ON_STARTUP:
            run_internal_regression_checks()
        self.client.sync_time()
        self.rules.refresh()
        if VALIDATE_API_ONLY:
            mode = self.client.position_mode()
            multi_assets = self.client.multi_assets_mode()
            balances = self.client.balance()
            account = self.client.account()
            positions = self.client.positions()
            logger.info(f"API V3 VALIDADA | signer={SIGNER_ADDRESS} | hedge={mode} | multi_assets={multi_assets} | balances={len(balances) if isinstance(balances, list) else 0} | positions={len(positions) if isinstance(positions, list) else 0} | canTrade={account.get('canTrade') if isinstance(account, dict) else None}")
            return
        if LIVE_TRADING:
            self.account.ensure_modes()
            self.account.sync(force=True)
            self._migrate_single_g0_to_legacy()
            if LEDGER_RECONCILE_ON_STARTUP:
                self.ledger.bootstrap_from_state(self.store)
            if EMERGENCY_CLOSE_ALL_AND_RESET:
                self.emergency_close_all_and_reset()
            self.reconciler.reconcile()
        else:
            logger.warning("MODO SIMULACAO: nenhuma ordem real sera enviada")

        # RANGE is permanently retired from this Directional robot.
        # On startup, close ONLY any legacy RANGE lots, preserving PYRAMID lots even when
        # Aster aggregates them on the same symbol/positionSide. Then clear RANGE anchors/state.
        self.range_engines = []
        if RETIRE_LEGACY_RANGE_ON_STARTUP:
            for s in SYMBOLS:
                legacy = self.store.state.get("range", {}).get(s, {})
                eng = RangeEngine(s, self.client, self.md, self.news, self.account, self.exe, self.store,
                                  allow_new_entries=False)
                if LIVE_TRADING and legacy.get("basket"):
                    ref_price = self.client.price(s)
                    eng.retire_legacy_range(ref_price)
                else:
                    # No open RANGE basket: remove stale anchor/protect state without sending an order.
                    legacy["basket"] = None
                    legacy["status"] = "RETIRED"
                    legacy["anchor"] = None
                    legacy["protect_anchor"] = None
                    legacy["failures"] = 0
                    legacy["recovery_deficit"] = "0"
                    legacy["last_result"] = "RETIRED_V28"
                    legacy["last_update"] = now_iso()
                    self.store.save()
                    release_owner(self.store, s, eng.id)
                    logger.warning(f"RANGE RETIRED | {s} | sem basket aberto; anchor legado removido")
            if LIVE_TRADING:
                self.account.sync(force=True)
                self.reconciler.reconcile()
        else:
            logger.warning("RANGE RETIRE DESATIVADO POR ENV | RETIRE_LEGACY_RANGE_ON_STARTUP=0")
        if PYRAMID_ENGINE_ENABLED:
            self.pyramid_engines = []
            if not LIVE_TRADING:
                self._migrate_single_g0_to_legacy()

            # Uma única escada ativa por símbolo/lado: PYRAMID original.
            for s in SYMBOLS:
                for side in ("LONG", "SHORT"):
                    legacy_key = f"{s}:{side}"
                    self.pyramid_engines.append(
                        PyramidEngine(
                            s, side, self.client, self.md, self.news, self.account, self.exe, self.store,
                            state_bucket="pyramid", state_key=legacy_key,
                            grid_id="LEGACY", grid_phase=D(0), allow_new_entries=True,
                        )
                    )

                    # Compatibilidade: se um estado antigo deixou G0 com legs simultâneas ao LEGACY,
                    # G0 é gerenciado sem receber novas adições.
                    gkey = f"{s}:{side}:G0"
                    g0 = self.store.state.get("pyramid_grids", {}).get(gkey, {})
                    if isinstance(g0, dict) and g0.get("legs"):
                        self.pyramid_engines.append(
                            PyramidEngine(
                                s, side, self.client, self.md, self.news, self.account, self.exe, self.store,
                                state_bucket="pyramid_grids", state_key=gkey,
                                grid_id="G0", grid_phase=D(0), allow_new_entries=False,
                            )
                        )
        self.md.start(); self.news.start()

    def emergency_close_all_and_reset(self) -> None:
        maintenance = self.store.state.setdefault("maintenance", {"completed_emergency_actions": []})
        completed = maintenance.setdefault("completed_emergency_actions", [])
        completed_ids = {
            str(x.get("id")) if isinstance(x, dict) else str(x)
            for x in completed
        }
        if EMERGENCY_RESET_ID in completed_ids:
            logger.warning(f"EMERGENCY RESET | id={EMERGENCY_RESET_ID} ja concluido; nenhuma ordem repetida")
            return

        logger.critical(
            f"EMERGENCY RESET INICIO | id={EMERGENCY_RESET_ID} | cancelando TODAS as ordens e fechando TODAS as posicoes da conta"
        )
        open_orders = self.client.open_orders()
        positions = self.client.positions()
        symbols = {
            str(x.get("symbol", "")).upper()
            for x in (open_orders if isinstance(open_orders, list) else [])
            if x.get("symbol")
        }
        symbols.update(
            str(x.get("symbol", "")).upper()
            for x in (positions if isinstance(positions, list) else [])
            if x.get("symbol") and abs(dec(x.get("positionAmt"))) > 0
        )
        for symbol in sorted(symbols):
            self.client.cancel_all(symbol)
            logger.warning(f"EMERGENCY RESET | ordens canceladas | {symbol}")

        for p in (positions if isinstance(positions, list) else []):
            qty = abs(dec(p.get("positionAmt")))
            if qty <= 0:
                continue
            symbol = str(p.get("symbol", "")).upper()
            position_side = str(p.get("positionSide", "")).upper()
            if position_side not in ("LONG", "SHORT"):
                raise RuntimeError(f"EMERGENCY RESET encontrou positionSide invalido: {p}")
            mark = dec(p.get("markPrice") or p.get("entryPrice") or 0)
            logger.critical(
                f"EMERGENCY CLOSE | symbol={symbol} strategy_owner={self.store.state.get('symbol_owner', {}).get(symbol)} side={position_side} qty={qty} entry={p.get('entryPrice')} mark={p.get('markPrice')} notional={abs(dec(p.get('notional') or qty * mark))} unreal={p.get('unRealizedProfit') or p.get('unrealizedProfit')}"
            )
            self.exe.market("EMERGENCY_RESET", symbol, position_side, qty, False, mark)

        remaining = []
        for _ in range(5):
            time.sleep(1)
            remaining = [
                p for p in self.client.positions()
                if abs(dec(p.get("positionAmt"))) > 0
            ]
            if not remaining:
                break
        if remaining:
            raise RuntimeError(
                "EMERGENCY RESET NAO CONFIRMADO; posicoes restantes="
                + str([(p.get("symbol"), p.get("positionSide"), p.get("positionAmt")) for p in remaining])
            )

        reset = fresh_state()
        reset["maintenance"]["completed_emergency_actions"] = [{
            "id": EMERGENCY_RESET_ID,
            "completed_at": now_iso(),
            "action": "CLOSE_ALL_POSITIONS_CANCEL_ALL_ORDERS_AND_RESET_STATE",
        }]
        with self.store.lock:
            self.store.state = reset
            self.store.save()
        self.ledger.reset()
        self.account.sync(force=True)
        logger.critical(
            f"EMERGENCY RESET CONCLUIDO | id={EMERGENCY_RESET_ID} | posicoes=0 | ordens=0 | estado zerado | novas entradas usam notional base configurado"
        )

    def hard_kill(self) -> None:
        if not LIVE_TRADING:
            return
        logger.error("HARD KILL EXECUTION | cancelando ordens e fechando posicoes conhecidas")
        for s in SYMBOLS:
            try: self.client.cancel_all(s)
            except Exception as e: logger.error(f"HARD KILL cancel {s} | {e}")
        for e in self.range_engines:
            try:
                st = e.st(); b = st.get("basket")
                p = self.md.get(e.symbol)
                if b and p: e._close_basket(p, "HARD_KILL", protect_after=True)
            except Exception as ex: logger.exception(f"HARD KILL range {e.symbol} | {ex}")
        for e in self.pyramid_engines:
            try:
                st = e.st(); p = self.md.get(e.symbol)
                if st.get("legs") and p: e._stop_and_close(p, e._net_unrealized(p))
            except Exception as ex: logger.exception(f"HARD KILL pyramid {e.id} | {ex}")

    def heartbeat(self) -> None:
        if time.time() - self.last_hb < HEARTBEAT_SECONDS:
            return
        self.last_hb = time.time()
        api_ok = True
        try:
            if LIVE_TRADING: self.account.sync(force=True)
        except Exception as e:
            api_ok = False
            logger.warning(f"HEARTBEAT account sync | {e}")
        parts = []
        with self.store.lock:
            for s in SYMBOLS:
                r = self.store.state["range"][s]
                parts.append(f"R:{s}:eq={r['equity']},RD={r['recovery_deficit']},status={r['status']},fail={r['failures']}")
            for e in self.pyramid_engines:
                p = e.st()
                parts.append(f"P:{p['symbol']}:{p['side']}:{p.get('grid_id','LEGACY')}:eq={p['equity']},lvl={p['next_level']},legs={len(p.get('legs',[]) or [])},net={p.get('last_net_pnl','0')},stop={int(bool(p.get('stopped')))},phase={p.get('grid_phase','0')}")
            ks = self.store.state["kill_switch"]
            gate = self.store.state.get("trade_gate", {})
        logger.info(f"HEARTBEAT | wallet={self.account.wallet_balance} avail={self.account.available_balance} unreal={self.account.unrealized} | kill={ks.get('mode')}:{ks.get('reason')} | entry_gate={gate.get('open_allowed')}:{gate.get('reason')} | ledger={self.ledger.open_by_symbol_side()} | {' | '.join(parts)}")
        # Monitor explícito do anchor RANGE e dos gatilhos efetivos +/-1%, já arredondados pelas regras da exchange.
        for e in self.range_engines:
            try:
                rst = e.st()
                anchor = dec(rst.get("anchor"))
                mark = self.md.get(e.symbol)
                if anchor <= 0:
                    logger.info(f"RANGE PRICE MONITOR | {e.symbol} | status={rst.get('status')} mark={mark} anchor_fixado=AGUARDANDO_PRIMEIRO_PRECO")
                    continue
                long_entry = e.exe.rules.trigger_price(e.symbol, anchor * (D(1) + RANGE_TRIGGER_PCT), "UP")
                short_entry = e.exe.rules.trigger_price(e.symbol, anchor * (D(1) - RANGE_TRIGGER_PCT), "DOWN")
                falta_long = max(D(0), (long_entry - mark) / mark * D(100)) if mark and mark > 0 else D(0)
                falta_short = max(D(0), (mark - short_entry) / mark * D(100)) if mark and mark > 0 else D(0)
                logger.info(
                    f"RANGE PRICE MONITOR | {e.symbol} | status={rst.get('status')} mark={mark} anchor_fixado={anchor} "
                    f"LONG_entrada={long_entry} SHORT_entrada={short_entry} "
                    f"faltam_LONG={dstr(falta_long, 6)}% faltam_SHORT={dstr(falta_short, 6)}%"
                )
            except Exception as ex:
                logger.warning(f"RANGE PRICE MONITOR FAIL | {e.symbol} | {ex}")
        # Diagnóstico explícito de cada PYRAMID: mostra exatamente por que ainda não abriu.
        for e in self.pyramid_engines:
            try:
                mark = self.md.get(e.symbol)
                d = e.diagnostic(mark)
                if d.get("status") == "WAITING_TRIGGER":
                    logger.info(
                        f"PYRAMID WAIT | {e.id} | status={d['status']} reason={d['reason']} "
                        f"mark={d['mark']} anchor={d['anchor']} next_level={d['level']} "
                        f"trigger={d['trigger']} faltam_pct={d['remaining_pct']:.6f}% "
                        f"next_notional_usd={d['desired_notional']} legs={d['legs']} eq={d['equity']} net={d['net']}"
                    )
                elif d.get("status") == "TRIGGER_REACHED":
                    logger.warning(
                        f"PYRAMID TRIGGER | {e.id} | status={d['status']} reason={d['reason']} "
                        f"mark={d['mark']} trigger={d['trigger']} next_level={d['level']} "
                        f"next_notional_usd={d['desired_notional']} legs={d['legs']}"
                    )
                else:
                    logger.info(f"PYRAMID STATUS | {e.id} | {d}")
            except Exception as ex:
                logger.warning(f"PYRAMID DIAGNOSTIC FAIL | {e.id} | {ex}")
        with self.news._lock:
            news_events = len(self.news.events)
            news_source = self.news.last_source
            news_age = int(max(0, time.time() - self.news.last_success)) if self.news.last_success else -1
        news_health = (
            "DISABLED" if not NEWS_FILTER_ENABLED else
            "OK" if self.news.last_success and news_age <= NEWS_MAX_STALE_SECONDS else
            "STALE"
        )
        logger.info(
            f"HEALTH SNAPSHOT | version={VERSION} live={LIVE_TRADING} api_v3={'OK' if api_ok else 'DEGRADED'} signer={SIGNER_ADDRESS} | "
            f"mode=HEDGE margin=ISOLATED multi_strategy_same_symbol={ALLOW_MULTI_STRATEGY_SAME_SYMBOL} native_protection={NATIVE_PROTECTIVE_ORDERS} | "
            f"news={news_health} source={news_source} events={news_events} age_s={news_age} fail_closed={NEWS_FAIL_CLOSED} window=-{NEWS_WINDOW_BEFORE_MIN}m/+{NEWS_WINDOW_AFTER_MIN}m | "
            f"range=False legacy_range_engines={len(self.range_engines)} | "
            f"pyramid={PYRAMID_ENGINE_ENABLED} architecture=SINGLE bankroll_logico={PYRAMID_BANKROLL_USD} initial={PYRAMID_INITIAL_NOTIONAL_USD} step={PYRAMID_STEP_PCT} add_base=REAL_FREE_MARGIN add_pct={PYRAMID_ADD_FREE_MARGIN_PCT} btc_add_floor={PYRAMID_BTC_MIN_ADD_NOTIONAL_USD} lev={PYRAMID_LEVERAGE}x max_loss={PYRAMID_MAX_LOSS_USD} native_risk_stop={PYRAMID_NATIVE_RISK_STOP}"
        )
        if LIVE_TRADING:
            try:
                self.log_open_positions_detailed()
            except Exception as e:
                logger.warning(f"OPEN POSITION DETAIL FAIL | {e}")

    def log_open_positions_detailed(self) -> None:
        positions = self.client.positions()
        found = 0
        with self.store.lock:
            state_range = self.store.state.get("range", {})
            state_pyramid = {}
            state_pyramid.update(self.store.state.get("pyramid", {}))
            state_pyramid.update(self.store.state.get("pyramid_grids", {}))
            legacy_owners = dict(self.store.state.get("symbol_owner", {}))

        logical: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}

        def add_logical(symbol: str, side: str, strategy: str, vqty: Decimal,
                        target: Any = "-", stop: Any = "-", recovery: Any = "-",
                        virtual_entry: Any = "-") -> None:
            symbol = str(symbol).upper()
            side = str(side).upper()
            if symbol not in SYMBOLS or side not in ("LONG", "SHORT") or vqty <= 0:
                return
            logical.setdefault((symbol, side), []).append({
                "strategy": strategy, "qty": vqty, "target": target,
                "stop": stop, "recovery": recovery, "entry": virtual_entry,
            })

        for symbol, rst in state_range.items():
            basket = (rst or {}).get("basket") or {}
            legs = basket.get("legs") or []
            grouped: Dict[str, Decimal] = {}
            weighted_entry: Dict[str, Decimal] = {}
            for leg in legs:
                side = str(leg.get("side", "")).upper()
                q = dec(leg.get("qty"))
                ep = dec(leg.get("entry_price"))
                if side in ("LONG", "SHORT") and q > 0:
                    grouped[side] = grouped.get(side, D(0)) + q
                    weighted_entry[side] = weighted_entry.get(side, D(0)) + q * ep
            for side, q in grouped.items():
                ventry = weighted_entry.get(side, D(0)) / q if q > 0 else D(0)
                add_logical(
                    symbol, side, str((rst or {}).get("strategy") or f"RANGE:{symbol}"), q,
                    basket.get("recovery_tp_price") or basket.get("tp_price") or "-",
                    basket.get("hard_stop_price") or "-",
                    basket.get("alternations", 0),
                    ventry,
                )


        for pst in state_pyramid.values():
            pst = pst or {}
            symbol = str(pst.get("symbol", "")).upper(); side = str(pst.get("side", "")).upper()
            legs = pst.get("legs", []) or []
            q = sum((dec(x.get("qty")) for x in legs), D(0))
            weighted = sum((dec(x.get("qty")) * dec(x.get("entry_price")) for x in legs), D(0))
            ventry = weighted / q if q > 0 else D(0)
            if q > 0:
                add_logical(symbol, side, str(pst.get("strategy")), q, "-", f"MAX_LOSS_USD={PYRAMID_MAX_LOSS_USD}",
                            int(pst.get("next_level", 1)) - 1, ventry)

        for p in (positions if isinstance(positions, list) else []):
            qty = abs(dec(p.get("positionAmt")))
            if qty <= 0:
                continue
            found += 1
            symbol = str(p.get("symbol", "")).upper()
            side = str(p.get("positionSide", "")).upper()
            candidates = logical.get((symbol, side), [])
            virtual_qty = sum((dec(x.get("qty")) for x in candidates), D(0))

            try:
                step = dec((self.rules.rules.get(symbol) or {}).get("stepSize"))
            except Exception:
                step = D(0)
            tol = max(D("0.00000001"), step)
            residual = qty - virtual_qty

            if candidates:
                names = [str(x["strategy"]) for x in candidates]
                unique_names = list(dict.fromkeys(names))
                if len(unique_names) == 1:
                    owner = unique_names[0]
                else:
                    owner = "AGREGADA[" + ",".join(unique_names) + "]"
                if abs(residual) > tol:
                    owner += f"+RESIDUO_EXTERNO({dstr(residual, 8)})"
            else:
                legacy = legacy_owners.get(symbol) if not ALLOW_MULTI_STRATEGY_SAME_SYMBOL else None
                owner = legacy or "DESCONHECIDO/EXTERNO"

            entry = dec(p.get("entryPrice"))
            mark = dec(p.get("markPrice"))
            notional = abs(dec(p.get("notional") or qty * mark))
            unreal = dec(p.get("unRealizedProfit") or p.get("unrealizedProfit"))
            margin = dec(p.get("isolatedWallet") or p.get("isolatedMargin"))
            leverage = p.get("leverage") or "?"
            liq = p.get("liquidationPrice") or "?"
            move = pct_change(entry, mark) if entry > 0 and mark > 0 else D(0)
            favorable = move if side == "LONG" else -move
            target = stop = recovery_level = "-"

            unique_strategies = list(dict.fromkeys(str(x["strategy"]) for x in candidates))
            if len(unique_strategies) == 1 and candidates:
                target = candidates[0].get("target") or "-"
                stop = candidates[0].get("stop") or "-"
                recovery_level = candidates[0].get("recovery", "-")

            virtual_lot_parts = []
            for x in candidates:
                x_qty = dec(x.get("qty"))
                x_recovery_raw = x.get("recovery", 0)
                try:
                    x_recovery = max(0, int(x_recovery_raw))
                except Exception:
                    x_recovery = 0
                is_pyramid = str(x.get("strategy", "")).startswith("PYRAMID:")
                x_multiplier = D(1) if is_pyramid else RECOVERY_MULTIPLIER ** x_recovery
                x_base_notional = PYRAMID_INITIAL_NOTIONAL_USD if is_pyramid else configured_initial_notional(symbol)
                x_notional = x_qty * mark if mark > 0 else D(0)
                x_mode = "PYRAMID" if is_pyramid else ("NORMAL" if x_recovery == 0 else "RECOVERY")
                if is_pyramid:
                    x_recovery = 0
                virtual_lot_parts.append(
                    f"{x['strategy']}:{side}"
                    f"|qty={dstr(x_qty, 8)}"
                    f"|entry={x.get('entry','-')}"
                    f"|notional_usd={dstr(x_notional, 8)}"
                    f"|mode={x_mode}"
                    f"|recovery_level={x_recovery}"
                    f"|multiplier={dstr(x_multiplier, 4)}x"
                    f"|base_notional_usd={dstr(x_base_notional, 8)}"
                    f"|tp={x.get('target','-')}"
                    f"|sl={x.get('stop','-')}"
                )
            virtual_lots = ";".join(virtual_lot_parts) or "-"

            def remaining_pct(raw: Any) -> str:
                try:
                    level = dec(raw)
                    if level <= 0 or mark <= 0:
                        return "-"
                    return dstr(abs(level - mark) / mark * D(100), 6)
                except Exception:
                    return "-"

            tp_distance = remaining_pct(target)
            stop_distance = remaining_pct(stop)
            liq_distance = remaining_pct(liq)
            stop_liq_buffer = "-"
            try:
                stop_px, liq_px = dec(stop), dec(liq)
                if stop_px > 0 and liq_px > 0 and entry > 0:
                    stop_liq_buffer = dstr(abs(liq_px - stop_px) / entry * D(100), 6)
            except Exception:
                pass

            logger.warning(
                f"OPEN POSITION | strategy={owner} | symbol={symbol} side={side} qty={qty} virtual_qty={virtual_qty} residual={dstr(residual, 8)} | "
                f"entry={entry} mark={mark} move_favoravel={dstr(favorable * D(100), 6)}% | notional_usd={notional} margin_isolada={margin} leverage={leverage}x unreal_pnl={unreal} | "
                f"tp={target} distancia_tp={tp_distance}% | stop={stop} distancia_stop={stop_distance}% | "
                f"liq={liq} distancia_liq={liq_distance}% buffer_stop_liq={stop_liq_buffer}% | recovery_level={recovery_level} | virtual_lots={virtual_lots}"
            )

            for x in candidates:
                x_qty = dec(x.get("qty"))
                try:
                    x_recovery = max(0, int(x.get("recovery", 0)))
                except Exception:
                    x_recovery = 0
                is_pyramid = str(x.get("strategy", "")).startswith("PYRAMID:")
                x_multiplier = D(1) if is_pyramid else RECOVERY_MULTIPLIER ** x_recovery
                x_base_notional = PYRAMID_INITIAL_NOTIONAL_USD if is_pyramid else configured_initial_notional(symbol)
                x_notional = x_qty * mark if mark > 0 else D(0)
                x_mode = "PYRAMID" if is_pyramid else ("NORMAL" if x_recovery == 0 else "RECOVERY")
                if is_pyramid:
                    x_recovery = 0
                logger.warning(
                    f"VIRTUAL STRATEGY | strategy={x.get('strategy')} | symbol={symbol} side={side} | qty={dstr(x_qty, 8)} | notional_usd={dstr(x_notional, 8)} | "
                    f"mode={x_mode} | recovery_level={x_recovery} | multiplier={dstr(x_multiplier, 4)}x | base_notional_usd={dstr(x_base_notional, 8)} | tp={x.get('target', '-')} | sl={x.get('stop', '-')}"
                )
        if found == 0:
            logger.info("OPEN POSITION | nenhuma posicao real aberta")

    def run(self) -> None:
        self.startup()
        if VALIDATE_API_ONLY:
            logger.info("VALIDATE_API_ONLY concluido; encerrando sem alterar configuracoes e sem enviar ordens")
            self.shutdown()
            return
        while not self.stop.is_set():
            try:
                if self.client.api_error_streak >= KILL_SWITCH_ON_API_ERRORS and self.store.killed() == "OFF":
                    self.store.kill("SOFT", f"API_ERROR_STREAK={self.client.api_error_streak}")
                if self.store.killed() == "HARD":
                    self.hard_kill()
                    self.store.kill("SOFT", "HARD_KILL_EXECUTED; manual review required")
                prices = {s: self.md.get(s) for s in SYMBOLS}

                _now_reconcile = now_ms()
                if _now_reconcile - self._last_periodic_reconcile_ms >= int(RECONCILE_INTERVAL_SECONDS * 1000):
                    self._last_periodic_reconcile_ms = _now_reconcile
                    try:
                        self.reconciler.reconcile()
                    except Exception as _re:
                        logger.warning(f"PERIODIC RECONCILE FAIL | {_re}")

                for e in self.range_engines:
                    p = prices.get(e.symbol)
                    if p and p > 0:
                        try: e.tick(p)
                        except Exception as ex: logger.exception(f"RANGE TICK FAIL | {e.symbol} | {ex}")
                for e in self.pyramid_engines:
                    p = prices.get(e.symbol)
                    if p and p > 0:
                        try: e.tick(p)
                        except Exception as ex: logger.exception(f"PYRAMID TICK FAIL | {e.id} | {ex}")
                self.heartbeat()
            except KeyboardInterrupt:
                break
            except Exception as e:
                logger.exception(f"MAIN LOOP | {e}")
            self.stop.wait(MAIN_LOOP_SECONDS)
        self.shutdown()

    def shutdown(self) -> None:
        logger.info("SHUTDOWN | salvando estado")
        self.store.save()
        try: self.ledger.close()
        except Exception: pass
        self.md.stop.set(); self.news.stop.set(); self.stop.set()


def main() -> None:
    bot = Bot()
    def _sig(signum, frame):
        logger.warning(f"SIGNAL {signum} recebido")
        bot.stop.set()
    signal.signal(signal.SIGINT, _sig)
    signal.signal(signal.SIGTERM, _sig)
    bot.run()


if __name__ == "__main__":
    main()
