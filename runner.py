#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Production entrypoint for Perpetual Direcional.

Keeps the PYRAMID risk-policy override, fixes watchdog observability, and repairs
the stale SCALPER exchange-flat release path without changing strategy execution:
- Native STOP_MARKET starts 1% adverse to weighted entry.
- After +1% favorable movement, existing main.py logic arms breakeven.
- OPEN_ORDERS_AUDIT uses ExchangeSnapshot.captured_ms (the actual dataclass field)
  instead of the obsolete timestamp_ms name.
- A SCALPER stopped only by EXCHANGE_FLAT_RECOVERY_UNACCOUNTED is auto-released
  after proving both Hedge-Mode sides are physically flat, have no open orders,
  durable ledger ownership is zero, and persistent state has no position/native stop.

State, ledger, bankrolls, positions, native orders, sizing, recovery parameters,
BOT_DIR and Volume semantics are preserved. This repair never cancels an order,
closes a position, mutates fill-ledger ownership, or resets bankroll/PnL.
"""

import main as bot


def _pyramid_one_percent_native_stop_price(self):
    entry = self._weighted_entry_price()
    if entry <= 0:
        return None

    pct = bot.PYRAMID_ADVERSE_REVERSAL_PCT
    if pct <= 0 or pct >= bot.D(1):
        raise RuntimeError(f"PYRAMID_ADVERSE_REVERSAL_PCT invalido para native stop: {pct}")

    if self.side == "LONG":
        raw = entry * (bot.D(1) - pct)
        direction = "DOWN"
    else:
        raw = entry * (bot.D(1) + pct)
        direction = "UP"

    if raw <= 0:
        return None
    return self.exe.rules.trigger_price(self.symbol, raw, direction)


def _fixed_audit_extract(reconciler, ledger):
    result = {"positions": [], "orders": [], "coverage": {}, "timestamp_ms": 0}
    snap = reconciler.last_snapshot
    if not snap:
        return result

    result["timestamp_ms"] = int(getattr(snap, "captured_ms", 0) or 0)

    for (sym, side), qty in snap.positions.items():
        qty = bot.dec(qty)
        if qty > 0:
            result["positions"].append({
                "symbol": str(sym).upper(),
                "positionSide": str(side).upper(),
                "qty": str(qty),
                "entryPrice": str(bot.dec(snap.entry_prices.get((sym, side), bot.D(0)))),
            })

    for o in snap.open_orders or []:
        status = str(o.get("status", "")).upper()
        if status not in ("NEW", "PARTIALLY_FILLED"):
            continue
        cid = str(o.get("clientOrderId") or o.get("origClientOrderId") or "")
        owner = ledger.order_owner(cid) if cid else None
        sym = str(o.get("symbol", "")).upper()
        ps = str(o.get("positionSide", "")).upper()
        otype = str(o.get("type", "")).upper()
        orig_qty = bot.dec(o.get("origQty"))
        exec_qty = bot.dec(o.get("executedQty"))
        remain = max(bot.D(0), orig_qty - exec_qty)

        rec = {
            "orderId": str(o.get("orderId", "")),
            "clientOrderId": cid,
            "owner": owner or "UNOWNED",
            "symbol": sym,
            "positionSide": ps,
            "side": str(o.get("side", "")).upper(),
            "type": otype,
            "status": status,
            "origQty": str(orig_qty),
            "executedQty": str(exec_qty),
            "remainingQty": str(remain),
            "stopPrice": str(o.get("stopPrice", "")),
            "price": str(o.get("price", "")),
            "reduceOnly": bool(o.get("reduceOnly", False)),
            "workingType": str(o.get("workingType", "")),
            "priceProtect": bool(o.get("priceProtect", False)),
        }
        result["orders"].append(rec)

        if remain > 0 and otype in ("STOP_MARKET", "TAKE_PROFIT_MARKET") and sym and ps:
            key = f"{sym}:{ps}"
            result["coverage"].setdefault(key, {})
            result["coverage"][key][otype] = str(
                bot.dec(result["coverage"][key].get(otype, "0")) + remain
            )

    result["positions"].sort(key=lambda x: (x["symbol"], x["positionSide"]))
    result["orders"].sort(key=lambda x: (x["symbol"], x["positionSide"], x["type"], x["clientOrderId"]))
    return result


_original_release_exchange_flat_recovery_blocks = bot.Bot.release_exchange_flat_recovery_blocks


def _release_exchange_flat_recovery_blocks_with_scalper(self) -> None:
    """Run v64 release and additionally release provably-flat stale SCALPER blocks.

    The main.py v64 implementation only enumerates pyramid buckets. This wrapper
    keeps that behavior and adds the missing scalper bucket conservatively.
    """
    _original_release_exchange_flat_recovery_blocks(self)

    if not bot.LIVE_TRADING:
        return

    marker = "EXCHANGE_FLAT_RECOVERY_UNACCOUNTED"
    candidates = []

    with self.store.lock:
        for key, st in self.store.state.get("scalper", {}).items():
            if not isinstance(st, dict):
                continue
            if str(st.get("stop_reason") or "").upper() != marker:
                continue
            strategy_id = str(st.get("strategy") or f"SCALPER:{key}")
            symbol = str(st.get("symbol") or key).upper()
            candidates.append((strategy_id, symbol, st))

    released = []
    skipped = []

    for strategy_id, symbol, st in candidates:
        try:
            if not symbol:
                raise RuntimeError("symbol vazio")

            positions = self.client.positions(symbol)
            if not isinstance(positions, list):
                raise RuntimeError(f"positionRisk indeterminado: {positions!r}")

            physical = {"LONG": bot.D(0), "SHORT": bot.D(0)}
            for row in positions:
                if str(row.get("symbol") or "").upper() != symbol:
                    continue
                side = str(row.get("positionSide") or "").upper()
                if side in physical:
                    physical[side] = abs(bot.dec(row.get("positionAmt")))

            orders = self.client.open_orders(symbol)
            if not isinstance(orders, list):
                raise RuntimeError(f"openOrders indeterminado: {orders!r}")
            hedge_orders = [
                o for o in orders
                if str(o.get("positionSide") or "").upper() in ("LONG", "SHORT")
            ]

            with self.store.lock:
                pos = st.get("position")
                native = st.get("native_risk_stop")

            ledger_long = self.ledger.open_strategy_qty(strategy_id, symbol, "LONG")
            ledger_short = self.ledger.open_strategy_qty(strategy_id, symbol, "SHORT")

            provably_flat = (
                physical["LONG"] <= 0
                and physical["SHORT"] <= 0
                and not hedge_orders
                and not pos
                and not native
                and ledger_long <= 0
                and ledger_short <= 0
            )

            if not provably_flat:
                skipped.append({
                    "strategy": strategy_id,
                    "symbol": symbol,
                    "physical_long": str(physical["LONG"]),
                    "physical_short": str(physical["SHORT"]),
                    "open_orders": len(hedge_orders),
                    "state_position": bool(pos),
                    "native_stop": bool(native),
                    "ledger_long": str(ledger_long),
                    "ledger_short": str(ledger_short),
                })
                continue

            with self.store.lock:
                # Preserve bankroll, equity, realized PnL, recovery deficit and all
                # lifetime counters. Only remove the stale operational stop marker.
                st["stopped"] = False
                st["stop_reason"] = None
                st["last_update"] = bot.now_iso()
                self.store.save()

                op_reason = str(
                    (self.store.state.get("operational_blocks", {}) or {}).get(strategy_id) or ""
                )
                pr_reason = str(
                    (self.store.state.get("protection_blocks", {}) or {}).get(strategy_id) or ""
                )

            if marker in op_reason.upper():
                self.store.set_operational_block(strategy_id, None)
            if marker in pr_reason.upper():
                self.store.set_protection_block(strategy_id, None)

            released.append({
                "strategy": strategy_id,
                "symbol": symbol,
                "bankroll_preserved": str(st.get("bankroll", "")),
                "equity_preserved": str(st.get("equity", "")),
                "realized_pnl_preserved": str(st.get("realized_pnl", "")),
                "recovery_deficit_preserved": str(st.get("recovery_deficit", "")),
            })
            bot.logger.warning(
                "SCALPER EXCHANGE FLAT RECOVERY RELEASE | strategy=%s | symbol=%s | "
                "flat=CONFIRMED_BOTH_SIDES | accounting=PRESERVED",
                strategy_id,
                symbol,
            )

        except Exception as exc:
            skipped.append({
                "strategy": strategy_id,
                "symbol": symbol,
                "reason": f"VERIFY_FAILED:{exc}",
            })
            bot.logger.exception(
                "SCALPER EXCHANGE FLAT RECOVERY RELEASE | SKIP FAIL-CLOSED | %s",
                strategy_id,
            )

    with self.store.lock:
        maintenance = self.store.state.setdefault("maintenance", {})
        maintenance["scalper_exchange_flat_auto_release_v65"] = {
            "at": bot.now_iso(),
            "released": released,
            "skipped": skipped,
            "policy": (
                "BOTH_HEDGE_SIDES_PHYSICALLY_FLAT; NO_OPEN_ORDERS; "
                "NO_STATE_POSITION; NO_NATIVE_STOP; NO_LEDGER_OWNERSHIP; "
                "ACCOUNTING_PRESERVED"
            ),
        }
        self.store.save()

    bot.logger.warning(
        "SCALPER EXCHANGE FLAT RECOVERY RELEASE COMPLETE | released=%s skipped=%s",
        len(released),
        len(skipped),
    )


bot.PyramidEngine._max_loss_stop_price = _pyramid_one_percent_native_stop_price
bot.audit_extract_open_orders_and_positions = _fixed_audit_extract
bot.Bot.release_exchange_flat_recovery_blocks = _release_exchange_flat_recovery_blocks_with_scalper
bot.VERSION = f"{bot.VERSION}-native-1pct-be-auditfix-scalper-flat-release-v65"


def main() -> None:
    bot.logger.warning(
        "PYRAMID RISK POLICY ACTIVE | native_adverse_stop=%s%% | breakeven_after_favorable=%s%% | max_loss_usd=%s additional_software_ceiling",
        bot.PYRAMID_ADVERSE_REVERSAL_PCT * bot.D(100),
        bot.PYRAMID_RETURN_EXIT_ARM_PCT * bot.D(100),
        bot.PYRAMID_MAX_LOSS_USD,
    )
    bot.logger.warning("OPEN_ORDERS_AUDIT FIX ACTIVE | snapshot_field=captured_ms")
    bot.logger.warning("SCALPER FLAT RELEASE FIX ACTIVE | version=v65 | accounting=PRESERVED")
    bot.main()


if __name__ == "__main__":
    main()
