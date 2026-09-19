#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Perpetual Direcional production entrypoint v67.

Adds a narrowly-scoped runtime repair over runner.py v66:
- EXCHANGE_FLAT_RECOVERY_UNACCOUNTED is released per Hedge-Mode side when that
  exact PYRAMID strategy is provably flat, even if the opposite PYRAMID side is live;
- the live opposite side, its legs, anchor, native stop and accounting are untouched;
- the released side gets anchor=None so its next normal tick anchors at the current market;
- realized PnL/equity/bankroll/ledger history are preserved;
- only stale operational/protection blocks containing the same exchange-flat marker
  are cleared for the released strategy.

No market order is sent by this repair and no position is closed.
"""

import runner as base

bot = base.bot
MARKER = "EXCHANGE_FLAT_RECOVERY_UNACCOUNTED"

# v70: Direcional uses CROSS margin for BTC/ETH/HYPE.
# This overrides the legacy startup routine in main.py that forced ISOLATED.
# No position is closed and no market order is sent by this migration.
def _ensure_modes_cross_v70(self):
    if not bot.LIVE_TRADING:
        bot.logger.info("MODES | simulacao: nao altera Hedge/CROSS")
        return
    self.client.set_single_asset_mode()
    bot.logger.info("MODES | Single-Asset Mode confirmado")
    self.client.set_hedge_mode()
    if not self.client.position_mode():
        raise RuntimeError("Conta nao esta em Hedge Mode")
    for symbol in bot.SYMBOLS:
        self.client.set_margin_type(symbol, False)
    bot.logger.info("MODES | Hedge Mode confirmado | CROSS solicitado em %s", ",".join(bot.SYMBOLS))

bot.AccountManager.ensure_modes = _ensure_modes_cross_v70


# v71: one-shot adoption of the three user-reconstructed LONG positions after
# the ISOLATED -> CROSS migration. This never submits an entry order. It runs
# only when physical quantities AND the three manually restored protective
# stops exactly match the migration manifest, while ledger/state ownership is
# still zero. Afterwards normal PYRAMID ownership/protection logic resumes.
_MIGRATION_V71 = {
    "BTCUSDT": {"qty": bot.D("0.002"), "stop": bot.D("77810.5"), "next_level": 3},
    "ETHUSDT": {"qty": bot.D("0.051"), "stop": bot.D("2484.87"), "next_level": 5},
    "HYPEUSDT": {"qty": bot.D("2.19"), "stop": bot.D("86.341"), "next_level": 15},
}
_MIGRATION_MARKER_V71 = "cross_manual_reconstruction_adopted_v71"
_original_reconcile_v70 = bot.Reconciler.reconcile


def _adopt_cross_reconstruction_v71(reconciler):
    if not bot.LIVE_TRADING:
        return False
    with reconciler.store.lock:
        maintenance = reconciler.store.state.setdefault("maintenance", {})
        if (maintenance.get(_MIGRATION_MARKER_V71) or {}).get("completed"):
            return False

    # This repair is intentionally impossible to trigger on a partial/different
    # portfolio: exact three LONG quantities, no SHORT exposure, empty durable
    # ownership, empty PYRAMID state and exact manual STOP_MARKET prices required.
    snap = reconciler.snapshot()
    expected_physical = {(s, "LONG"): v["qty"] for s, v in _MIGRATION_V71.items()}
    actual = {k: bot.dec(v) for k, v in snap.positions.items() if bot.dec(v) > 0}
    if set(actual) != set(expected_physical):
        return False
    for key, wanted in expected_physical.items():
        step = reconciler.rules.rules[key[0]].step_size
        if abs(bot.dec(actual.get(key)) - wanted) >= step:
            return False
    if any(bot.dec(q) > 0 for q in reconciler.ledger.open_by_symbol_side().values()):
        return False
    if any(bot.dec(q) > 0 for q in reconciler.expected_from_state_by_symbol_side().values()):
        return False

    manual_stops = {}
    for order in snap.open_orders or []:
        sym = str(order.get("symbol") or "").upper()
        ps = str(order.get("positionSide") or "").upper()
        typ = str(order.get("type") or "").upper()
        status = str(order.get("status") or "NEW").upper()
        if sym not in _MIGRATION_V71 or ps != "LONG" or typ != "STOP_MARKET" or status not in ("NEW", "PARTIALLY_FILLED"):
            continue
        sp = bot.dec(order.get("stopPrice"))
        tick = reconciler.rules.rules[sym].tick_size
        if abs(sp - _MIGRATION_V71[sym]["stop"]) <= max(tick, tick * bot.D(2)):
            manual_stops[sym] = order
    if set(manual_stops) != set(_MIGRATION_V71):
        return False

    adopted = []
    with reconciler.store.lock:
        for sym, spec in _MIGRATION_V71.items():
            key = f"{sym}:LONG"
            st = reconciler.store.state["pyramid"][key]
            if st.get("legs") or st.get("native_risk_stop"):
                raise RuntimeError(f"V71_ADOPTION_STATE_NOT_EMPTY:{key}")
            entry = bot.dec(snap.entry_prices.get((sym, "LONG")))
            if entry <= 0:
                raise RuntimeError(f"V71_ADOPTION_ENTRY_MISSING:{sym}")
            leg_id = f"migration-v71-{sym.lower()}-long"
            leg = {
                "id": leg_id, "side": "LONG", "qty": str(spec["qty"]),
                "entry_price": str(entry), "signal_price": str(entry),
                "price_source": "EXCHANGE_POSITION_RECONSTRUCTION",
                "leverage": bot.PYRAMID_LEVERAGE,
                "requested_leverage": bot.PYRAMID_LEVERAGE,
                "notional": str(spec["qty"] * entry),
                "margin_est": str((spec["qty"] * entry) / bot.D(bot.PYRAMID_LEVERAGE)),
                "opened_at": bot.now_iso(), "reason": "CROSS_MANUAL_RECONSTRUCTION_V71",
            }
            reconciler.ledger.record_open_lot(
                leg_id, f"PYRAMID:{sym}:LONG", sym, "LONG",
                spec["qty"], entry, leg_id, "MANUAL_CROSS_RECONSTRUCTION_V71",
            )
            order = manual_stops[sym]
            cid = str(order.get("clientOrderId") or order.get("origClientOrderId") or "")
            st["legs"] = [leg]
            st["next_level"] = int(spec["next_level"])
            st["levels_filled"] = max(1, int(spec["next_level"]) - 1)
            st["last_trigger_price"] = str(entry)
            st["stopped"] = False
            st["stop_reason"] = None
            st["native_risk_stop"] = {
                "target_price": str(spec["stop"]),
                "orders": [{
                    "position_side": "LONG", "qty": str(spec["qty"]),
                    "client_id": cid, "order_id": order.get("orderId"),
                    "type": "STOP_MARKET", "stop_price": str(spec["stop"]),
                    "status": status,
                }],
                "installed_at": bot.now_iso(),
                "adopted_manual": True,
            }
            st["last_update"] = bot.now_iso()
            adopted.append({"symbol": sym, "qty": str(spec["qty"]), "entry": str(entry),
                            "stop": str(spec["stop"]), "next_level": int(spec["next_level"])})
        maintenance[_MIGRATION_MARKER_V71] = {
            "completed": True, "completed_at": bot.now_iso(), "adopted": adopted,
            "policy": "EXACT_PHYSICAL+EXACT_MANUAL_STOPS+EMPTY_LEDGER_STATE; ONE_SHOT",
        }
        reconciler.store.save()

    reconciler.store.set_trade_gate(True, None)
    reconciler.store.clear_soft_position_mismatch()
    bot.logger.warning("CROSS RECONSTRUCTION ADOPT V71 | COMPLETE | %s", adopted)
    return True


def _reconcile_with_cross_adoption_v71(self):
    try:
        _adopt_cross_reconstruction_v71(self)
    except Exception as exc:
        reason = f"CROSS_RECONSTRUCTION_V71_FAILED:{exc}"
        self.store.set_trade_gate(False, reason)
        bot.logger.exception("CROSS RECONSTRUCTION ADOPT V71 | FAIL CLOSED | %s", reason)
        return False
    return _original_reconcile_v70(self)


bot.Reconciler.reconcile = _reconcile_with_cross_adoption_v71
_original_pyramid_tick_v66 = bot.PyramidEngine.tick


def _strategy_side_provably_flat(engine) -> bool:
    """Prove only this PYRAMID strategy/side is flat without touching opposite side."""
    symbol = str(engine.symbol).upper()
    side = str(engine.side).upper()
    strategy_id = str(engine.id)

    if side not in ("LONG", "SHORT"):
        return False

    with engine.store.lock:
        st = engine.st()
        state_qty = sum(
            (bot.dec(x.get("qty")) for x in (st.get("legs") or []) if isinstance(x, dict)),
            bot.D(0),
        )
        native = st.get("native_risk_stop")
        gate = engine.store.state.get("trade_gate", {}) or {}
        if state_qty > 0 or native:
            return False
        if gate and not bool(gate.get("open_allowed", True)):
            return False

    if not bot.LIVE_TRADING:
        return True

    try:
        step = engine.exe.rules.rules[symbol].step_size
        own_ledger_qty = engine.exe.ledger.open_strategy_qty(strategy_id, symbol, side)
        if own_ledger_qty >= step:
            return False

        rows = engine.client.positions(symbol)
        if not isinstance(rows, list):
            return False
        physical_side = bot.D(0)
        for row in rows:
            if (
                str(row.get("symbol") or "").upper() == symbol
                and str(row.get("positionSide") or "").upper() == side
            ):
                physical_side = abs(bot.dec(row.get("positionAmt")))
                break

        ledger_all = engine.exe.ledger.open_by_symbol_side()
        aggregate_ledger = bot.dec(ledger_all.get((symbol, side), bot.D(0)))
        if abs(aggregate_ledger - physical_side) >= step:
            return False

        orders = engine.client.open_orders(symbol)
        if not isinstance(orders, list):
            return False
        for order in orders:
            if str(order.get("positionSide") or "").upper() != side:
                continue
            if str(order.get("status") or "NEW").upper() not in ("NEW", "PARTIALLY_FILLED"):
                continue
            cid = str(order.get("clientOrderId") or order.get("origClientOrderId") or "")
            owner = engine.exe.ledger.order_owner(cid) if cid else None
            if owner and (owner == strategy_id or str(owner).startswith(strategy_id + ":")):
                return False

        return True
    except Exception as exc:
        bot.logger.warning(
            "PYRAMID SIDE FLAT PROOF FAIL | %s | %s",
            strategy_id,
            exc,
        )
        return False


def _clear_marker_blocks(engine) -> None:
    strategy_id = str(engine.id)
    with engine.store.lock:
        op_reason = str(
            (engine.store.state.get("operational_blocks", {}) or {}).get(strategy_id) or ""
        )
        pr_reason = str(
            (engine.store.state.get("protection_blocks", {}) or {}).get(strategy_id) or ""
        )
    if MARKER in op_reason.upper():
        engine.store.set_operational_block(strategy_id, None)
    if MARKER in pr_reason.upper():
        engine.store.set_protection_block(strategy_id, None)


def _release_exchange_flat_side_if_safe(engine) -> bool:
    st = engine.st()
    if not bool(st.get("stopped")):
        return False
    reason = str(st.get("stop_reason") or "")
    if MARKER not in reason.upper():
        return False
    if not _strategy_side_provably_flat(engine):
        return False

    previous = {
        "equity": str(st.get("equity", "")),
        "realized_pnl": str(st.get("realized_pnl", "")),
        "bankroll": str(st.get("bankroll", "")),
        "reason": reason,
    }

    with engine.store.lock:
        # Recheck local state immediately before mutation.
        st = engine.st()
        state_qty = sum(
            (bot.dec(x.get("qty")) for x in (st.get("legs") or []) if isinstance(x, dict)),
            bot.D(0),
        )
        if state_qty > 0 or st.get("native_risk_stop"):
            return False
        st["stopped"] = False
        st["stop_reason"] = None
        st["anchor"] = None
        st["next_level"] = 1
        st["levels_filled"] = 0
        st["last_trigger_price"] = None
        st["native_risk_stop"] = None
        st["return_exit_armed"] = False
        st["return_exit_reference"] = None
        st["last_update"] = bot.now_iso()
        engine.store.save()

    _clear_marker_blocks(engine)
    bot.logger.warning(
        "PYRAMID SIDE EXCHANGE-FLAT RELEASE | %s | side_flat=CONFIRMED | "
        "opposite_side=UNTOUCHED | anchor=RESET_TO_NEXT_CURRENT_MARK | accounting=PRESERVED | previous=%s",
        engine.id,
        previous,
    )
    return True


def _pyramid_tick_v67(self, price):
    _release_exchange_flat_side_if_safe(self)
    return _original_pyramid_tick_v66(self, price)


bot.PyramidEngine.tick = _pyramid_tick_v67
bot.VERSION = f"{bot.VERSION}-side-flat-runtime-release-v67"
# v68: stepped native profit-lock for PYRAMID, measured from operational anchor.
# User policy: at +5% from anchor lock +1% above/below physical average entry;
# from +10%, advance another 1% for every complete +2% favorable move.
_original_effective_native_stop_v68 = bot.PyramidEngine._effective_native_stop_price


def _profit_lock_pct_v68(favorable_from_anchor):
    f = bot.dec(favorable_from_anchor)
    if f < bot.D("0.05"):
        return bot.D(0)
    if f < bot.D("0.10"):
        return bot.D("0.01")
    steps = int((f - bot.D("0.10")) / bot.D("0.02"))
    return bot.D("0.02") + bot.D(steps) * bot.D("0.01")


def _physical_entry_v68(engine):
    try:
        p = engine._physical_position_snapshot() or {}
        for k in ("entryPrice", "entry_price"):
            v = bot.dec(p.get(k))
            if v > 0:
                return v
    except Exception:
        pass
    return engine._weighted_entry_price()


def _effective_native_stop_v68(self, mark):
    target, meta = _original_effective_native_stop_v68(self, mark)
    st = self.st()
    anchor = bot.dec(st.get("anchor"))
    entry = _physical_entry_v68(self)
    mark = bot.dec(mark)
    if anchor <= 0 or entry <= 0 or mark <= 0:
        return target, meta
    favorable = ((mark / anchor) - bot.D(1)) if self.side == "LONG" else ((anchor / mark) - bot.D(1))
    lock_pct = _profit_lock_pct_v68(favorable)
    if lock_pct <= 0:
        return target, meta
    raw = entry * (bot.D(1) + lock_pct) if self.side == "LONG" else entry * (bot.D(1) - lock_pct)
    direction = "DOWN" if self.side == "LONG" else "UP"
    lock_stop = self.exe.rules.trigger_price(self.symbol, raw, direction)
    if target is None:
        chosen = lock_stop
    else:
        chosen = max(target, lock_stop) if self.side == "LONG" else min(target, lock_stop)
    valid = chosen < mark if self.side == "LONG" else chosen > mark
    meta = dict(meta or {})
    meta.update({"profit_lock_v68": lock_stop, "profit_lock_pct": lock_pct, "favorable_from_anchor": favorable, "physical_entry_v68": entry})
    return (chosen if valid else None), meta


bot.PyramidEngine._effective_native_stop_price = _effective_native_stop_v68

bot.VERSION = f"{bot.VERSION}-anchor-profit-lock-v68-monotonic-v69-cross-v70-adopt-v71-log-v72"


def main() -> None:
    bot.logger.warning(
        "PYRAMID SIDE-FLAT RUNTIME RELEASE FIX ACTIVE | version=v75 | margin=CROSS | profit_lock=anchor:+5%%=>entry+1%%; +10%%=>entry+2%%; every+2%%=>+1%%; monotonic=NEVER_LOOSEN_NATIVE_STOP | marker=%s | "
        "policy=EXACT_SIDE_PROOF; OPPOSITE_SIDE_UNTOUCHED; ACCOUNTING_PRESERVED",
        MARKER,
    )
    base.main()


# PRE_MIGRATION_TRAILING_V73
# The CROSS reconstruction created new exchange entry prices, but economically
# these positions continue the pre-migration PYRAMID baskets. Preserve the last
# confirmed pre-migration trailing stops instead of tightening them to the
# 1%-adverse stop of the reconstructed exchange entries. Once the normal v68/v69
# profit-lock produces a stop stricter than this legacy floor, monotonic trailing
# resumes from the stricter value and can never loosen again.
_PRE_MIGRATION_TRAILING_V73 = {
    "BTCUSDT": bot.D("77810.5"),
    "ETHUSDT": bot.D("2484.87"),
    "HYPEUSDT": bot.D("86.341"),
}
_effective_native_stop_v72 = bot.PyramidEngine._effective_native_stop_price

def _effective_native_stop_v73(self, mark):
    target, meta = _effective_native_stop_v72(self, mark)
    if self.side != "LONG" or self.symbol not in _PRE_MIGRATION_TRAILING_V73:
        return target, meta
    maintenance = self.store.state.get("maintenance", {})
    adopted = (maintenance.get(_MIGRATION_MARKER_V71) or {}).get("completed")
    if not adopted:
        return target, meta
    legacy = _PRE_MIGRATION_TRAILING_V73[self.symbol]
    # v68/v69 target includes the reconstructed-entry max-loss stop. During this
    # continuity bridge that component must not tighten the inherited basket.
    # Only a genuine profit-lock step may supersede the inherited stop.
    physical_entry = _physical_entry_v68(self)
    anchor = bot.dec(self.st().get("anchor"))
    lock_target = None
    favorable = bot.D(0)
    lock_pct = bot.D(0)
    if anchor > 0 and physical_entry > 0:
        favorable = (mark / anchor) - bot.D(1)
        lock_pct = _profit_lock_pct_v68(favorable)
        if lock_pct > 0:
            raw = physical_entry * (bot.D(1) + lock_pct)
            lock_target = self.exe.rules.trigger_price(self.symbol, raw, "DOWN")
    chosen = legacy
    if lock_target is not None and lock_target > chosen:
        chosen = lock_target
    # Never loosen an already-live bot-owned stop.
    existing = self.st().get("native_risk_stop") or {}
    old = bot.dec(existing.get("target_price"))
    if old > 0 and old != target and old > chosen:
        chosen = old
    if chosen >= mark:
        return target, meta
    meta = dict(meta or {})
    meta.update({
        "continuity_v73": True,
        "pre_migration_stop": legacy,
        "profit_lock_only_target": lock_target,
        "favorable_from_anchor": favorable,
        "profit_lock_pct": lock_pct,
        "reconstructed_entry_max_loss_ignored": True,
    })
    return chosen, meta

bot.PyramidEngine._effective_native_stop_price = _effective_native_stop_v73

# V75: force one safe refresh after startup so stale v71/v72 native stops are
# replaced immediately by the continuity target computed above. This does not
# submit entries or close positions; it uses the existing native-stop replace
# path (verify -> cancel old protective stop -> install new protective stop).
_original_tick_v74 = bot.PyramidEngine.tick
_v75_refreshed = set()

def _tick_force_continuity_refresh_v75(self, price):
    key = str(self.id)
    if self.side == "LONG" and self.symbol in _PRE_MIGRATION_TRAILING_V73 and key not in _v75_refreshed:
        maintenance = self.store.state.get("maintenance", {})
        adopted = (maintenance.get(_MIGRATION_MARKER_V71) or {}).get("completed")
        if adopted and (self.st().get("legs") or []):
            ok = self._ensure_native_risk_stop(bot.dec(price), force=True)
            if ok:
                _v75_refreshed.add(key)
                bot.logger.warning(
                    "PRE-MIGRATION TRAILING V75 | REFRESH CONFIRMED | %s | target=%s",
                    self.id, (self.st().get("native_risk_stop") or {}).get("target_price"),
                )
            else:
                bot.logger.error("PRE-MIGRATION TRAILING V75 | REFRESH NOT CONFIRMED | %s", self.id)
    return _original_tick_v74(self, price)

bot.PyramidEngine.tick = _tick_force_continuity_refresh_v75
bot.VERSION = f"{bot.VERSION}-premigration-trailing-v73-loadorder-v74-refresh-v75"


# MARGIN LOG FIX V72
# main.py still has a legacy informational banner hard-coded as ISOLATED.
# Runtime mode is CROSS (v70+). Suppress only that stale banner and emit the
# authoritative CROSS banner; no trading/risk/order behavior is changed.
_original_info_v72 = bot.logger.info

def _info_cross_log_v72(msg, *args, **kwargs):
    rendered = str(msg)
    if rendered.startswith("MARGIN=ISOLATED | MODE=HEDGE"):
        rendered = rendered.replace("MARGIN=ISOLATED", "MARGIN=CROSS", 1)
        msg = rendered
        args = ()
    return _original_info_v72(msg, *args, **kwargs)

bot.logger.info = _info_cross_log_v72


if __name__ == "__main__":
    main()
