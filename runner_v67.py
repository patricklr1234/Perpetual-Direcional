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


def main() -> None:
    bot.logger.warning(
        "PYRAMID SIDE-FLAT RUNTIME RELEASE FIX ACTIVE | version=v67 | marker=%s | "
        "policy=EXACT_SIDE_PROOF; OPPOSITE_SIDE_UNTOUCHED; ACCOUNTING_PRESERVED",
        MARKER,
    )
    base.main()


if __name__ == "__main__":
    main()
