#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Perpetual Direcional production entrypoint v79.

Repairs stale global trade-gate blocks left by a failed SCALPER native-stop
installation after the failed entry has already been safely closed and the
SCALPER is provably flat.

The underlying fail-closed behavior remains unchanged while any SCALPER
position, native stop, durable ledger ownership, or physical quantity that is
not fully explained by the durable ledger exists.  The repair never cancels
orders, closes positions, resets accounting, changes risk parameters, or
modifies PYRAMID protection.
"""

import runner_v78 as base

bot = base.bot

_STALE_PREFIX = "PROTECTION_BLOCK:SCALPER:"
_STALE_TOKEN = "SCALPER_NATIVE_STOP_INSTALL_FAILED"


def _scalper_flat_proof_v79(engine):
    """Return (safe, details) without mutating exchange/state."""
    symbol = str(engine.symbol).upper()
    sid = str(engine.id)
    st = engine.st()
    if st.get("position") or st.get("native_risk_stop"):
        return False, "STATE_NOT_FLAT"

    step = engine.exe.rules.rules[symbol].step_size
    own_long = engine.exe.ledger.open_strategy_qty(sid, symbol, "LONG")
    own_short = engine.exe.ledger.open_strategy_qty(sid, symbol, "SHORT")
    if own_long >= step or own_short >= step:
        return False, f"SCALPER_LEDGER_NOT_FLAT long={own_long} short={own_short}"

    if not bot.LIVE_TRADING:
        return True, "SIMULATION_FLAT"

    rows = engine.client.positions(symbol)
    if not isinstance(rows, list):
        return False, "POSITION_SNAPSHOT_UNKNOWN"
    physical = {"LONG": bot.D(0), "SHORT": bot.D(0)}
    for row in rows:
        if str(row.get("symbol") or "").upper() != symbol:
            continue
        side = str(row.get("positionSide") or "").upper()
        if side in physical:
            physical[side] = abs(bot.dec(row.get("positionAmt")))

    # Other strategies (notably PYRAMID) may legitimately own the same physical
    # side.  Prove there is no unexplained residual instead of requiring the
    # entire symbol to be physically flat.
    ledger_all = engine.exe.ledger.open_by_symbol_side()
    for side in ("LONG", "SHORT"):
        expected = bot.dec(ledger_all.get((symbol, side), bot.D(0)))
        if abs(expected - physical[side]) >= step:
            return False, (
                f"AGGREGATE_MISMATCH side={side} physical={physical[side]} "
                f"ledger={expected} step={step}"
            )

    return True, (
        f"SCALPER_STATE_LEDGER_FLAT aggregate_reconciled "
        f"physical_long={physical['LONG']} physical_short={physical['SHORT']}"
    )


def _release_stale_scalper_stop_gate_v79(engine):
    """Clear only the proven stale SCALPER stop-install protection block."""
    sid = str(engine.id)
    with engine.store.lock:
        pblocks = engine.store.state.get("protection_blocks", {}) or {}
        reason = str(pblocks.get(sid) or "")
        gate = engine.store.state.get("trade_gate", {}) or {}
        gate_reason = str(gate.get("reason") or "")

    if _STALE_TOKEN not in reason:
        return False
    expected_gate = f"PROTECTION_BLOCK:{sid}:{reason}"
    if gate_reason != expected_gate:
        # Never clear a different/newer global safety condition.
        return False

    try:
        safe, proof = _scalper_flat_proof_v79(engine)
    except Exception as exc:
        bot.logger.warning(
            "SCALPER STALE STOP BLOCK V79 | PROOF FAIL | %s | %s", sid, exc
        )
        return False
    if not safe:
        bot.logger.warning(
            "SCALPER STALE STOP BLOCK V79 | KEEP FAIL-CLOSED | %s | %s", sid, proof
        )
        return False

    # StateStore owns the invariant between protection_blocks and trade_gate.
    # Clear through the existing API only after the exact stale reason and flat
    # proof above have both been confirmed.
    engine.store.set_protection_block(sid, None)

    with engine.store.lock:
        p_after = str(
            (engine.store.state.get("protection_blocks", {}) or {}).get(sid) or ""
        )
        gate_after = engine.store.state.get("trade_gate", {}) or {}
        gate_open = bool(gate_after.get("open_allowed", True))
        gate_reason_after = str(gate_after.get("reason") or "")

    if p_after or not gate_open:
        bot.logger.error(
            "SCALPER STALE STOP BLOCK V79 | RELEASE UNCONFIRMED | %s | "
            "pblock=%s gate_open=%s gate_reason=%s",
            sid, p_after, gate_open, gate_reason_after,
        )
        return False

    bot.logger.warning(
        "SCALPER STALE STOP BLOCK V79 | RELEASE CONFIRMED | %s | %s | "
        "accounting=PRESERVED orders=UNTOUCHED positions=UNTOUCHED",
        sid, proof,
    )
    return True


_original_scalper_tick_v78 = bot.ScalperEngine.tick


def _scalper_tick_v79(self, ref):
    _release_stale_scalper_stop_gate_v79(self)
    return _original_scalper_tick_v78(self, ref)


bot.ScalperEngine.tick = _scalper_tick_v79
bot.VERSION = f"{bot.VERSION}-scalper-stale-stop-gate-v79"


def main():
    bot.logger.warning(
        "SCALPER STALE STOP GATE FIX ACTIVE | version=v79 | "
        "release=EXACT_FAILED_STOP_BLOCK+SCALPER_STATE_LEDGER_FLAT+AGGREGATE_RECONCILED | "
        "fail_closed=True | accounting=PRESERVED | orders=UNTOUCHED | positions=UNTOUCHED"
    )
    base.main()


if __name__ == "__main__":
    main()
