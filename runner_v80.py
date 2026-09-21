#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Perpetual Direcional production entrypoint v80.

Corrects v79 stale SCALPER protection-block release. StateStore.entry_allowed()
derives PROTECTION_BLOCK directly from protection_blocks; trade_gate itself can
remain open. Therefore release is keyed to the exact persisted SCALPER block,
not to trade_gate.reason.

Release remains fail-closed and requires: no SCALPER state position/native stop,
zero SCALPER durable ledger quantity on both Hedge sides, and aggregate physical
quantity equal to aggregate durable ledger quantity on both sides. No orders or
positions are changed and accounting/risk parameters are preserved.
"""

import runner_v79 as base

bot = base.bot
_STALE_TOKEN = "SCALPER_NATIVE_STOP_INSTALL_FAILED"


def _scalper_flat_proof_v80(engine):
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


def _release_stale_scalper_stop_block_v80(engine):
    sid = str(engine.id)
    with engine.store.lock:
        reason = str(
            (engine.store.state.get("protection_blocks", {}) or {}).get(sid) or ""
        )
    if _STALE_TOKEN not in reason:
        return False

    try:
        safe, proof = _scalper_flat_proof_v80(engine)
    except Exception as exc:
        bot.logger.warning(
            "SCALPER STALE STOP BLOCK V80 | PROOF FAIL | %s | %s", sid, exc
        )
        return False
    if not safe:
        bot.logger.warning(
            "SCALPER STALE STOP BLOCK V80 | KEEP FAIL-CLOSED | %s | %s", sid, proof
        )
        return False

    # Re-read immediately before mutation so a newer protection failure cannot be
    # cleared based on an older observation.
    with engine.store.lock:
        current = str(
            (engine.store.state.get("protection_blocks", {}) or {}).get(sid) or ""
        )
    if current != reason or _STALE_TOKEN not in current:
        return False

    engine.store.set_protection_block(sid, None)

    with engine.store.lock:
        after = str(
            (engine.store.state.get("protection_blocks", {}) or {}).get(sid) or ""
        )
    if after:
        bot.logger.error(
            "SCALPER STALE STOP BLOCK V80 | RELEASE UNCONFIRMED | %s | block=%s",
            sid, after,
        )
        return False

    bot.logger.warning(
        "SCALPER STALE STOP BLOCK V80 | RELEASE CONFIRMED | %s | %s | "
        "accounting=PRESERVED orders=UNTOUCHED positions=UNTOUCHED",
        sid, proof,
    )
    return True


# Replace the v79 tick wrapper entirely so the obsolete trade_gate predicate is
# not evaluated. Its saved original is the v78 chain.
_original_scalper_tick_v78 = base._original_scalper_tick_v78


def _scalper_tick_v80(self, ref):
    _release_stale_scalper_stop_block_v80(self)
    return _original_scalper_tick_v78(self, ref)


bot.ScalperEngine.tick = _scalper_tick_v80
bot.VERSION = f"{bot.VERSION}-stateblock-release-v80"


def main():
    bot.logger.warning(
        "SCALPER STALE STOP BLOCK FIX ACTIVE | version=v80 | "
        "source=protection_blocks | release=EXACT_FAILED_STOP_BLOCK+SCALPER_FLAT+AGGREGATE_RECONCILED | "
        "fail_closed=True | accounting=PRESERVED | orders=UNTOUCHED | positions=UNTOUCHED"
    )
    base.main()


if __name__ == "__main__":
    main()
