#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Perpetual Direcional production entrypoint v77.

Adds an exact quantity-coverage guard for PYRAMID native STOP_MARKET orders.
The underlying v76 logic correctly validates target price and order liveness, but
could accept a still-LIVE stop whose quantity belonged to an older, smaller
basket after the PYRAMID position quantity changed.

This layer does not submit entries, close positions, reset state/accounting, or
change strategy/risk parameters.  It only forces the existing native-stop
replacement path when all of the following are true:
- the PYRAMID side has live legs;
- persisted native-stop quantity differs from the current leg quantity by at
  least one exchange quantity step; and
- physical exchange quantity exactly matches the current state/legs quantity.

If physical quantity cannot be proved equal to state, it fails closed instead of
changing the protective order.
"""

import runner_v67 as base

bot = base.bot

_original_ensure_native_risk_stop_v76 = bot.PyramidEngine._ensure_native_risk_stop


def _native_stop_qty_v77(native):
    total = bot.D(0)
    if not isinstance(native, dict):
        return total
    for order in native.get("orders") or []:
        if not isinstance(order, dict):
            continue
        status = str(order.get("status") or "NEW").upper()
        if status not in ("NEW", "PARTIALLY_FILLED"):
            continue
        remaining = bot.dec(order.get("remaining_qty") or order.get("remainingQty"))
        if remaining > 0:
            total += remaining
            continue
        qty = bot.dec(order.get("qty") or order.get("origQty"))
        executed = bot.dec(order.get("executed_qty") or order.get("executedQty"))
        total += max(bot.D(0), qty - executed)
    return total


def _physical_side_qty_v77(engine):
    snap = engine._physical_position_snapshot() or {}
    raw = snap.get("positionAmt")
    if raw is None:
        raw = snap.get("qty")
    if raw is None:
        raw = snap.get("position_qty")
    return abs(bot.dec(raw))


def _ensure_native_risk_stop_v77(self, price, force=False):
    st = self.st()
    legs = list(st.get("legs") or [])
    expected_qty = sum(
        (bot.dec(leg.get("qty")) for leg in legs if isinstance(leg, dict)),
        bot.D(0),
    )
    native = st.get("native_risk_stop")
    protected_qty = _native_stop_qty_v77(native)
    step = self.exe.rules.rules[self.symbol].step_size

    qty_mismatch = (
        expected_qty > 0
        and native
        and abs(expected_qty - protected_qty) >= step
    )

    if qty_mismatch:
        try:
            physical_qty = _physical_side_qty_v77(self)
        except Exception as exc:
            self.store.set_protection_block(
                self.id, "PYRAMID_NATIVE_STOP_QTY_PHYSICAL_VERIFY_FAILED_V77"
            )
            bot.logger.exception(
                "PYRAMID STOP QTY V77 | PHYSICAL VERIFY FAIL | %s | state_qty=%s protected_qty=%s | %s",
                self.id, expected_qty, protected_qty, exc,
            )
            return False

        if abs(physical_qty - expected_qty) >= step:
            self.store.set_protection_block(
                self.id, "PYRAMID_NATIVE_STOP_QTY_STATE_PHYSICAL_MISMATCH_V77"
            )
            bot.logger.error(
                "PYRAMID STOP QTY V77 | FAIL CLOSED | %s | state_qty=%s physical_qty=%s protected_qty=%s step=%s",
                self.id, expected_qty, physical_qty, protected_qty, step,
            )
            return False

        bot.logger.warning(
            "PYRAMID STOP QTY V77 | REFRESH REQUIRED | %s | state_qty=%s physical_qty=%s protected_qty=%s step=%s",
            self.id, expected_qty, physical_qty, protected_qty, step,
        )
        force = True

    ok = _original_ensure_native_risk_stop_v76(self, bot.dec(price), force=force)
    if not ok:
        return False

    if qty_mismatch:
        refreshed_qty = _native_stop_qty_v77(self.st().get("native_risk_stop"))
        if abs(refreshed_qty - expected_qty) >= step:
            self.store.set_protection_block(
                self.id, "PYRAMID_NATIVE_STOP_QTY_REFRESH_UNCONFIRMED_V77"
            )
            bot.logger.error(
                "PYRAMID STOP QTY V77 | REFRESH UNCONFIRMED | %s | expected=%s persisted_protected=%s step=%s",
                self.id, expected_qty, refreshed_qty, step,
            )
            return False
        self.store.set_protection_block(self.id, None)
        bot.logger.warning(
            "PYRAMID STOP QTY V77 | REFRESH CONFIRMED | %s | protected_qty=%s target=%s",
            self.id,
            refreshed_qty,
            (self.st().get("native_risk_stop") or {}).get("target_price"),
        )

    return True


bot.PyramidEngine._ensure_native_risk_stop = _ensure_native_risk_stop_v77
bot.VERSION = f"{bot.VERSION}-stop-qty-coverage-v77"


def main() -> None:
    bot.logger.warning(
        "PYRAMID STOP QTY COVERAGE FIX ACTIVE | version=v77 | policy=STATE_QTY=PHYSICAL_QTY=>NATIVE_STOP_QTY_EXACT | fail_closed=True | accounting=PRESERVED"
    )
    base.main()


if __name__ == "__main__":
    main()
