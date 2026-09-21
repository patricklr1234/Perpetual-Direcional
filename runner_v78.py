#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Perpetual Direcional production entrypoint v78.

Completes the v77 native-stop quantity coverage repair.

The inherited `_ensure_native_risk_stop(..., force=True)` bypasses its refresh
interval, but the older implementation can still return early when the existing
STOP_MARKET is LIVE and its target price is unchanged.  That shortcut does not
compare protected quantity, so v77 could detect a quantity mismatch yet the
underlying routine would keep the stale smaller order.

For the exact v77-proven case (state quantity == physical quantity and persisted
protective quantity differs by at least one exchange step), this layer
transiently marks the persisted target as stale in memory before invoking v77.
That makes the existing, battle-tested cancel-and-reinstall path run and install
the same effective target price for the full current basket quantity.  If the
replacement path fails before replacing the persisted object, the original
target metadata is restored and the protection block remains fail-closed.

No entry, market close, accounting reset, strategy change, or risk-parameter
change is performed by this layer.
"""

import runner_v77 as base

bot = base.bot

_original_ensure_native_risk_stop_v77 = bot.PyramidEngine._ensure_native_risk_stop


def _ensure_native_risk_stop_v78(self, price, force=False):
    st = self.st()
    legs = list(st.get("legs") or [])
    expected_qty = sum(
        (bot.dec(leg.get("qty")) for leg in legs if isinstance(leg, dict)),
        bot.D(0),
    )
    native = st.get("native_risk_stop")
    protected_qty = base._native_stop_qty_v77(native)
    step = self.exe.rules.rules[self.symbol].step_size

    qty_mismatch = (
        expected_qty > 0
        and isinstance(native, dict)
        and abs(expected_qty - protected_qty) >= step
    )

    old_target = None
    target_was_overridden = False
    if qty_mismatch:
        # v77 performs the authoritative state==physical proof.  This temporary
        # metadata change only defeats the legacy same-target early return so
        # that its normal cancel-and-reinstall path can execute.
        old_target = native.get("target_price")
        native["target_price"] = "0"
        target_was_overridden = True
        bot.logger.warning(
            "PYRAMID STOP QTY V78 | SAME-TARGET SHORTCUT BYPASS | %s | expected_qty=%s protected_qty=%s original_target=%s",
            self.id, expected_qty, protected_qty, old_target,
        )

    try:
        return _original_ensure_native_risk_stop_v77(self, bot.dec(price), force=force or qty_mismatch)
    finally:
        if target_was_overridden:
            # Successful replacement swaps st['native_risk_stop'] to a new dict.
            # Restore only if the old object is still the persisted one.
            current = self.st().get("native_risk_stop")
            if current is native:
                native["target_price"] = old_target
                try:
                    self.store.save()
                except Exception:
                    bot.logger.exception(
                        "PYRAMID STOP QTY V78 | FAILED TO RESTORE OLD TARGET METADATA | %s",
                        self.id,
                    )


bot.PyramidEngine._ensure_native_risk_stop = _ensure_native_risk_stop_v78
bot.VERSION = f"{bot.VERSION}-same-target-qty-refresh-v78"


def main() -> None:
    bot.logger.warning(
        "PYRAMID STOP QTY SAME-TARGET BYPASS FIX ACTIVE | version=v78 | replacement=SAME_EFFECTIVE_TARGET+FULL_CURRENT_QTY | fail_closed=True | accounting=PRESERVED"
    )
    base.main()


if __name__ == "__main__":
    main()
