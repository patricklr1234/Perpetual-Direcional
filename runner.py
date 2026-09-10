#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Production entrypoint for Perpetual Direcional risk-policy override.

Risk policy for PYRAMID exposures:
- Native STOP_MARKET starts exactly 1% adverse to the current weighted entry.
- After +1% favorable movement, existing main.py logic arms breakeven and
  _effective_native_stop_price() moves the native stop to weighted entry.
- PYRAMID_MAX_LOSS_USD remains an additional software loss ceiling; it no longer
  determines the initial native stop distance.

This module intentionally imports the existing production bot and changes only
PyramidEngine._max_loss_stop_price, preserving state, ledger, bankrolls, orders,
positions, strategy sizing, recovery parameters, BOT_DIR and Volume semantics.
"""

import main as bot


def _pyramid_one_percent_native_stop_price(self):
    """Return the exchange-native adverse stop from weighted entry.

    LONG  -> weighted_entry * (1 - PYRAMID_ADVERSE_REVERSAL_PCT)
    SHORT -> weighted_entry * (1 + PYRAMID_ADVERSE_REVERSAL_PCT)

    Exchange trigger rounding is delegated to the same SymbolRules helper used
    by the production engine. The existing effective-stop selector can still
    choose an earlier liquidation guard or, once armed, the breakeven stop.
    """
    entry = self._weighted_entry_price()
    if entry <= 0:
        return None

    pct = bot.PYRAMID_ADVERSE_REVERSAL_PCT
    if pct <= 0 or pct >= bot.D(1):
        raise RuntimeError(
            f"PYRAMID_ADVERSE_REVERSAL_PCT invalido para native stop: {pct}"
        )

    if self.side == "LONG":
        raw = entry * (bot.D(1) - pct)
        direction = "DOWN"
    else:
        raw = entry * (bot.D(1) + pct)
        direction = "UP"

    if raw <= 0:
        return None
    return self.exe.rules.trigger_price(self.symbol, raw, direction)


# Replace only the initial PYRAMID native-risk stop price policy. All other
# protection/watchdog/fail-closed code remains the implementation in main.py.
bot.PyramidEngine._max_loss_stop_price = _pyramid_one_percent_native_stop_price
bot.VERSION = f"{bot.VERSION}-native-1pct-be"


def main() -> None:
    bot.logger.warning(
        "PYRAMID RISK POLICY ACTIVE | native_adverse_stop=%s%% | "
        "breakeven_after_favorable=%s%% | max_loss_usd=%s additional_software_ceiling",
        bot.PYRAMID_ADVERSE_REVERSAL_PCT * bot.D(100),
        bot.PYRAMID_RETURN_EXIT_ARM_PCT * bot.D(100),
        bot.PYRAMID_MAX_LOSS_USD,
    )
    bot.main()


if __name__ == "__main__":
    main()
