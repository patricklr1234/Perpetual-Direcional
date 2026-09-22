#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Perpetual Direcional production entrypoint v82.

Completes v81 by enforcing the full PYRAMID stop hierarchy independently of the
legacy v73 continuity bridge:
1) initial native stop = 1% adverse from physical average entry;
2) once +1% favorable/return_exit_armed, native stop >= breakeven (LONG) or
   <= breakeven (SHORT);
3) inherited anchor profit-lock/trailing may tighten further;
4) an already-live stricter stop is never loosened.

No market close, position quantity, ledger/state accounting, bankroll, leverage,
anchor, sizing, or entry rule is changed by this layer.
"""

import runner_v81 as base

bot = base.bot

_inherited_effective_v81 = bot.PyramidEngine._effective_native_stop_price
_original_tick_v81 = bot.PyramidEngine.tick
_v82_refreshed = set()


def _entry_v82(engine):
    return base._physical_or_weighted_entry_v81(engine)


def _effective_native_stop_v82(self, mark):
    mark = bot.dec(mark)
    inherited, meta = _inherited_effective_v81(self, mark)
    entry = _entry_v82(self)
    if entry <= 0 or mark <= 0:
        return inherited, meta

    if self.side == "LONG":
        initial = self.exe.rules.trigger_price(
            self.symbol, entry * (bot.D(1) - bot.PYRAMID_ADVERSE_REVERSAL_PCT), "DOWN"
        )
    else:
        initial = self.exe.rules.trigger_price(
            self.symbol, entry * (bot.D(1) + bot.PYRAMID_ADVERSE_REVERSAL_PCT), "UP"
        )

    candidates = [initial]
    if inherited is not None and bot.dec(inherited) > 0:
        candidates.append(bot.dec(inherited))

    breakeven = None
    if bot.PYRAMID_BREAKEVEN_STOP_ENABLED and bool(self.st().get("return_exit_armed")):
        direction = "DOWN" if self.side == "LONG" else "UP"
        breakeven = self.exe.rules.trigger_price(self.symbol, entry, direction)
        candidates.append(breakeven)

    old = bot.dec((self.st().get("native_risk_stop") or {}).get("target_price"))
    if old > 0:
        candidates.append(old)

    chosen = max(candidates) if self.side == "LONG" else min(candidates)
    valid = chosen < mark if self.side == "LONG" else chosen > mark

    out = dict(meta or {})
    out.update({
        "initial_adverse_stop_v82": initial,
        "breakeven_stop_v82": breakeven,
        "physical_entry_v82": entry,
        "chosen_target_v82": chosen,
        "policy_v82": "INITIAL_1PCT_THEN_BREAKEVEN_OR_STRICTER_NEVER_LOOSEN",
    })
    return (chosen if valid else None), out


bot.PyramidEngine._effective_native_stop_price = _effective_native_stop_v82


def _tick_v82(self, price):
    # If an inherited state is already >1% favorable but the persisted arm bit
    # predates this policy, arm it before refreshing the native stop.
    st = self.st()
    legs = st.get("legs") or []
    if legs and not bool(st.get("return_exit_armed")):
        entry = _entry_v82(self)
        mark = bot.dec(price)
        if entry > 0 and mark > 0:
            favorable, _ = self._directional_move_pct(mark, entry)
            if favorable >= bot.PYRAMID_RETURN_EXIT_ARM_PCT:
                st["return_exit_armed"] = True
                st["return_exit_reference"] = str(entry)
                st["last_update"] = bot.now_iso()
                self.store.save()
                bot.logger.warning(
                    "PYRAMID BREAKEVEN V82 | INHERITED ARM CONFIRMED | %s | entry=%s mark=%s favorable=%s%%",
                    self.id, entry, mark, favorable * bot.D(100),
                )

    key = str(self.id)
    if key not in _v82_refreshed and legs:
        ok = self._ensure_native_risk_stop(bot.dec(price), force=True)
        if ok:
            _v82_refreshed.add(key)
            native = self.st().get("native_risk_stop") or {}
            entry = _entry_v82(self)
            target = bot.dec(native.get("target_price"))
            if self.side == "LONG":
                pct = ((target / entry) - bot.D(1)) * bot.D(100) if entry > 0 else bot.D(0)
            else:
                pct = ((entry / target) - bot.D(1)) * bot.D(100) if entry > 0 and target > 0 else bot.D(0)
            bot.logger.warning(
                "PYRAMID STOP POLICY V82 | REFRESH CONFIRMED | %s | entry=%s target=%s distance_from_entry=%s%% armed=%s qty=%s",
                self.id, entry, target, pct, bool(self.st().get("return_exit_armed")),
                sum((bot.dec(x.get("qty")) for x in (self.st().get("legs") or [])), bot.D(0)),
            )
        else:
            bot.logger.error(
                "PYRAMID STOP POLICY V82 | REFRESH NOT CONFIRMED | %s | fail_closed=True",
                self.id,
            )
    return _original_tick_v81(self, price)


bot.PyramidEngine.tick = _tick_v82
bot.VERSION = f"{bot.VERSION}-breakeven-hierarchy-v82"


def main():
    bot.logger.warning(
        "PYRAMID STOP HIERARCHY V82 ACTIVE | initial=-1%%_adverse | favorable_+1%%=>BREAKEVEN | "
        "profit_lock=UNCHANGED | never_loosen=True | max_loss_usd=ADDITIONAL_TERMINAL_FAILSAFE | "
        "positions=UNTOUCHED accounting=PRESERVED"
    )
    base.main()


if __name__ == "__main__":
    main()
