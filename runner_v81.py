#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Perpetual Direcional production entrypoint v81.

Restores the user's PYRAMID risk contract:
- initial native STOP_MARKET is 1% adverse from the current physical average entry;
- after +1% favorable, breakeven remains eligible and is stricter than the initial stop;
- anchor profit-lock/trailing from the inherited chain remains eligible;
- an already-live stricter stop is never loosened;
- max-loss/liquidation guards remain additional fail-safe candidates, never substitutes
  for the 1% initial price stop.

This supersedes the v73 pre-migration continuity exception that could preserve legacy
stops roughly 8-10% below reconstructed entries. It does not close positions, change
position quantities, ledger/state accounting, bankrolls, leverage, sizing, anchors,
or strategy parameters.
"""

import runner_v80 as base

bot = base.bot

_original_effective_stop_v80 = bot.PyramidEngine._effective_native_stop_price
_original_tick_v80 = bot.PyramidEngine.tick
_v81_refreshed = set()


def _physical_or_weighted_entry_v81(engine):
    try:
        p = engine._physical_position_snapshot() or {}
        for key in ("entryPrice", "entry_price"):
            v = bot.dec(p.get(key))
            if v > 0:
                return v
    except Exception:
        pass
    return engine._weighted_entry_price()


def _effective_native_stop_v81(self, mark):
    mark = bot.dec(mark)
    inherited, meta = _original_effective_stop_v80(self, mark)
    entry = _physical_or_weighted_entry_v81(self)
    if entry <= 0 or mark <= 0:
        return inherited, meta

    adverse_pct = bot.PYRAMID_ADVERSE_REVERSAL_PCT
    if self.side == "LONG":
        raw = entry * (bot.D(1) - adverse_pct)
        initial_1pct = self.exe.rules.trigger_price(self.symbol, raw, "DOWN")
    else:
        raw = entry * (bot.D(1) + adverse_pct)
        initial_1pct = self.exe.rules.trigger_price(self.symbol, raw, "UP")

    candidates = [initial_1pct]
    if inherited is not None and bot.dec(inherited) > 0:
        candidates.append(bot.dec(inherited))

    # Preserve an already-live stop only when it is stricter. This makes the
    # protection monotonic and prevents v81 from loosening breakeven/profit-lock.
    existing = self.st().get("native_risk_stop") or {}
    old = bot.dec(existing.get("target_price"))
    if old > 0:
        candidates.append(old)

    chosen = max(candidates) if self.side == "LONG" else min(candidates)
    valid = chosen < mark if self.side == "LONG" else chosen > mark

    out = dict(meta or {})
    out.update({
        "initial_adverse_stop_v81": initial_1pct,
        "initial_adverse_pct_v81": adverse_pct,
        "physical_entry_v81": entry,
        "inherited_target_v81": inherited,
        "existing_target_v81": old,
        "chosen_target_v81": chosen,
        "policy_v81": "INITIAL_1PCT_OR_STRICTER_NEVER_LOOSEN",
    })
    return (chosen if valid else None), out


bot.PyramidEngine._effective_native_stop_price = _effective_native_stop_v81


def _tick_v81(self, price):
    # Force one refresh for every currently open PYRAMID side after deployment.
    # Existing _ensure_native_risk_stop performs verify -> cancel -> reinstall,
    # and v77/v78 verify exact quantity coverage. No market close is requested.
    key = str(self.id)
    if key not in _v81_refreshed and (self.st().get("legs") or []):
        ok = self._ensure_native_risk_stop(bot.dec(price), force=True)
        if ok:
            _v81_refreshed.add(key)
            native = self.st().get("native_risk_stop") or {}
            entry = _physical_or_weighted_entry_v81(self)
            target = bot.dec(native.get("target_price"))
            if self.side == "LONG":
                distance = ((target / entry) - bot.D(1)) if entry > 0 else bot.D(0)
            else:
                distance = ((entry / target) - bot.D(1)) if entry > 0 and target > 0 else bot.D(0)
            bot.logger.warning(
                "PYRAMID INITIAL 1PCT V81 | REFRESH CONFIRMED | %s | entry=%s target=%s distance_from_entry=%s%% | qty=%s",
                self.id, entry, target, distance * bot.D(100),
                sum((bot.dec(x.get("qty")) for x in (self.st().get("legs") or [])), bot.D(0)),
            )
        else:
            bot.logger.error(
                "PYRAMID INITIAL 1PCT V81 | REFRESH NOT CONFIRMED | %s | fail_closed=True",
                self.id,
            )
    return _original_tick_v80(self, price)


bot.PyramidEngine.tick = _tick_v81
bot.VERSION = f"{bot.VERSION}-initial-native-1pct-v81"


def main():
    bot.logger.warning(
        "PYRAMID INITIAL STOP POLICY V81 ACTIVE | initial_adverse=1%%_PHYSICAL_AVG_ENTRY | "
        "after_+1%%=BREAKEVEN_OR_STRICTER | profit_lock=UNCHANGED | never_loosen=True | "
        "max_loss_usd=ADDITIONAL_FAILSAFE_NOT_INITIAL_STOP | positions=UNTOUCHED | accounting=PRESERVED"
    )
    base.main()


if __name__ == "__main__":
    main()
