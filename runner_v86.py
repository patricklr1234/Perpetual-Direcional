#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Perpetual Direcional v86.

Safe release of stale PYRAMID_DIRECTIONAL_PARTIAL_EXIT_REMAINS blocks.
The release is fail-closed: it occurs only after reconciliation proves that
there is no durable PYRAMID ownership, no state legs/native stop, no physical
PYRAMID exposure and no open PYRAMID-owned orders. It never closes/cancels
orders, changes risk parameters, or alters the fill ledger.
"""

import runner_v85 as base

bot = base.bot
_original_release = bot.Bot.release_stale_protection_blocks_if_flat


def _release_stale_directional_partial_exit_v86(self):
    try:
        with self.store.lock:
            blocks = dict(self.store.state.get("operational_blocks", {}) or {})

        for strategy_id, reason in blocks.items():
            reason_text = str(reason or "").upper()
            if "PYRAMID_DIRECTIONAL_PARTIAL_EXIT_REMAINS:" not in reason_text:
                continue
            if not str(strategy_id).startswith("PYRAMID:"):
                continue

            parts = str(strategy_id).split(":")
            if len(parts) != 3:
                continue
            _, symbol, side = parts
            symbol = symbol.upper()
            side = side.upper()
            if side not in ("LONG", "SHORT"):
                continue

            with self.store.lock:
                st = (self.store.state.get("pyramid", {}) or {}).get(f"{symbol}:{side}")
                st = dict(st) if isinstance(st, dict) else {}
            if st.get("legs") or st.get("native_risk_stop"):
                continue

            try:
                lots = self.ledger.open_lots_by_strategy_prefix(f"PYRAMID:{symbol}:{side}")
                if any(bot.dec(x.get("qty")) > 0 for x in (lots or [])):
                    continue
            except Exception as exc:
                bot.logger.warning("V86 PARTIAL RELEASE | ledger proof failed | %s | %s", strategy_id, exc)
                continue

            snap = self.reconciler.last_snapshot
            if snap is None:
                continue
            physical = bot.dec((snap.positions or {}).get((symbol, side), bot.D(0)))
            if physical > 0:
                continue

            owned_open = False
            for order in (snap.open_orders or []):
                status = str(order.get("status") or "NEW").upper()
                if status not in ("NEW", "PARTIALLY_FILLED"):
                    continue
                if str(order.get("symbol") or "").upper() != symbol:
                    continue
                if str(order.get("positionSide") or "").upper() != side:
                    continue
                cid = str(order.get("clientOrderId") or order.get("origClientOrderId") or "")
                owner = self.ledger.order_owner(cid) if cid else None
                if owner and str(owner).startswith(f"PYRAMID:{symbol}:{side}"):
                    owned_open = True
                    break
            if owned_open:
                continue

            self.store.set_operational_block(strategy_id, None)
            bot.logger.warning(
                "STALE PYRAMID PARTIAL-EXIT AUTO-RELEASE V86 | %s | "
                "proof=state_flat,ledger_flat,physical_flat,orders_flat | accounting=PRESERVED",
                strategy_id,
            )
    except Exception as exc:
        bot.logger.exception("STALE PYRAMID PARTIAL-EXIT AUTO-RELEASE V86 | fail_closed=True | %s", exc)


def _release_all_stale_v86(self):
    _original_release(self)
    _release_stale_directional_partial_exit_v86(self)


bot.Bot.release_stale_protection_blocks_if_flat = _release_all_stale_v86
bot.VERSION = f"{bot.VERSION}-directional-partial-release-v86"


def main():
    bot.logger.warning(
        "DIRECTIONAL PARTIAL-EXIT AUTO-RELEASE V86 ACTIVE | "
        "proof=state+ledger+physical+orders | fail_closed=True"
    )
    base.main()


if __name__ == "__main__":
    main()
