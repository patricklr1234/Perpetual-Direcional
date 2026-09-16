#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Production entrypoint for Perpetual Direcional.

Keeps the PYRAMID risk-policy override, fixes watchdog observability, repairs
stale SCALPER exchange-flat release, and normalizes the PYRAMID cycle after a
non-terminal protective/directional exit:
- Native PYRAMID STOP_MARKET starts 1% adverse to weighted entry.
- After +1% favorable movement, existing main.py logic arms breakeven.
- A normal 1% native/protective exit does NOT permanently stop the Pyramid.
- A real PYRAMID max-loss remains terminal for that strategy.
- Once PYRAMID ownership is provably flat, LONG and SHORT anchors for the symbol
  are cleared together; the next tick anchors both directions at the current mark.
- Realized PnL/equity/accounting are preserved during a normal cycle rearm.
- OPEN_ORDERS_AUDIT uses ExchangeSnapshot.captured_ms (the actual dataclass field)
  instead of the obsolete timestamp_ms name.
- A SCALPER stopped only by EXCHANGE_FLAT_RECOVERY_UNACCOUNTED is auto-released
  after proving both Hedge-Mode sides are physically flat, have no open orders,
  durable ledger ownership is zero, and persistent state has no position/native stop.

State, ledger, bankrolls, positions, native orders, sizing, recovery parameters,
BOT_DIR and Volume semantics are preserved. This repair never fabricates a fill,
forgives a realized loss, or re-arms a PYRAMID while its ownership is unresolved.
"""

import main as bot


def _pyramid_one_percent_native_stop_price(self):
    entry = self._weighted_entry_price()
    if entry <= 0:
        return None

    pct = bot.PYRAMID_ADVERSE_REVERSAL_PCT
    if pct <= 0 or pct >= bot.D(1):
        raise RuntimeError(f"PYRAMID_ADVERSE_REVERSAL_PCT invalido para native stop: {pct}")

    if self.side == "LONG":
        raw = entry * (bot.D(1) - pct)
        direction = "DOWN"
    else:
        raw = entry * (bot.D(1) + pct)
        direction = "UP"

    if raw <= 0:
        return None
    return self.exe.rules.trigger_price(self.symbol, raw, direction)


def _fixed_audit_extract(reconciler, ledger):
    result = {"positions": [], "orders": [], "coverage": {}, "timestamp_ms": 0}
    snap = reconciler.last_snapshot
    if not snap:
        return result

    result["timestamp_ms"] = int(getattr(snap, "captured_ms", 0) or 0)

    for (sym, side), qty in snap.positions.items():
        qty = bot.dec(qty)
        if qty > 0:
            result["positions"].append({
                "symbol": str(sym).upper(),
                "positionSide": str(side).upper(),
                "qty": str(qty),
                "entryPrice": str(bot.dec(snap.entry_prices.get((sym, side), bot.D(0)))),
            })

    for o in snap.open_orders or []:
        status = str(o.get("status", "")).upper()
        if status not in ("NEW", "PARTIALLY_FILLED"):
            continue
        cid = str(o.get("clientOrderId") or o.get("origClientOrderId") or "")
        owner = ledger.order_owner(cid) if cid else None
        sym = str(o.get("symbol", "")).upper()
        ps = str(o.get("positionSide", "")).upper()
        otype = str(o.get("type", "")).upper()
        orig_qty = bot.dec(o.get("origQty"))
        exec_qty = bot.dec(o.get("executedQty"))
        remain = max(bot.D(0), orig_qty - exec_qty)

        rec = {
            "orderId": str(o.get("orderId", "")),
            "clientOrderId": cid,
            "owner": owner or "UNOWNED",
            "symbol": sym,
            "positionSide": ps,
            "side": str(o.get("side", "")).upper(),
            "type": otype,
            "status": status,
            "origQty": str(orig_qty),
            "executedQty": str(exec_qty),
            "remainingQty": str(remain),
            "stopPrice": str(o.get("stopPrice", "")),
            "price": str(o.get("price", "")),
            "reduceOnly": bool(o.get("reduceOnly", False)),
            "workingType": str(o.get("workingType", "")),
            "priceProtect": bool(o.get("priceProtect", False)),
        }
        result["orders"].append(rec)

        if remain > 0 and otype in ("STOP_MARKET", "TAKE_PROFIT_MARKET") and sym and ps:
            key = f"{sym}:{ps}"
            result["coverage"].setdefault(key, {})
            result["coverage"][key][otype] = str(
                bot.dec(result["coverage"][key].get(otype, "0")) + remain
            )

    result["positions"].sort(key=lambda x: (x["symbol"], x["positionSide"]))
    result["orders"].sort(key=lambda x: (x["symbol"], x["positionSide"], x["type"], x["clientOrderId"]))
    return result


def _pyramid_max_loss_limit(st) -> bot.Decimal:
    share = bot.dec((st or {}).get("capital_share", "1"), "1")
    return bot.PYRAMID_MAX_LOSS_USD * share


def _pyramid_real_max_loss(st, source_reason: str = "", net_before=None, realized_close=None) -> bool:
    """Return True only for an explicit/actually-reached PYRAMID max-loss.

    Legacy v65 protective stops were persisted as NATIVE_RISK_STOP_FILLED even
    when the close was only ~1% adverse. Those must not become permanent stops.
    """
    st = st or {}
    reason = str(source_reason or st.get("stop_reason") or "").upper()
    if "PYRAMID_MAX_LOSS" in reason or "MAX_LOSS_REAL" in reason:
        return True

    limit = _pyramid_max_loss_limit(st)
    if limit <= 0:
        return False

    values = []
    if net_before is not None:
        values.append(bot.dec(net_before))
    if realized_close is not None:
        values.append(bot.dec(realized_close))
    # For a persisted STOPPED state produced by the old _stop_state_after_close,
    # last_net_pnl is the realized close result for that cycle.
    values.append(bot.dec(st.get("last_net_pnl")))
    return any(v <= -limit for v in values)


def _pyramid_reason_allows_cycle_rearm(reason: str) -> bool:
    reason = str(reason or "").upper()
    return any(token in reason for token in (
        "PYRAMID_REARM_PENDING",
        "NATIVE_RISK_STOP_FILLED",
        "NATIVE_PYRAMID_RISK_STOP",
        "PYRAMID_LIQUIDATION_GUARD",
        "PYRAMID_ADVERSE_1PCT_REVERSAL_EXIT",
        "PYRAMID_RETURN_TO_ENTRY_AFTER_1PCT",
    ))


def _pyramid_symbol_provably_flat(engine) -> bool:
    """Prove that PYRAMID owns no exposure on either side of this symbol.

    Physical Hedge-Mode rows may still contain SCALPER exposure. Therefore the
    proof requires physical == durable ledger for each side and zero PYRAMID lots,
    instead of incorrectly requiring the whole symbol to be physically zero.
    """
    symbol = str(engine.symbol).upper()

    with engine.store.lock:
        for side in ("LONG", "SHORT"):
            st = engine.store.state.get("pyramid", {}).get(f"{symbol}:{side}")
            if isinstance(st, dict) and (st.get("legs") or st.get("native_risk_stop")):
                return False
        # A legacy G0 still carrying exposure is drain-only and blocks a fresh cycle.
        for st in engine.store.state.get("pyramid_grids", {}).values():
            if not isinstance(st, dict):
                continue
            if str(st.get("symbol") or "").upper() == symbol and (
                st.get("legs") or st.get("native_risk_stop")
            ):
                return False

    if not bot.LIVE_TRADING:
        return True

    try:
        pyramid_lots = engine.exe.ledger.open_lots_by_strategy_prefix(f"PYRAMID:{symbol}:")
        if any(bot.dec(x.get("qty")) > 0 for x in pyramid_lots):
            return False

        rows = engine.client.positions(symbol)
        if not isinstance(rows, list):
            return False
        physical = {"LONG": bot.D(0), "SHORT": bot.D(0)}
        for row in rows:
            if str(row.get("symbol") or "").upper() != symbol:
                continue
            side = str(row.get("positionSide") or "").upper()
            if side in physical:
                physical[side] = abs(bot.dec(row.get("positionAmt")))

        ledger_all = engine.exe.ledger.open_by_symbol_side()
        step = engine.exe.rules.rules[symbol].step_size
        for side in ("LONG", "SHORT"):
            expected = bot.dec(ledger_all.get((symbol, side), bot.D(0)))
            if abs(expected - physical[side]) >= step:
                return False

        orders = engine.client.open_orders(symbol)
        if not isinstance(orders, list):
            return False
        for order in orders:
            status = str(order.get("status") or "NEW").upper()
            if status not in ("NEW", "PARTIALLY_FILLED"):
                continue
            cid = str(order.get("clientOrderId") or order.get("origClientOrderId") or "")
            owner = engine.exe.ledger.order_owner(cid) if cid else None
            if owner and str(owner).startswith(f"PYRAMID:{symbol}:"):
                return False

        return True
    except Exception as exc:
        bot.logger.warning(
            "PYRAMID FLAT PROOF FAIL | symbol=%s | %s",
            symbol,
            exc,
        )
        return False


def _reset_pyramid_symbol_cycle_if_flat(engine, source_reason: str) -> bool:
    """Clear LONG/SHORT anchors together after PYRAMID ownership is proven flat.

    `anchor=None` is intentional: the normal tick path then captures the current
    live mark as the new reference for both +1% LONG and -1% SHORT triggers.
    """
    if not _pyramid_symbol_provably_flat(engine):
        return False

    symbol = str(engine.symbol).upper()
    changed = []
    with engine.store.lock:
        for side in ("LONG", "SHORT"):
            key = f"{symbol}:{side}"
            st = engine.store.state.get("pyramid", {}).get(key)
            if not isinstance(st, dict):
                continue
            if st.get("legs") or st.get("native_risk_stop"):
                return False

            previous_reason = str(st.get("stop_reason") or "")
            terminal = bool(st.get("stopped")) and _pyramid_real_max_loss(st, previous_reason)
            if bool(st.get("stopped")) and previous_reason and not terminal:
                # Only known normal/protective cycle stops are auto-released. Unknown
                # operational stops stay fail-closed for review.
                if _pyramid_reason_allows_cycle_rearm(previous_reason):
                    st["stopped"] = False
                    st["stop_reason"] = None
                else:
                    terminal = True

            st["anchor"] = None
            st["next_level"] = 1
            st["levels_filled"] = 0
            st["last_trigger_price"] = None
            st["native_risk_stop"] = None
            st["return_exit_armed"] = False
            st["return_exit_reference"] = None
            st["last_update"] = bot.now_iso()
            changed.append({
                "strategy": str(st.get("strategy") or f"PYRAMID:{symbol}:{side}"),
                "stopped": bool(st.get("stopped")),
                "terminal": terminal,
                "equity": str(st.get("equity", "")),
                "realized_pnl": str(st.get("realized_pnl", "")),
            })
        engine.store.save()

    bot.logger.warning(
        "PYRAMID CYCLE REARM | symbol=%s | source=%s | anchors=RESET_TO_NEXT_CURRENT_MARK | "
        "both_directions=True | accounting=PRESERVED | states=%s",
        symbol,
        source_reason,
        changed,
    )
    return True


_original_pyramid_directional_close = bot.PyramidEngine._directional_close


def _directional_close_reanchor_current_market(self, price, reason):
    closed = _original_pyramid_directional_close(self, price, reason)
    if not closed:
        return False

    # The original method writes anchor=exit price and clears stopped. Freeze this
    # cycle until we independently prove that no PYRAMID ownership remains, then
    # clear BOTH directional anchors so next tick uses the current market level.
    st = self.st()
    with self.store.lock:
        st["stopped"] = True
        st["stop_reason"] = f"PYRAMID_REARM_PENDING:{reason}"
        st["anchor"] = None
        st["next_level"] = 1
        st["levels_filled"] = 0
        st["last_trigger_price"] = None
        st["return_exit_armed"] = False
        st["return_exit_reference"] = None
        st["last_update"] = bot.now_iso()
        self.store.save()

    _reset_pyramid_symbol_cycle_if_flat(self, reason)
    return True


def _stop_state_after_close_rearm(self, total, net_before_close, reason, remaining):
    """Replace old blanket STOPPED behavior with reason-aware cycle handling."""
    st = self.st()
    total = bot.dec(total)
    net_before_close = bot.dec(net_before_close)
    remaining = list(remaining or [])

    st["legs"] = remaining
    st["realized_pnl"] = str(bot.dec(st.get("realized_pnl")) + total)
    st["equity"] = str(
        bot.PYRAMID_BANKROLL_USD * bot.dec(st.get("capital_share", "1"))
        + bot.dec(st.get("realized_pnl"))
    )
    st["last_unrealized"] = "0"
    st["last_net_pnl"] = str(total)
    st["native_risk_stop"] = None
    st["return_exit_armed"] = False
    st["return_exit_reference"] = None
    st["last_directional_exit_reason"] = reason
    st["last_update"] = bot.now_iso()

    max_loss_real = _pyramid_real_max_loss(
        st,
        source_reason=reason,
        net_before=net_before_close,
        realized_close=total,
    )

    if remaining:
        st["stopped"] = True
        st["stop_reason"] = (
            f"PYRAMID_PARTIAL_EXIT_REMAINS:{reason} "
            f"net_before_close={net_before_close} realized_close={total} "
            f"remaining_legs={len(remaining)}"
        )
        self.store.save()
        return

    if max_loss_real:
        st["stopped"] = bool(bot.PYRAMID_STOP_AFTER_MAX_LOSS)
        st["stop_reason"] = (
            f"PYRAMID_MAX_LOSS_REAL source={reason} "
            f"net_before_close={net_before_close} realized_close={total} "
            f"limit={_pyramid_max_loss_limit(st)}"
        )
        st["anchor"] = None
        st["next_level"] = 1
        st["levels_filled"] = 0
        st["last_trigger_price"] = None
        self.store.save()
        bot.logger.critical(
            "PYRAMID REAL MAX LOSS | %s | source=%s net_before=%s realized=%s limit=%s stopped=%s",
            self.id,
            reason,
            net_before_close,
            total,
            _pyramid_max_loss_limit(st),
            st.get("stopped"),
        )
        return

    if _pyramid_reason_allows_cycle_rearm(reason):
        st["stopped"] = True
        st["stop_reason"] = (
            f"PYRAMID_REARM_PENDING:{reason} "
            f"net_before_close={net_before_close} realized_close={total}"
        )
        st["anchor"] = None
        st["next_level"] = 1
        st["levels_filled"] = 0
        st["last_trigger_price"] = None
        self.store.save()
        _reset_pyramid_symbol_cycle_if_flat(self, reason)
        return

    # Unknown/explicit operational exits remain fail-closed. This includes HARD_KILL
    # and protection-install failures; they are not ordinary stop/rearm events.
    st["stopped"] = True
    st["stop_reason"] = (
        f"{reason} net_before_close={net_before_close} realized_close={total}"
    )
    self.store.save()


_original_pyramid_tick = bot.PyramidEngine.tick


def _pyramid_tick_with_flat_rearm(self, price):
    """Release old/current non-terminal STOPPED states once flat is provable."""
    st = self.st()
    if st.get("stopped"):
        reason = str(st.get("stop_reason") or "")
        if (
            _pyramid_reason_allows_cycle_rearm(reason)
            and not _pyramid_real_max_loss(st, reason)
        ):
            _reset_pyramid_symbol_cycle_if_flat(self, reason)
    return _original_pyramid_tick(self, price)


_original_release_exchange_flat_recovery_blocks = bot.Bot.release_exchange_flat_recovery_blocks


def _release_exchange_flat_recovery_blocks_with_scalper(self) -> None:
    """Run v64 release and additionally release provably-flat stale SCALPER blocks.

    The main.py v64 implementation only enumerates pyramid buckets. This wrapper
    keeps that behavior and adds the missing scalper bucket conservatively.
    """
    _original_release_exchange_flat_recovery_blocks(self)

    if not bot.LIVE_TRADING:
        return

    marker = "EXCHANGE_FLAT_RECOVERY_UNACCOUNTED"
    candidates = []

    with self.store.lock:
        for key, st in self.store.state.get("scalper", {}).items():
            if not isinstance(st, dict):
                continue
            if str(st.get("stop_reason") or "").upper() != marker:
                continue
            strategy_id = str(st.get("strategy") or f"SCALPER:{key}")
            symbol = str(st.get("symbol") or key).upper()
            candidates.append((strategy_id, symbol, st))

    released = []
    skipped = []

    for strategy_id, symbol, st in candidates:
        try:
            if not symbol:
                raise RuntimeError("symbol vazio")

            positions = self.client.positions(symbol)
            if not isinstance(positions, list):
                raise RuntimeError(f"positionRisk indeterminado: {positions!r}")

            physical = {"LONG": bot.D(0), "SHORT": bot.D(0)}
            for row in positions:
                if str(row.get("symbol") or "").upper() != symbol:
                    continue
                side = str(row.get("positionSide") or "").upper()
                if side in physical:
                    physical[side] = abs(bot.dec(row.get("positionAmt")))

            orders = self.client.open_orders(symbol)
            if not isinstance(orders, list):
                raise RuntimeError(f"openOrders indeterminado: {orders!r}")
            hedge_orders = [
                o for o in orders
                if str(o.get("positionSide") or "").upper() in ("LONG", "SHORT")
            ]

            with self.store.lock:
                pos = st.get("position")
                native = st.get("native_risk_stop")

            ledger_long = self.ledger.open_strategy_qty(strategy_id, symbol, "LONG")
            ledger_short = self.ledger.open_strategy_qty(strategy_id, symbol, "SHORT")

            provably_flat = (
                physical["LONG"] <= 0
                and physical["SHORT"] <= 0
                and not hedge_orders
                and not pos
                and not native
                and ledger_long <= 0
                and ledger_short <= 0
            )

            if not provably_flat:
                skipped.append({
                    "strategy": strategy_id,
                    "symbol": symbol,
                    "physical_long": str(physical["LONG"]),
                    "physical_short": str(physical["SHORT"]),
                    "open_orders": len(hedge_orders),
                    "state_position": bool(pos),
                    "native_stop": bool(native),
                    "ledger_long": str(ledger_long),
                    "ledger_short": str(ledger_short),
                })
                continue

            with self.store.lock:
                # Preserve bankroll, equity, realized PnL, recovery deficit and all
                # lifetime counters. Only remove the stale operational stop marker.
                st["stopped"] = False
                st["stop_reason"] = None
                st["last_update"] = bot.now_iso()
                self.store.save()

                op_reason = str(
                    (self.store.state.get("operational_blocks", {}) or {}).get(strategy_id) or ""
                )
                pr_reason = str(
                    (self.store.state.get("protection_blocks", {}) or {}).get(strategy_id) or ""
                )

            if marker in op_reason.upper():
                self.store.set_operational_block(strategy_id, None)
            if marker in pr_reason.upper():
                self.store.set_protection_block(strategy_id, None)

            released.append({
                "strategy": strategy_id,
                "symbol": symbol,
                "bankroll_preserved": str(st.get("bankroll", "")),
                "equity_preserved": str(st.get("equity", "")),
                "realized_pnl_preserved": str(st.get("realized_pnl", "")),
                "recovery_deficit_preserved": str(st.get("recovery_deficit", "")),
            })
            bot.logger.warning(
                "SCALPER EXCHANGE FLAT RECOVERY RELEASE | strategy=%s | symbol=%s | "
                "flat=CONFIRMED_BOTH_SIDES | accounting=PRESERVED",
                strategy_id,
                symbol,
            )

        except Exception as exc:
            skipped.append({
                "strategy": strategy_id,
                "symbol": symbol,
                "reason": f"VERIFY_FAILED:{exc}",
            })
            bot.logger.exception(
                "SCALPER EXCHANGE FLAT RECOVERY RELEASE | SKIP FAIL-CLOSED | %s",
                strategy_id,
            )

    with self.store.lock:
        maintenance = self.store.state.setdefault("maintenance", {})
        maintenance["scalper_exchange_flat_auto_release_v65"] = {
            "at": bot.now_iso(),
            "released": released,
            "skipped": skipped,
            "policy": (
                "BOTH_HEDGE_SIDES_PHYSICALLY_FLAT; NO_OPEN_ORDERS; "
                "NO_STATE_POSITION; NO_NATIVE_STOP; NO_LEDGER_OWNERSHIP; "
                "ACCOUNTING_PRESERVED"
            ),
        }
        self.store.save()

    bot.logger.warning(
        "SCALPER EXCHANGE FLAT RECOVERY RELEASE COMPLETE | released=%s skipped=%s",
        len(released),
        len(skipped),
    )


bot.PyramidEngine._max_loss_stop_price = _pyramid_one_percent_native_stop_price
bot.PyramidEngine._directional_close = _directional_close_reanchor_current_market
bot.PyramidEngine._stop_state_after_close = _stop_state_after_close_rearm
bot.PyramidEngine.tick = _pyramid_tick_with_flat_rearm
bot.audit_extract_open_orders_and_positions = _fixed_audit_extract
bot.Bot.release_exchange_flat_recovery_blocks = _release_exchange_flat_recovery_blocks_with_scalper
bot.VERSION = f"{bot.VERSION}-native-1pct-be-auditfix-scalper-flat-release-pyramid-rearm-v66"


def main() -> None:
    bot.logger.warning(
        "PYRAMID RISK POLICY ACTIVE | native_adverse_stop=%s%% | breakeven_after_favorable=%s%% | max_loss_usd=%s software_terminal_ceiling",
        bot.PYRAMID_ADVERSE_REVERSAL_PCT * bot.D(100),
        bot.PYRAMID_RETURN_EXIT_ARM_PCT * bot.D(100),
        bot.PYRAMID_MAX_LOSS_USD,
    )
    bot.logger.warning(
        "PYRAMID REARM FIX ACTIVE | version=v66 | normal_protective_stop=REARM_WHEN_FLAT | "
        "anchors=RESET_BOTH_SIDES_TO_NEXT_CURRENT_MARK | accounting=PRESERVED | real_max_loss=TERMINAL"
    )
    bot.logger.warning("OPEN_ORDERS_AUDIT FIX ACTIVE | snapshot_field=captured_ms")
    bot.logger.warning("SCALPER FLAT RELEASE FIX ACTIVE | version=v65 | accounting=PRESERVED")
    bot.main()


if __name__ == "__main__":
    main()
