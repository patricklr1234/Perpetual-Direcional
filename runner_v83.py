#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Perpetual Direcional production entrypoint v83.

Fixes stale SOFT API_ERROR_STREAK kill state.

Root cause: AsterClient resets api_error_streak to zero after a successful signed
request, but Bot.run persisted SOFT:API_ERROR_STREAK and never released it. The
persistent kill could therefore survive indefinitely while authenticated API,
reconciliation and native protection were healthy.

v83 releases ONLY a SOFT kill whose reason starts API_ERROR_STREAK, and ONLY after:
- current authenticated api_error_streak == 0;
- a fresh successful reconciliation proves ledger == state == physical;
- no operational/protection blocks exist;
- every open physical position has STOP_MARKET coverage >= physical quantity.

No position/order is closed or cancelled. State/ledger/bankroll/risk parameters
are preserved. Any failed proof remains fail-closed.
"""

import runner_v82 as base

bot = base.bot
_original_run_v82 = bot.Bot.run


def _native_stop_coverage_ok_v83(self):
    snap = self.reconciler.last_snapshot
    if snap is None:
        return False, "NO_RECONCILE_SNAPSHOT"
    coverage = {}
    for order in (snap.open_orders or []):
        if str(order.get("status") or "").upper() not in ("NEW", "PARTIALLY_FILLED"):
            continue
        if str(order.get("type") or "").upper() != "STOP_MARKET":
            continue
        sym = str(order.get("symbol") or "").upper()
        side = str(order.get("positionSide") or "").upper()
        if side not in ("LONG", "SHORT"):
            continue
        qty = bot.dec(order.get("origQty")) - bot.dec(order.get("executedQty"))
        if qty > 0:
            coverage[(sym, side)] = coverage.get((sym, side), bot.D(0)) + qty
    for key, physical in snap.positions.items():
        if physical <= 0:
            continue
        step = self.rules.rules[key[0]].step_size
        protected = coverage.get(key, bot.D(0))
        if protected + step < physical:
            return False, f"STOP_COVERAGE:{key}:physical={physical}:protected={protected}:step={step}"
    return True, "FULL_NATIVE_STOP_COVERAGE"


def _release_stale_api_kill_v83(self):
    with self.store.lock:
        ks = dict(self.store.state.get("kill_switch", {}) or {})
        operational = dict(self.store.state.get("operational_blocks", {}) or {})
        protection = dict(self.store.state.get("protection_blocks", {}) or {})
    if str(ks.get("mode")) != "SOFT" or not str(ks.get("reason") or "").startswith("API_ERROR_STREAK="):
        return False
    if self.client.api_error_streak != 0:
        return False
    if operational or protection:
        bot.logger.warning(
            "API SOFT KILL V83 | KEEP FAIL-CLOSED | blocks operational=%s protection=%s",
            operational, protection,
        )
        return False
    try:
        if not self.reconciler.reconcile():
            bot.logger.warning("API SOFT KILL V83 | KEEP FAIL-CLOSED | reconcile=False")
            return False
        ok, proof = _native_stop_coverage_ok_v83(self)
        if not ok:
            bot.logger.warning("API SOFT KILL V83 | KEEP FAIL-CLOSED | %s", proof)
            return False
        # Re-check immediately before mutation.
        with self.store.lock:
            current = dict(self.store.state.get("kill_switch", {}) or {})
        if str(current.get("mode")) != "SOFT" or not str(current.get("reason") or "").startswith("API_ERROR_STREAK="):
            return False
        if self.client.api_error_streak != 0:
            return False
        self.store.kill("OFF", None)
        bot.logger.warning(
            "API SOFT KILL V83 | RELEASE CONFIRMED | authenticated_streak=0 | "
            "reconcile=OK | native_stop_coverage=FULL | blocks=NONE | accounting=PRESERVED"
        )
        return True
    except Exception as exc:
        bot.logger.exception("API SOFT KILL V83 | RELEASE VERIFY FAILED | fail_closed=True | %s", exc)
        return False


def _run_v83(self):
    # startup first, then apply the narrow stale-kill repair before normal loops.
    self.startup()
    if bot.VALIDATE_API_ONLY:
        bot.logger.info("VALIDATE_API_ONLY concluido; encerrando sem alterar configuracoes e sem enviar ordens")
        self.shutdown()
        return
    _release_stale_api_kill_v83(self)

    # Keep the original main-loop semantics, adding automatic recovery after the
    # authenticated API has demonstrably recovered.
    while not self.stop.is_set():
        try:
            if self.client.api_error_streak >= bot.KILL_SWITCH_ON_API_ERRORS and self.store.killed() == "OFF":
                self.store.kill("SOFT", f"API_ERROR_STREAK={self.client.api_error_streak}")
            elif self.client.api_error_streak == 0:
                _release_stale_api_kill_v83(self)

            if self.store.killed() == "HARD":
                if self.hard_kill():
                    self.store.kill("SOFT", "HARD_KILL_CONFIRMED_ZERO_EXPOSURE; manual review required")
                else:
                    bot.logger.critical("HARD KILL permanece HARD | zero exposicao nao confirmado")

            prices = {s: self.md.get(s) for s in bot.SYMBOLS}
            for se in self.scalper_engines:
                try:
                    se.pre_reconcile(prices.get(se.symbol))
                except Exception as exc:
                    bot.logger.warning("SCALPER PRE-RECONCILE FAIL | %s | %s", se.id, exc)

            now_reconcile = bot.now_ms()
            if now_reconcile - self._last_periodic_reconcile_ms >= int(bot.RECONCILE_INTERVAL_SECONDS * 1000):
                self._last_periodic_reconcile_ms = now_reconcile
                try:
                    self.reconciler.reconcile()
                    self.retry_pending_bankroll_reset_after_reconcile()
                    if self.client.api_error_streak == 0:
                        _release_stale_api_kill_v83(self)
                except Exception as exc:
                    reason = f"RECONCILE_UNAVAILABLE:{type(exc).__name__}:{exc}"
                    with self.store.lock:
                        self.store.state["trade_gate"] = {"open_allowed": False, "reason": reason, "at": bot.now_iso()}
                    try:
                        self.store.save()
                    except Exception as save_error:
                        bot.logger.critical("RECONCILE FAIL-CLOSED | gate bloqueado em memoria; persistencia falhou | %s", save_error)
                    bot.logger.error("PERIODIC RECONCILE FAIL-CLOSED | novas entradas bloqueadas | %s", exc)

            for engine in self.range_engines:
                p = prices.get(engine.symbol)
                if p and p > 0:
                    try: engine.tick(p)
                    except Exception as exc: bot.logger.exception("RANGE TICK FAIL | %s | %s", engine.symbol, exc)
            for engine in self.pyramid_engines:
                p = prices.get(engine.symbol)
                if p and p > 0:
                    try: engine.tick(p)
                    except Exception as exc: bot.logger.exception("PYRAMID TICK FAIL | %s | %s", engine.id, exc)
            for engine in self.scalper_engines:
                p = prices.get(engine.symbol)
                if p and p > 0:
                    try: engine.tick(p)
                    except Exception as exc: bot.logger.exception("SCALPER TICK FAIL | %s | %s", engine.id, exc)

            now_audit = bot.now_ms()
            if now_audit - self._last_audit_log_ms >= 60000:
                self._last_audit_log_ms = now_audit
                try:
                    data = bot.audit_extract_open_orders_and_positions(self.reconciler, self.ledger)
                    bot.logger.info("OPEN_ORDERS_AUDIT %s", bot.audit_format_json_line(data))
                except Exception as exc:
                    bot.logger.debug("AUDIT EXTRACTION FAILED | %s", exc)
            self.heartbeat()
        except KeyboardInterrupt:
            break
        except Exception as exc:
            bot.logger.exception("MAIN LOOP | %s", exc)
        self.stop.wait(bot.MAIN_LOOP_SECONDS)
    self.shutdown()


bot.Bot.run = _run_v83
bot.VERSION = f"{bot.VERSION}-api-soft-kill-autorelease-v83"


def main():
    bot.logger.warning(
        "API SOFT KILL AUTORELEASE V83 ACTIVE | release=API_ERROR_STREAK_ONLY + "
        "authenticated_streak_zero + reconcile_OK + full_native_stop_coverage + no_blocks | fail_closed=True"
    )
    base.main()


if __name__ == "__main__":
    main()
