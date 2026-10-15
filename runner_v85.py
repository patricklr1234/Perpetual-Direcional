#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Perpetual Direcional production entrypoint v85.

Fixes a confirmed production deadlock introduced by v83/v84: the main loop
continued to reconcile and trade, but the periodic path did not call the already
implemented release_stale_protection_blocks_if_flat(). Because StateStore.entry_allowed()
intentionally fails closed when ANY protection block exists, one stale flat
PYRAMID native-stop installation block could permanently prevent every strategy
from entering, even when ledger/state/physical/open-orders were all flat.

v85 keeps the existing fail-closed protection policy. It only invokes the existing
safe stale-block release after a successful periodic reconciliation. That routine
independently proves state/ledger/physical/open-orders are flat before clearing
only the affected stale protection metadata. No position/order is closed or
cancelled and no risk/strategy parameters are changed.
"""

import runner_v84 as base

bot = base.bot
_original_run_v84 = bot.Bot.run


def _native_stop_coverage_ok_v85(self):
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


def _release_stale_api_kill_v85(self):
    with self.store.lock:
        ks = dict(self.store.state.get("kill_switch", {}) or {})
        operational = dict(self.store.state.get("operational_blocks", {}) or {})
        protection = dict(self.store.state.get("protection_blocks", {}) or {})
    if str(ks.get("mode")) != "SOFT" or not str(ks.get("reason") or "").startswith("API_ERROR_STREAK="):
        return False
    if self.client.api_error_streak != 0:
        return False
    if operational or protection:
        bot.logger.warning("API SOFT KILL V85 | KEEP FAIL-CLOSED | blocks operational=%s protection=%s", operational, protection)
        return False
    try:
        if not self.reconciler.reconcile():
            bot.logger.warning("API SOFT KILL V85 | KEEP FAIL-CLOSED | reconcile=False")
            return False
        ok, proof = _native_stop_coverage_ok_v85(self)
        if not ok:
            bot.logger.warning("API SOFT KILL V85 | KEEP FAIL-CLOSED | %s", proof)
            return False
        with self.store.lock:
            current = dict(self.store.state.get("kill_switch", {}) or {})
        if str(current.get("mode")) != "SOFT" or not str(current.get("reason") or "").startswith("API_ERROR_STREAK="):
            return False
        if self.client.api_error_streak != 0:
            return False
        self.store.kill("OFF", None)
        bot.logger.warning("API SOFT KILL V85 | RELEASE CONFIRMED | authenticated_streak=0 | reconcile=OK | native_stop_coverage=FULL | blocks=NONE | accounting=PRESERVED")
        return True
    except Exception as exc:
        bot.logger.exception("API SOFT KILL V85 | RELEASE VERIFY FAILED | fail_closed=True | %s", exc)
        return False


def _run_v85(self):
    self.startup()
    if bot.VALIDATE_API_ONLY:
        bot.logger.info("VALIDATE_API_ONLY concluido; encerrando sem alterar configuracoes e sem enviar ordens")
        self.shutdown()
        return
    _release_stale_api_kill_v85(self)

    # One immediate stale-block cleanup after startup/reconciliation. This is
    # strictly fail-closed and only clears a block after independent flat proof.
    try:
        self.release_stale_protection_blocks_if_flat()
    except Exception as exc:
        bot.logger.exception("STALE PROTECTION AUTO-RELEASE V85 | startup verify failed | fail_closed=True | %s", exc)

    while not self.stop.is_set():
        try:
            if self.client.api_error_streak >= bot.KILL_SWITCH_ON_API_ERRORS and self.store.killed() == "OFF":
                self.store.kill("SOFT", f"API_ERROR_STREAK={self.client.api_error_streak}")
            elif self.client.api_error_streak == 0:
                _release_stale_api_kill_v85(self)

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
                    # Confirmed fix: v83/v84 omitted this existing safe release path.
                    # It must run after successful reconciliation so a flat stale
                    # PYRAMID native-stop block cannot permanently freeze all entries.
                    self.release_stale_protection_blocks_if_flat()
                    self.retry_pending_bankroll_reset_after_reconcile()
                    if self.client.api_error_streak == 0:
                        _release_stale_api_kill_v85(self)
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
                    try:
                        engine.tick(p)
                    except Exception as exc:
                        bot.logger.exception("RANGE TICK FAIL | %s | %s", engine.symbol, exc)
            for engine in self.pyramid_engines:
                p = prices.get(engine.symbol)
                if p and p > 0:
                    try:
                        engine.tick(p)
                    except Exception as exc:
                        bot.logger.exception("PYRAMID TICK FAIL | %s | %s", engine.id, exc)
            for engine in self.scalper_engines:
                p = prices.get(engine.symbol)
                if p and p > 0:
                    try:
                        engine.tick(p)
                    except Exception as exc:
                        bot.logger.exception("SCALPER TICK FAIL | %s | %s", engine.id, exc)

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


bot.Bot.run = _run_v85
bot.VERSION = f"{bot.VERSION}-stale-protection-autorelease-v85"


def main():
    bot.logger.warning(
        "STALE PROTECTION AUTO-RELEASE V85 ACTIVE | periodic_reconcile=RELEASE_STALE_FLAT_BLOCKS | "
        "fail_closed=True | accounting=PRESERVED"
    )
    base.main()


if __name__ == "__main__":
    main()
