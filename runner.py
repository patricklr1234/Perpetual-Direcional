#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Production entrypoint for Perpetual Direcional.

Keeps the PYRAMID risk-policy override and fixes watchdog observability without
changing strategy execution:
- Native STOP_MARKET starts 1% adverse to weighted entry.
- After +1% favorable movement, existing main.py logic arms breakeven.
- OPEN_ORDERS_AUDIT uses ExchangeSnapshot.captured_ms (the actual dataclass field)
  instead of the obsolete timestamp_ms name.

State, ledger, bankrolls, positions, native orders, Scalper logic, sizing,
recovery parameters, BOT_DIR and Volume semantics are preserved.
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


bot.PyramidEngine._max_loss_stop_price = _pyramid_one_percent_native_stop_price
bot.audit_extract_open_orders_and_positions = _fixed_audit_extract
bot.VERSION = f"{bot.VERSION}-native-1pct-be-auditfix"


def main() -> None:
    bot.logger.warning(
        "PYRAMID RISK POLICY ACTIVE | native_adverse_stop=%s%% | breakeven_after_favorable=%s%% | max_loss_usd=%s additional_software_ceiling",
        bot.PYRAMID_ADVERSE_REVERSAL_PCT * bot.D(100),
        bot.PYRAMID_RETURN_EXIT_ARM_PCT * bot.D(100),
        bot.PYRAMID_MAX_LOSS_USD,
    )
    bot.logger.warning("OPEN_ORDERS_AUDIT FIX ACTIVE | snapshot_field=captured_ms")
    bot.main()


if __name__ == "__main__":
    main()
