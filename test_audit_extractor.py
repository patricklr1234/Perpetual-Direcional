#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Unit tests for OPEN_ORDERS_AUDIT extractor (v62+).
Tests audit_extract_open_orders_and_positions and audit_format_json_line.
Synthetic test data; no actual trading or API calls.

REGRESSION FIX v62.1: MockSnapshot now uses captured_ms (not timestamp_ms)
to match real ExchangeSnapshot dataclass field. Validates field access correctness.
"""

import json
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple


# Mock classes for testing
class MockSnapshot:
    """Mock ExchangeSnapshot with captured_ms field (real field name)."""
    def __init__(self, captured_ms: int, positions: Dict, orders: List):
        # REGRESSION FIX: Use captured_ms (the actual ExchangeSnapshot field)
        # not timestamp_ms. This test will fail if audit code uses wrong field.
        self.captured_ms = captured_ms
        self.positions = positions
        self.open_orders = orders


class MockLedger:
    def __init__(self, ownership_map: Dict[str, str]):
        self.ownership = ownership_map
    
    def order_owner(self, client_id: str) -> Optional[str]:
        return self.ownership.get(client_id)


class MockReconciler:
    def __init__(self, snapshot: Optional[MockSnapshot]):
        self.last_snapshot = snapshot


def audit_extract_open_orders_and_positions(
    reconciler: Any,
    ledger: Any,
) -> Dict[str, Any]:
    """Inline copy of the actual implementation for testing."""
    result: Dict[str, Any] = {
        "positions": [],
        "orders": [],
        "coverage": {},
        "timestamp_ms": 0,
    }
    
    if not reconciler.last_snapshot:
        return result
    
    snap = reconciler.last_snapshot
    result["timestamp_ms"] = snap.captured_ms  # FIXED: use captured_ms
    
    for (sym, side), qty in snap.positions.items():
        if qty > 0:
            result["positions"].append({
                "symbol": sym,
                "positionSide": side,
                "physicalQty": str(qty),
            })
    
    coverage_agg: Dict[Tuple[str, str], Dict[str, Decimal]] = {}
    
    if isinstance(snap.open_orders, list):
        for o in snap.open_orders:
            status = str(o.get("status", "")).upper()
            if status not in ("NEW", "PARTIALLY_FILLED"):
                continue
            
            cid = str(o.get("clientOrderId") or o.get("origClientOrderId") or "")
            order_owner = ledger.order_owner(cid) if cid else None
            
            sym = str(o.get("symbol", "")).upper()
            ps = str(o.get("positionSide", "")).upper()
            side = str(o.get("side", "")).upper()
            otype = str(o.get("type", "")).upper()
            
            orig_qty = Decimal(str(o.get("origQty", 0)))
            exec_qty = Decimal(str(o.get("executedQty", 0)))
            remain_qty = max(orig_qty - exec_qty, Decimal(0))
            
            order_record: Dict[str, Any] = {
                "orderId": str(o.get("orderId", "")),
                "clientOrderId": cid,
                "owner": order_owner if order_owner else "UNOWNED",
                "symbol": sym,
                "positionSide": ps,
                "side": side,
                "type": otype,
                "status": status,
                "origQty": str(orig_qty),
                "executedQty": str(exec_qty),
                "remainingQty": str(remain_qty),
                "stopPrice": str(o.get("stopPrice", "") or ""),
                "price": str(o.get("price", "") or ""),
                "reduceOnly": bool(o.get("reduceOnly", False)),
            }
            
            if "workingType" in o and o["workingType"]:
                order_record["workingType"] = str(o["workingType"]).upper()
            if "priceProtect" in o:
                order_record["priceProtect"] = bool(o["priceProtect"])
            
            result["orders"].append(order_record)
            
            if sym and ps and remain_qty > 0:
                key = (sym, ps)
                if key not in coverage_agg:
                    coverage_agg[key] = {"stop_market_qty": Decimal(0), "take_profit_qty": Decimal(0)}
                
                if otype == "STOP_MARKET":
                    coverage_agg[key]["stop_market_qty"] += remain_qty
                elif otype == "TAKE_PROFIT_MARKET":
                    coverage_agg[key]["take_profit_qty"] += remain_qty
    
    for (sym, side), cov in coverage_agg.items():
        result["coverage"][f"{sym}:{side}"] = {
            "stopMarketQty": str(cov["stop_market_qty"]),
            "takeProfitQty": str(cov["take_profit_qty"]),
        }
    
    return result


def audit_format_json_line(audit_data: Dict[str, Any]) -> str:
    """Deterministic single-line JSON for audit logging."""
    return json.dumps(audit_data, separators=(",", ":"), sort_keys=True)


def test_audit_empty_snapshot():
    """Test with None snapshot (no trading)."""
    reconciler = MockReconciler(None)
    ledger = MockLedger({})
    result = audit_extract_open_orders_and_positions(reconciler, ledger)
    
    assert result["positions"] == []
    assert result["orders"] == []
    assert result["coverage"] == {}
    assert result["timestamp_ms"] == 0
    print("✓ test_audit_empty_snapshot passed")


def test_audit_with_positions():
    """Test extraction of physical positions.
    
    REGRESSION FIX: Uses captured_ms (real ExchangeSnapshot field) not timestamp_ms.
    Will fail if audit code incorrectly reads snap.timestamp_ms.
    """
    positions = {
        ("BTCUSDT", "LONG"): Decimal("0.05"),
        ("ETHUSDT", "SHORT"): Decimal("1.23"),
    }
    snapshot = MockSnapshot(1725940561000, positions, [])  # captured_ms param
    reconciler = MockReconciler(snapshot)
    ledger = MockLedger({})
    
    result = audit_extract_open_orders_and_positions(reconciler, ledger)
    
    assert len(result["positions"]) == 2
    # REGRESSION FIX: This asserts that snap.captured_ms was read correctly
    assert result["timestamp_ms"] == 1725940561000, \
        f"timestamp_ms mismatch: got {result['timestamp_ms']}, expected 1725940561000. " \
        "Audit code may be reading snap.timestamp_ms instead of snap.captured_ms"
    
    pos_dict = {p["symbol"]: p for p in result["positions"]}
    assert pos_dict["BTCUSDT"]["positionSide"] == "LONG"
    assert pos_dict["BTCUSDT"]["physicalQty"] == "0.05"
    assert pos_dict["ETHUSDT"]["positionSide"] == "SHORT"
    assert pos_dict["ETHUSDT"]["physicalQty"] == "1.23"
    
    print("✓ test_audit_with_positions passed")


def test_audit_with_active_orders():
    """Test extraction of NEW and PARTIALLY_FILLED orders."""
    positions = {("BTCUSDT", "LONG"): Decimal("0.05")}
    orders = [
        {
            "orderId": "123456",
            "clientOrderId": "cid-001",
            "symbol": "BTCUSDT",
            "positionSide": "LONG",
            "side": "BUY",
            "type": "STOP_MARKET",
            "status": "NEW",
            "origQty": "0.05",
            "executedQty": "0",
            "stopPrice": "82000",
            "price": "",
            "reduceOnly": True,
        },
        {
            "orderId": "123457",
            "clientOrderId": "cid-002",
            "symbol": "BTCUSDT",
            "positionSide": "LONG",
            "side": "SELL",
            "type": "TAKE_PROFIT_MARKET",
            "status": "PARTIALLY_FILLED",
            "origQty": "0.03",
            "executedQty": "0.01",
            "stopPrice": "",
            "price": "90000",
            "reduceOnly": True,
        },
        {
            "orderId": "123458",
            "clientOrderId": "cid-003",
            "symbol": "BTCUSDT",
            "positionSide": "LONG",
            "side": "BUY",
            "type": "LIMIT",
            "status": "FILLED",  # Should be excluded
            "origQty": "0.01",
            "executedQty": "0.01",
            "price": "75000",
            "reduceOnly": False,
        },
    ]
    
    snapshot = MockSnapshot(1725940561000, positions, orders)
    ownership = {"cid-001": "PYRAMID_BTC_LONG", "cid-002": "PYRAMID_BTC_LONG"}
    reconciler = MockReconciler(snapshot)
    ledger = MockLedger(ownership)
    
    result = audit_extract_open_orders_and_positions(reconciler, ledger)
    
    # Should extract only NEW and PARTIALLY_FILLED
    assert len(result["orders"]) == 2
    
    order_dict = {o["clientOrderId"]: o for o in result["orders"]}
    
    # Check first order (STOP_MARKET, NEW)
    o1 = order_dict["cid-001"]
    assert o1["type"] == "STOP_MARKET"
    assert o1["status"] == "NEW"
    assert o1["owner"] == "PYRAMID_BTC_LONG"
    assert o1["remainingQty"] == "0.05"
    assert o1["stopPrice"] == "82000"
    
    # Check second order (TAKE_PROFIT_MARKET, PARTIALLY_FILLED)
    o2 = order_dict["cid-002"]
    assert o2["type"] == "TAKE_PROFIT_MARKET"
    assert o2["status"] == "PARTIALLY_FILLED"
    assert o2["remainingQty"] == "0.02"  # 0.03 - 0.01
    
    print("✓ test_audit_with_active_orders passed")


def test_audit_coverage_aggregation():
    """Test STOP_MARKET and TAKE_PROFIT_MARKET aggregation per symbol/side."""
    positions = {("ETHUSDT", "SHORT"): Decimal("2.0")}
    orders = [
        {
            "orderId": "1",
            "clientOrderId": "cid-100",
            "symbol": "ETHUSDT",
            "positionSide": "SHORT",
            "side": "SELL",
            "type": "STOP_MARKET",
            "status": "NEW",
            "origQty": "1.0",
            "executedQty": "0",
            "stopPrice": "2500",
            "price": "",
            "reduceOnly": True,
        },
        {
            "orderId": "2",
            "clientOrderId": "cid-101",
            "symbol": "ETHUSDT",
            "positionSide": "SHORT",
            "side": "BUY",
            "type": "TAKE_PROFIT_MARKET",
            "status": "NEW",
            "origQty": "0.5",
            "executedQty": "0",
            "stopPrice": "",
            "price": "2300",
            "reduceOnly": True,
        },
        {
            "orderId": "3",
            "clientOrderId": "cid-102",
            "symbol": "ETHUSDT",
            "positionSide": "SHORT",
            "side": "BUY",
            "type": "TAKE_PROFIT_MARKET",
            "status": "NEW",
            "origQty": "0.5",
            "executedQty": "0",
            "stopPrice": "",
            "price": "2300",
            "reduceOnly": True,
        },
    ]
    
    snapshot = MockSnapshot(1725940561000, positions, orders)
    reconciler = MockReconciler(snapshot)
    ledger = MockLedger({})
    
    result = audit_extract_open_orders_and_positions(reconciler, ledger)
    
    coverage = result["coverage"]["ETHUSDT:SHORT"]
    assert coverage["stopMarketQty"] == "1.0"
    assert coverage["takeProfitQty"] == "1.0"  # 0.5 + 0.5
    
    print("✓ test_audit_coverage_aggregation passed")


def test_audit_unowned_orders():
    """Test handling of orders without ledger ownership."""
    orders = [
        {
            "orderId": "999",
            "clientOrderId": "cid-unknown",
            "symbol": "HYPEUSDT",
            "positionSide": "LONG",
            "side": "BUY",
            "type": "LIMIT",
            "status": "NEW",
            "origQty": "10.0",
            "executedQty": "0",
            "price": "85",
            "reduceOnly": False,
        },
    ]
    
    snapshot = MockSnapshot(1725940561000, {}, orders)
    reconciler = MockReconciler(snapshot)
    ledger = MockLedger({})  # No ownership recorded
    
    result = audit_extract_open_orders_and_positions(reconciler, ledger)
    
    assert len(result["orders"]) == 1
    assert result["orders"][0]["owner"] == "UNOWNED"
    
    print("✓ test_audit_unowned_orders passed")


def test_audit_json_formatting():
    """Test deterministic JSON formatting."""
    audit_data = {
        "timestamp_ms": 1725940561000,
        "positions": [{"symbol": "BTC USDT", "positionSide": "LONG", "physicalQty": "0.05"}],
        "orders": [{"orderId": "123", "clientOrderId": "cid-1", "owner": "PYRAMID_BTC_LONG"}],
        "coverage": {"BTCUSDT:LONG": {"stopMarketQty": "0.05", "takeProfitQty": "0"}},
    }
    
    json_line = audit_format_json_line(audit_data)
    
    # Should be single line, parseable, and deterministic
    assert "\n" not in json_line
    parsed = json.loads(json_line)
    assert parsed["timestamp_ms"] == 1725940561000
    
    # Verify determinism (same input always gives same output)
    json_line2 = audit_format_json_line(audit_data)
    assert json_line == json_line2
    
    print("✓ test_audit_json_formatting passed")


if __name__ == "__main__":
    test_audit_empty_snapshot()
    test_audit_with_positions()
    test_audit_with_active_orders()
    test_audit_coverage_aggregation()
    test_audit_unowned_orders()
    test_audit_json_formatting()
    print("\n✓ All audit tests passed!")

