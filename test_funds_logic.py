
import unittest
from dataclasses import dataclass
from typing import Dict, Tuple

# Mocking the constants and classes needed from arbitrage_vb06
@dataclass
class PositionLine:
    code: str
    product: str
    qty: int = 0
    avg_price: float = 0.0
    last_price: float = 0.0
    direction: str = ""

class InstrumentRegistry:
    @classmethod
    def get_multiplier(cls, code: str):
        if "TX" in code: return 200
        return 1000

class PortfolioLedger:
    def __init__(self, fut_multiplier_default: int = 2000, fut_margin_ratio: float = 0.5):
        self.fut_multiplier_default = fut_multiplier_default 
        self.fut_margin_ratio = fut_margin_ratio
        self.cash_total: float = 0.0
        self.reserved_stock: float = 0.0
        self.reserved_margin: float = 0.0
        self.positions: Dict[str, PositionLine] = {}

    def upsert(self, line: PositionLine):
        self.positions[line.code] = line

    def check_funds(self, req_cash: float, req_margin: float) -> bool:
        s_cost, f_margin = self.est_holding_cost()
        occupied = s_cost + f_margin + self.reserved_stock + self.reserved_margin
        return (occupied + req_cash + req_margin) <= self.cash_total

    def reserve_funds(self, req_cash: float, req_margin: float):
        self.reserved_stock += req_cash
        self.reserved_margin += req_margin

    def release_reserved(self, req_cash: float, req_margin: float):
        self.reserved_stock = max(0.0, self.reserved_stock - req_cash)
        self.reserved_margin = max(0.0, self.reserved_margin - req_margin)

    def est_holding_cost(self) -> Tuple[float, float]:
        stock_cost = 0.0
        fut_margin = 0.0
        for p in self.positions.values():
            px = p.avg_price if p.avg_price > 0 else p.last_price
            if px <= 0: continue
            mult = InstrumentRegistry.get_multiplier(p.code)
            if p.product == "Stock":
                if p.qty > 0: stock_cost += px * p.qty * mult
            else:
                fut_margin += px * abs(p.qty) * mult * self.fut_margin_ratio
        return stock_cost, fut_margin

class TestFundsLogic(unittest.TestCase):
    def setUp(self):
        self.ledger = PortfolioLedger(fut_margin_ratio=0.5)
        self.ledger.cash_total = 1000000.0

    def test_basic_check(self):
        # Case 1: Enough funds
        self.assertTrue(self.ledger.check_funds(500000, 100000))
        # Case 2: Exactly enough
        self.assertTrue(self.ledger.check_funds(900000, 100000))
        # Case 3: Not enough
        self.assertFalse(self.ledger.check_funds(900001, 100000))

    def test_reservation_lifecycle(self):
        req_c = 200000
        req_m = 50000
        # Reserve
        self.ledger.reserve_funds(req_c, req_m)
        self.assertEqual(self.ledger.reserved_stock, req_c)
        self.assertEqual(self.ledger.reserved_margin, req_m)
        
        # After reserve, check_funds should account for it
        # Total occupied: 250,000. Remaining: 750,000
        self.assertTrue(self.ledger.check_funds(750000, 0))
        self.assertFalse(self.ledger.check_funds(750001, 0))
        
        # Release
        self.ledger.release_reserved(req_c, req_m)
        self.assertEqual(self.ledger.reserved_stock, 0)
        self.assertEqual(self.ledger.reserved_margin, 0)
        self.assertTrue(self.ledger.check_funds(1000000, 0))

    def test_with_positions(self):
        # Add a stock position: 1000 shares at 100 = 100,000 cost
        self.ledger.upsert(PositionLine(code="2330", product="Stock", qty=1, avg_price=100.0))
        s_cost, f_margin = self.ledger.est_holding_cost()
        self.assertEqual(s_cost, 100000.0)
        
        # Remaining: 900,000
        self.assertTrue(self.ledger.check_funds(900000, 0))
        self.assertFalse(self.ledger.check_funds(900001, 0))

if __name__ == "__main__":
    unittest.main()
