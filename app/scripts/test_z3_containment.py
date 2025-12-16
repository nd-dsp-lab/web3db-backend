"""
Unit Tests for Z3-Based Query Containment Checker

Tests the Z3ContainmentChecker used in the semantic cache to verify
that query containment detection works correctly.
"""

import unittest
import sys
import os

# Ensure we can import from the scripts directory
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from query_parser import Predicate, PredicateGroup, PredicateOperator, LogicalOperator

try:
    from z3_containment import Z3ContainmentChecker, Z3QueryEncoder, is_z3_available
    Z3_AVAILABLE = is_z3_available()
except ImportError:
    Z3_AVAILABLE = False


@unittest.skipIf(not Z3_AVAILABLE, "Z3 solver not installed")
class TestZ3ContainmentBasic(unittest.TestCase):
    """Basic containment tests for single predicates"""
    
    def setUp(self):
        self.checker = Z3ContainmentChecker(timeout_ms=1000)
    
    # ==================== Range Tests (GT/GTE) ====================
    
    def test_gt_contained(self):
        """Age > 50 ⊆ Age > 40 → True"""
        cached = PredicateGroup(predicates=[Predicate("Age", PredicateOperator.GT, 40)])
        new = PredicateGroup(predicates=[Predicate("Age", PredicateOperator.GT, 50)])
        is_contained, _ = self.checker.is_contained(cached, new)
        self.assertTrue(is_contained)
    
    def test_gt_not_contained(self):
        """Age > 30 ⊆ Age > 40 → False"""
        cached = PredicateGroup(predicates=[Predicate("Age", PredicateOperator.GT, 40)])
        new = PredicateGroup(predicates=[Predicate("Age", PredicateOperator.GT, 30)])
        is_contained, _ = self.checker.is_contained(cached, new)
        self.assertFalse(is_contained)
    
    def test_gte_contained(self):
        """Age >= 50 ⊆ Age >= 40 → True"""
        cached = PredicateGroup(predicates=[Predicate("Age", PredicateOperator.GTE, 40)])
        new = PredicateGroup(predicates=[Predicate("Age", PredicateOperator.GTE, 50)])
        is_contained, _ = self.checker.is_contained(cached, new)
        self.assertTrue(is_contained)
    
    def test_gt_eq_contained(self):
        """Age = 50 ⊆ Age > 40 → True (50 > 40)"""
        cached = PredicateGroup(predicates=[Predicate("Age", PredicateOperator.GT, 40)])
        new = PredicateGroup(predicates=[Predicate("Age", PredicateOperator.EQ, 50)])
        is_contained, _ = self.checker.is_contained(cached, new)
        self.assertTrue(is_contained)
    
    def test_gt_eq_not_contained(self):
        """Age = 30 ⊆ Age > 40 → False (30 < 40)"""
        cached = PredicateGroup(predicates=[Predicate("Age", PredicateOperator.GT, 40)])
        new = PredicateGroup(predicates=[Predicate("Age", PredicateOperator.EQ, 30)])
        is_contained, _ = self.checker.is_contained(cached, new)
        self.assertFalse(is_contained)
    
    # ==================== Range Tests (LT/LTE) ====================
    
    def test_lt_contained(self):
        """Age < 30 ⊆ Age < 50 → True"""
        cached = PredicateGroup(predicates=[Predicate("Age", PredicateOperator.LT, 50)])
        new = PredicateGroup(predicates=[Predicate("Age", PredicateOperator.LT, 30)])
        is_contained, _ = self.checker.is_contained(cached, new)
        self.assertTrue(is_contained)
    
    def test_lt_not_contained(self):
        """Age < 60 ⊆ Age < 50 → False"""
        cached = PredicateGroup(predicates=[Predicate("Age", PredicateOperator.LT, 50)])
        new = PredicateGroup(predicates=[Predicate("Age", PredicateOperator.LT, 60)])
        is_contained, _ = self.checker.is_contained(cached, new)
        self.assertFalse(is_contained)
    
    # ==================== BETWEEN Tests ====================
    
    def test_between_contained(self):
        """Age BETWEEN 20 AND 60 ⊆ Age BETWEEN 10 AND 80 → True"""
        cached = PredicateGroup(predicates=[Predicate("Age", PredicateOperator.BETWEEN, (10, 80))])
        new = PredicateGroup(predicates=[Predicate("Age", PredicateOperator.BETWEEN, (20, 60))])
        is_contained, _ = self.checker.is_contained(cached, new)
        self.assertTrue(is_contained)
    
    def test_between_not_contained(self):
        """Age BETWEEN 5 AND 90 ⊆ Age BETWEEN 10 AND 80 → False"""
        cached = PredicateGroup(predicates=[Predicate("Age", PredicateOperator.BETWEEN, (10, 80))])
        new = PredicateGroup(predicates=[Predicate("Age", PredicateOperator.BETWEEN, (5, 90))])
        is_contained, _ = self.checker.is_contained(cached, new)
        self.assertFalse(is_contained)
    
    # ==================== IN Clause Tests ====================
    
    def test_in_contained(self):
        """ID IN (1,2) ⊆ ID IN (1,2,3) → True"""
        cached = PredicateGroup(predicates=[Predicate("ID", PredicateOperator.IN, [1, 2, 3])])
        new = PredicateGroup(predicates=[Predicate("ID", PredicateOperator.IN, [1, 2])])
        is_contained, _ = self.checker.is_contained(cached, new)
        self.assertTrue(is_contained)
    
    def test_in_not_contained(self):
        """ID IN (1,2,4) ⊆ ID IN (1,2,3) → False (4 not in cached)"""
        cached = PredicateGroup(predicates=[Predicate("ID", PredicateOperator.IN, [1, 2, 3])])
        new = PredicateGroup(predicates=[Predicate("ID", PredicateOperator.IN, [1, 2, 4])])
        is_contained, _ = self.checker.is_contained(cached, new)
        self.assertFalse(is_contained)
    
    def test_in_eq_contained(self):
        """ID = 2 ⊆ ID IN (1,2,3) → True"""
        cached = PredicateGroup(predicates=[Predicate("ID", PredicateOperator.IN, [1, 2, 3])])
        new = PredicateGroup(predicates=[Predicate("ID", PredicateOperator.EQ, 2)])
        is_contained, _ = self.checker.is_contained(cached, new)
        self.assertTrue(is_contained)
    
    # ==================== Empty Predicate Tests ====================
    
    def test_empty_cached_with_filter(self):
        """New has filter, cached is empty (all data) → True with filter"""
        cached = PredicateGroup()  # Empty = all data
        new = PredicateGroup(predicates=[Predicate("Age", PredicateOperator.GT, 40)])
        is_contained, filter_sql = self.checker.is_contained(cached, new)
        self.assertTrue(is_contained)
        self.assertIsNotNone(filter_sql)
    
    def test_empty_new_cached_has_filter(self):
        """New is empty (wants all), cached has filter → False"""
        cached = PredicateGroup(predicates=[Predicate("Age", PredicateOperator.GT, 40)])
        new = PredicateGroup()  # Empty = wants all data
        is_contained, _ = self.checker.is_contained(cached, new)
        self.assertFalse(is_contained)
    
    def test_both_empty(self):
        """Both empty → True"""
        cached = PredicateGroup()
        new = PredicateGroup()
        is_contained, filter_sql = self.checker.is_contained(cached, new)
        self.assertTrue(is_contained)
        self.assertIsNone(filter_sql)


@unittest.skipIf(not Z3_AVAILABLE, "Z3 solver not installed")
class TestZ3ContainmentLogical(unittest.TestCase):
    """Tests for AND/OR combined predicates"""
    
    def setUp(self):
        self.checker = Z3ContainmentChecker(timeout_ms=1000)
    
    def test_and_contained(self):
        """(Age > 50 AND Salary > 60000) ⊆ (Age > 40 AND Salary > 50000) → True"""
        cached = PredicateGroup(
            operator=LogicalOperator.AND,
            predicates=[
                Predicate("Age", PredicateOperator.GT, 40),
                Predicate("Salary", PredicateOperator.GT, 50000)
            ]
        )
        new = PredicateGroup(
            operator=LogicalOperator.AND,
            predicates=[
                Predicate("Age", PredicateOperator.GT, 50),
                Predicate("Salary", PredicateOperator.GT, 60000)
            ]
        )
        is_contained, _ = self.checker.is_contained(cached, new)
        self.assertTrue(is_contained)
    
    def test_or_contained(self):
        """(Age > 50 OR Status='VIP') ⊆ (Age > 40 OR Status='VIP') → True"""
        cached = PredicateGroup(
            operator=LogicalOperator.OR,
            predicates=[
                Predicate("Age", PredicateOperator.GT, 40),
                Predicate("Status", PredicateOperator.EQ, "VIP")
            ]
        )
        new = PredicateGroup(
            operator=LogicalOperator.OR,
            predicates=[
                Predicate("Age", PredicateOperator.GT, 50),
                Predicate("Status", PredicateOperator.EQ, "VIP")
            ]
        )
        is_contained, _ = self.checker.is_contained(cached, new)
        self.assertTrue(is_contained)
    
    def test_or_not_contained(self):
        """(Age > 30 OR Status='VIP') ⊆ (Age > 40 OR Status='VIP') → False"""
        cached = PredicateGroup(
            operator=LogicalOperator.OR,
            predicates=[
                Predicate("Age", PredicateOperator.GT, 40),
                Predicate("Status", PredicateOperator.EQ, "VIP")
            ]
        )
        new = PredicateGroup(
            operator=LogicalOperator.OR,
            predicates=[
                Predicate("Age", PredicateOperator.GT, 30),  # Less restrictive
                Predicate("Status", PredicateOperator.EQ, "VIP")
            ]
        )
        is_contained, _ = self.checker.is_contained(cached, new)
        self.assertFalse(is_contained)


@unittest.skipIf(not Z3_AVAILABLE, "Z3 solver not installed")
class TestZ3ContainmentStrings(unittest.TestCase):
    """Tests for string predicates"""
    
    def setUp(self):
        self.checker = Z3ContainmentChecker(timeout_ms=1000)
    
    def test_string_eq_contained(self):
        """Status = 'Active' ⊆ Status IN ('Active', 'Pending') → True"""
        cached = PredicateGroup(predicates=[
            Predicate("Status", PredicateOperator.IN, ["Active", "Pending"])
        ])
        new = PredicateGroup(predicates=[
            Predicate("Status", PredicateOperator.EQ, "Active")
        ])
        is_contained, _ = self.checker.is_contained(cached, new)
        self.assertTrue(is_contained)
    
    def test_string_eq_same(self):
        """Status = 'Active' ⊆ Status = 'Active' → True"""
        cached = PredicateGroup(predicates=[
            Predicate("Status", PredicateOperator.EQ, "Active")
        ])
        new = PredicateGroup(predicates=[
            Predicate("Status", PredicateOperator.EQ, "Active")
        ])
        is_contained, _ = self.checker.is_contained(cached, new)
        self.assertTrue(is_contained)


@unittest.skipIf(not Z3_AVAILABLE, "Z3 solver not installed")
class TestZ3Performance(unittest.TestCase):
    """Performance tests for Z3 solver"""
    
    def setUp(self):
        self.checker = Z3ContainmentChecker(timeout_ms=1000)
    
    def test_simple_check_under_100ms(self):
        """Simple containment check should complete in under 100ms"""
        import time
        
        cached = PredicateGroup(predicates=[Predicate("Age", PredicateOperator.GT, 40)])
        new = PredicateGroup(predicates=[Predicate("Age", PredicateOperator.GT, 50)])
        
        start = time.time()
        for _ in range(10):
            self.checker.is_contained(cached, new)
        elapsed = (time.time() - start) * 1000 / 10  # Average per check
        
        self.assertLess(elapsed, 100, f"Check took {elapsed:.2f}ms, expected < 100ms")


if __name__ == "__main__":
    if not Z3_AVAILABLE:
        print("Z3 is not available. Install with: pip install z3-solver")
        sys.exit(1)
    
    unittest.main(verbosity=2)
