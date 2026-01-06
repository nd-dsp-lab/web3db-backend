"""
Unit Tests for Z3-Based JOIN Query Containment Checker

Tests the Z3JoinContainmentChecker used in the semantic cache to verify
that JOIN query containment detection works correctly.
"""

import unittest
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from query_parser import (
    JoinCondition, 
    Predicate, 
    PredicateGroup, 
    PredicateOperator,
    LogicalOperator
)

try:
    from z3_containment import (
        Z3JoinContainmentChecker, 
        is_z3_available,
        JOIN_TYPE_COMPATIBLE
    )
    Z3_AVAILABLE = is_z3_available()
except ImportError:
    Z3_AVAILABLE = False


@unittest.skipUnless(Z3_AVAILABLE, "Z3 not available")
class TestJoinStructureEquivalence(unittest.TestCase):
    """Tests for join condition equivalence checking"""
    
    def setUp(self):
        self.checker = Z3JoinContainmentChecker()
    
    def test_same_joins_equivalent(self):
        """Same join conditions are equivalent"""
        cached = [JoinCondition("patients", "HospitalID", "hospitals", "ID", "INNER")]
        new = [JoinCondition("patients", "HospitalID", "hospitals", "ID", "INNER")]
        
        self.assertTrue(self.checker.check_join_structure_equivalence(cached, new))
    
    def test_reversed_order_equivalent(self):
        """A.x = B.y is equivalent to B.y = A.x"""
        cached = [JoinCondition("patients", "HospitalID", "hospitals", "ID", "INNER")]
        new = [JoinCondition("hospitals", "ID", "patients", "HospitalID", "INNER")]
        
        self.assertTrue(self.checker.check_join_structure_equivalence(cached, new))
    
    def test_different_columns_not_equivalent(self):
        """Different join columns are not equivalent"""
        cached = [JoinCondition("patients", "HospitalID", "hospitals", "ID", "INNER")]
        new = [JoinCondition("patients", "DoctorID", "hospitals", "ID", "INNER")]
        
        self.assertFalse(self.checker.check_join_structure_equivalence(cached, new))
    
    def test_different_tables_not_equivalent(self):
        """Different tables are not equivalent"""
        cached = [JoinCondition("patients", "HospitalID", "hospitals", "ID", "INNER")]
        new = [JoinCondition("patients", "HospitalID", "clinics", "ID", "INNER")]
        
        self.assertFalse(self.checker.check_join_structure_equivalence(cached, new))
    
    def test_additional_join_not_equivalent(self):
        """Additional join condition is not equivalent"""
        cached = [JoinCondition("patients", "HospitalID", "hospitals", "ID", "INNER")]
        new = [
            JoinCondition("patients", "HospitalID", "hospitals", "ID", "INNER"),
            JoinCondition("patients", "DoctorID", "doctors", "ID", "INNER")
        ]
        
        self.assertFalse(self.checker.check_join_structure_equivalence(cached, new))
    
    def test_multi_join_equivalent(self):
        """Multiple joins can be equivalent"""
        cached = [
            JoinCondition("patients", "HospitalID", "hospitals", "ID", "INNER"),
            JoinCondition("patients", "DoctorID", "doctors", "ID", "INNER")
        ]
        # Same joins, different order in list
        new = [
            JoinCondition("patients", "DoctorID", "doctors", "ID", "INNER"),
            JoinCondition("patients", "HospitalID", "hospitals", "ID", "INNER")
        ]
        
        self.assertTrue(self.checker.check_join_structure_equivalence(cached, new))


@unittest.skipUnless(Z3_AVAILABLE, "Z3 not available")
class TestJoinTypeCompatibility(unittest.TestCase):
    """Tests for join type compatibility"""
    
    def setUp(self):
        self.checker = Z3JoinContainmentChecker()
    
    def test_inner_inner_compatible(self):
        """INNER JOIN is compatible with INNER JOIN"""
        cached = [JoinCondition("A", "x", "B", "y", "INNER")]
        new = [JoinCondition("A", "x", "B", "y", "INNER")]
        
        self.assertTrue(self.checker.check_join_types_compatible(cached, new))
    
    def test_left_left_compatible(self):
        """LEFT JOIN is compatible with LEFT JOIN"""
        cached = [JoinCondition("A", "x", "B", "y", "LEFT")]
        new = [JoinCondition("A", "x", "B", "y", "LEFT")]
        
        self.assertTrue(self.checker.check_join_types_compatible(cached, new))
    
    def test_left_inner_not_compatible(self):
        """LEFT JOIN cached cannot serve INNER JOIN query"""
        cached = [JoinCondition("A", "x", "B", "y", "LEFT")]
        new = [JoinCondition("A", "x", "B", "y", "INNER")]
        
        self.assertFalse(self.checker.check_join_types_compatible(cached, new))
    
    def test_inner_left_not_compatible(self):
        """INNER JOIN cached cannot serve LEFT JOIN query"""
        cached = [JoinCondition("A", "x", "B", "y", "INNER")]
        new = [JoinCondition("A", "x", "B", "y", "LEFT")]
        
        self.assertFalse(self.checker.check_join_types_compatible(cached, new))


@unittest.skipUnless(Z3_AVAILABLE, "Z3 not available")
class TestInnerJoinContainment(unittest.TestCase):
    """Tests for INNER JOIN containment"""
    
    def setUp(self):
        self.checker = Z3JoinContainmentChecker()
    
    def test_stricter_where_on_left_table(self):
        """Stricter WHERE on left table is contained"""
        # Cached: patients JOIN hospitals WHERE Age > 40
        # New: patients JOIN hospitals WHERE Age > 50
        cached_tables = ["patients", "hospitals"]
        new_tables = ["patients", "hospitals"]
        cached_joins = [JoinCondition("patients", "HospitalID", "hospitals", "ID", "INNER")]
        new_joins = [JoinCondition("patients", "HospitalID", "hospitals", "ID", "INNER")]
        
        cached_where = {
            "patients": PredicateGroup(predicates=[
                Predicate("Age", PredicateOperator.GT, 40)
            ])
        }
        new_where = {
            "patients": PredicateGroup(predicates=[
                Predicate("Age", PredicateOperator.GT, 50)
            ])
        }
        
        is_contained, filter_sql = self.checker.check_join_containment(
            cached_tables, new_tables,
            cached_joins, new_joins,
            cached_where, new_where
        )
        
        self.assertTrue(is_contained)
        self.assertIsNotNone(filter_sql)
        self.assertIn("Age", filter_sql)
    
    def test_stricter_where_on_right_table(self):
        """Stricter WHERE on right table is contained"""
        # Cached: patients JOIN hospitals WHERE hospitals.Region IN ('East', 'West')
        # New: patients JOIN hospitals WHERE hospitals.Region = 'East'
        cached_tables = ["patients", "hospitals"]
        new_tables = ["patients", "hospitals"]
        cached_joins = [JoinCondition("patients", "HospitalID", "hospitals", "ID", "INNER")]
        new_joins = [JoinCondition("patients", "HospitalID", "hospitals", "ID", "INNER")]
        
        cached_where = {
            "hospitals": PredicateGroup(predicates=[
                Predicate("Region", PredicateOperator.IN, ["East", "West"])
            ])
        }
        new_where = {
            "hospitals": PredicateGroup(predicates=[
                Predicate("Region", PredicateOperator.EQ, "East")
            ])
        }
        
        is_contained, filter_sql = self.checker.check_join_containment(
            cached_tables, new_tables,
            cached_joins, new_joins,
            cached_where, new_where
        )
        
        self.assertTrue(is_contained)
    
    def test_stricter_where_on_both_tables(self):
        """Stricter WHERE on both tables is contained"""
        cached_tables = ["patients", "hospitals"]
        new_tables = ["patients", "hospitals"]
        cached_joins = [JoinCondition("patients", "HospitalID", "hospitals", "ID", "INNER")]
        new_joins = [JoinCondition("patients", "HospitalID", "hospitals", "ID", "INNER")]
        
        cached_where = {
            "patients": PredicateGroup(predicates=[
                Predicate("Age", PredicateOperator.GT, 40)
            ]),
            "hospitals": PredicateGroup(predicates=[
                Predicate("Capacity", PredicateOperator.GT, 100)
            ])
        }
        new_where = {
            "patients": PredicateGroup(predicates=[
                Predicate("Age", PredicateOperator.GT, 50)
            ]),
            "hospitals": PredicateGroup(predicates=[
                Predicate("Capacity", PredicateOperator.GT, 200)
            ])
        }
        
        is_contained, filter_sql = self.checker.check_join_containment(
            cached_tables, new_tables,
            cached_joins, new_joins,
            cached_where, new_where
        )
        
        self.assertTrue(is_contained)
    
    def test_looser_where_not_contained(self):
        """Looser WHERE predicate is NOT contained"""
        cached_tables = ["patients", "hospitals"]
        new_tables = ["patients", "hospitals"]
        cached_joins = [JoinCondition("patients", "HospitalID", "hospitals", "ID", "INNER")]
        new_joins = [JoinCondition("patients", "HospitalID", "hospitals", "ID", "INNER")]
        
        # Cached has Age > 50, but new wants Age > 40 (looser)
        cached_where = {
            "patients": PredicateGroup(predicates=[
                Predicate("Age", PredicateOperator.GT, 50)
            ])
        }
        new_where = {
            "patients": PredicateGroup(predicates=[
                Predicate("Age", PredicateOperator.GT, 40)
            ])
        }
        
        is_contained, filter_sql = self.checker.check_join_containment(
            cached_tables, new_tables,
            cached_joins, new_joins,
            cached_where, new_where
        )
        
        self.assertFalse(is_contained)
    
    def test_no_where_cached_with_new_where(self):
        """Cache with no WHERE can serve query with WHERE"""
        cached_tables = ["patients", "hospitals"]
        new_tables = ["patients", "hospitals"]
        cached_joins = [JoinCondition("patients", "HospitalID", "hospitals", "ID", "INNER")]
        new_joins = [JoinCondition("patients", "HospitalID", "hospitals", "ID", "INNER")]
        
        cached_where = {}  # No WHERE predicates
        new_where = {
            "patients": PredicateGroup(predicates=[
                Predicate("Age", PredicateOperator.GT, 50)
            ])
        }
        
        is_contained, filter_sql = self.checker.check_join_containment(
            cached_tables, new_tables,
            cached_joins, new_joins,
            cached_where, new_where
        )
        
        self.assertTrue(is_contained)
        self.assertIn("Age", filter_sql)
    
    def test_different_tables_not_contained(self):
        """Different tables are not contained"""
        cached_tables = ["patients", "hospitals"]
        new_tables = ["patients", "clinics"]  # Different table
        cached_joins = [JoinCondition("patients", "HospitalID", "hospitals", "ID", "INNER")]
        new_joins = [JoinCondition("patients", "ClinicID", "clinics", "ID", "INNER")]
        
        cached_where = {}
        new_where = {}
        
        is_contained, filter_sql = self.checker.check_join_containment(
            cached_tables, new_tables,
            cached_joins, new_joins,
            cached_where, new_where
        )
        
        self.assertFalse(is_contained)


@unittest.skipUnless(Z3_AVAILABLE, "Z3 not available")
class TestLeftJoinContainment(unittest.TestCase):
    """Tests for LEFT JOIN containment"""
    
    def setUp(self):
        self.checker = Z3JoinContainmentChecker()
    
    def test_left_join_same_structure_stricter_left_where(self):
        """LEFT JOIN with stricter WHERE on left table is contained"""
        cached_tables = ["patients", "hospitals"]
        new_tables = ["patients", "hospitals"]
        cached_joins = [JoinCondition("patients", "HospitalID", "hospitals", "ID", "LEFT")]
        new_joins = [JoinCondition("patients", "HospitalID", "hospitals", "ID", "LEFT")]
        
        cached_where = {
            "patients": PredicateGroup(predicates=[
                Predicate("Age", PredicateOperator.GT, 40)
            ])
        }
        new_where = {
            "patients": PredicateGroup(predicates=[
                Predicate("Age", PredicateOperator.GT, 50)
            ])
        }
        
        is_contained, filter_sql = self.checker.check_join_containment(
            cached_tables, new_tables,
            cached_joins, new_joins,
            cached_where, new_where
        )
        
        self.assertTrue(is_contained)
    
    def test_left_to_inner_not_contained(self):
        """LEFT JOIN cache cannot serve INNER JOIN query"""
        cached_tables = ["patients", "hospitals"]
        new_tables = ["patients", "hospitals"]
        cached_joins = [JoinCondition("patients", "HospitalID", "hospitals", "ID", "LEFT")]
        new_joins = [JoinCondition("patients", "HospitalID", "hospitals", "ID", "INNER")]
        
        cached_where = {}
        new_where = {}
        
        is_contained, filter_sql = self.checker.check_join_containment(
            cached_tables, new_tables,
            cached_joins, new_joins,
            cached_where, new_where
        )
        
        self.assertFalse(is_contained)
    
    def test_inner_to_left_not_contained(self):
        """INNER JOIN cache cannot serve LEFT JOIN query"""
        cached_tables = ["patients", "hospitals"]
        new_tables = ["patients", "hospitals"]
        cached_joins = [JoinCondition("patients", "HospitalID", "hospitals", "ID", "INNER")]
        new_joins = [JoinCondition("patients", "HospitalID", "hospitals", "ID", "LEFT")]
        
        cached_where = {}
        new_where = {}
        
        is_contained, filter_sql = self.checker.check_join_containment(
            cached_tables, new_tables,
            cached_joins, new_joins,
            cached_where, new_where
        )
        
        self.assertFalse(is_contained)


@unittest.skipUnless(Z3_AVAILABLE, "Z3 not available")
class TestJoinConditionHelpers(unittest.TestCase):
    """Tests for JoinCondition helper methods"""
    
    def test_normalized_key_same_join(self):
        """Same join should have same normalized key"""
        jc1 = JoinCondition("A", "x", "B", "y", "INNER")
        jc2 = JoinCondition("A", "x", "B", "y", "INNER")
        
        self.assertEqual(jc1.get_normalized_key(), jc2.get_normalized_key())
    
    def test_normalized_key_reversed(self):
        """Reversed join should have same normalized key"""
        jc1 = JoinCondition("A", "x", "B", "y", "INNER")
        jc2 = JoinCondition("B", "y", "A", "x", "INNER")
        
        self.assertEqual(jc1.get_normalized_key(), jc2.get_normalized_key())
    
    def test_normalized_key_different_type(self):
        """Different join type should have different normalized key"""
        jc1 = JoinCondition("A", "x", "B", "y", "INNER")
        jc2 = JoinCondition("A", "x", "B", "y", "LEFT")
        
        self.assertNotEqual(jc1.get_normalized_key(), jc2.get_normalized_key())
    
    def test_is_equivalent_to_same(self):
        """Same joins are equivalent"""
        jc1 = JoinCondition("A", "x", "B", "y", "INNER")
        jc2 = JoinCondition("A", "x", "B", "y", "INNER")
        
        self.assertTrue(jc1.is_equivalent_to(jc2))
    
    def test_is_equivalent_to_reversed(self):
        """Reversed joins are equivalent"""
        jc1 = JoinCondition("A", "x", "B", "y", "INNER")
        jc2 = JoinCondition("B", "y", "A", "x", "INNER")
        
        self.assertTrue(jc1.is_equivalent_to(jc2))

@unittest.skipUnless(Z3_AVAILABLE, "Z3 not available")
class TestVariableCollision(unittest.TestCase):
    """Tests for variable collision fix (users.id vs orders.id)"""
    
    def setUp(self):
        self.checker = Z3JoinContainmentChecker()
    
    def test_same_column_different_tables_not_confused(self):
        """users.id and orders.id should be different Z3 variables"""
        # Cached: users.id = 1 (only filters users table)
        # New: orders.id = 1 (needs filter on orders table)
        # These should NOT be satisfied because cache doesn't have orders.id filter
        cached_tables = ["users", "orders"]
        new_tables = ["users", "orders"]
        cached_joins = [JoinCondition("users", "id", "orders", "user_id", "INNER")]
        new_joins = [JoinCondition("users", "id", "orders", "user_id", "INNER")]
        
        # Cached has filter on users.id, new has filter on orders.id
        # Cached only has users with id=1, but new wants orders with id=1
        # Since cached has no filter on orders table, it should be able to 
        # serve the new query with an additional filter on orders.id
        cached_where = {
            "users": PredicateGroup(predicates=[
                Predicate("id", PredicateOperator.EQ, 1, table="users")
            ])
        }
        new_where = {
            "users": PredicateGroup(predicates=[
                Predicate("id", PredicateOperator.EQ, 1, table="users")  # Same as cached
            ]),
            "orders": PredicateGroup(predicates=[
                Predicate("id", PredicateOperator.EQ, 1, table="orders")  # Additional filter
            ])
        }
        
        is_contained, filter_sql = self.checker.check_join_containment(
            cached_tables, new_tables,
            cached_joins, new_joins,
            cached_where, new_where
        )
        
        # This SHOULD be contained because:
        # - users.id = 1 on both (exact match)
        # - orders.id = 1 is an additional filter on cached data (which has all orders)
        self.assertTrue(is_contained)
        # New should apply orders.id = 1 as additional filter
        self.assertIn("id", filter_sql)
    
    def test_different_tables_different_id_predicates(self):
        """Stricter 'id' on different tables should be independent"""
        cached_tables = ["users", "orders"]
        new_tables = ["users", "orders"]
        cached_joins = [JoinCondition("users", "id", "orders", "user_id", "INNER")]
        new_joins = [JoinCondition("users", "id", "orders", "user_id", "INNER")]
        
        # Cached: users.id > 10
        # New: users.id > 20 (stricter on same table - should be contained)
        cached_where = {
            "users": PredicateGroup(predicates=[
                Predicate("id", PredicateOperator.GT, 10, table="users")
            ])
        }
        new_where = {
            "users": PredicateGroup(predicates=[
                Predicate("id", PredicateOperator.GT, 20, table="users")
            ])
        }
        
        is_contained, filter_sql = self.checker.check_join_containment(
            cached_tables, new_tables,
            cached_joins, new_joins,
            cached_where, new_where
        )
        
        self.assertTrue(is_contained)


if __name__ == "__main__":
    unittest.main(verbosity=2)
