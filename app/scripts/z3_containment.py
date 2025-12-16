"""
Z3-Based Query Containment Checker for Semantic Cache

Uses Z3 SMT solver to verify if a new query's results are contained within
a cached query's results. Inspired by VeriEQL's approach to SQL equivalence.

Containment Logic:
    For cache hit: ∀ tuple: satisfies(new_predicates) → satisfies(cached_predicates)
    
    Negation (what we check):
        ∃ tuple: satisfies(new_predicates) ∧ ¬satisfies(cached_predicates)
    
    UNSAT → Contained (new ⊆ cached) → Cache HIT
    SAT   → Counterexample exists    → Cache MISS
"""

import time
import logging
from typing import Optional, Tuple, Dict, Any, List
from dataclasses import dataclass

try:
    import z3
    Z3_AVAILABLE = True
except ImportError:
    Z3_AVAILABLE = False
    z3 = None

from query_parser import (
    Predicate,
    PredicateGroup,
    PredicateOperator,
    LogicalOperator
)

logger = logging.getLogger(__name__)


@dataclass
class ContainmentResult:
    """Result of a containment check"""
    is_contained: bool
    additional_filter: Optional[str]  # SQL filter to apply on cached data if contained
    check_time_ms: float
    solver_result: str  # "sat", "unsat", "unknown"
    

class Z3QueryEncoder:
    """
    Encodes SQL predicates as Z3 constraints.
    
    Maps SQL types to Z3 sorts:
    - Numeric columns → z3.Int or z3.Real
    - String columns → z3.String
    - Boolean columns → z3.Bool
    
    Maps SQL operators to Z3 operations:
    - =, !=, >, <, >=, <= → arithmetic comparisons
    - IN → z3.Or of equalities
    - BETWEEN → z3.And of range bounds
    - AND/OR → z3.And/z3.Or
    """
    
    def __init__(self):
        if not Z3_AVAILABLE:
            raise ImportError("z3-solver is not installed. Run: pip install z3-solver")
        
        # Cache for Z3 variables by (attribute, sort)
        self._variables: Dict[str, z3.ExprRef] = {}
        
    def _get_variable(self, name: str, value: Any = None) -> z3.ExprRef:
        """
        Get or create a Z3 variable for an attribute.
        Infers sort from the value if provided.
        """
        name_lower = name.lower()
        
        if name_lower in self._variables:
            return self._variables[name_lower]
        
        # Infer sort from value type
        if value is None:
            # Default to Int if no value provided
            var = z3.Int(name_lower)
        elif isinstance(value, bool):
            var = z3.Bool(name_lower)
        elif isinstance(value, int):
            var = z3.Int(name_lower)
        elif isinstance(value, float):
            var = z3.Real(name_lower)
        elif isinstance(value, str):
            var = z3.String(name_lower)
        elif isinstance(value, (list, tuple)):
            # For IN/BETWEEN, infer from first element
            if value:
                return self._get_variable(name, value[0])
            var = z3.Int(name_lower)
        else:
            var = z3.Int(name_lower)
        
        self._variables[name_lower] = var
        return var
    
    def _value_to_z3(self, value: Any, var: z3.ExprRef) -> z3.ExprRef:
        """Convert a Python value to Z3 value matching the variable's sort."""
        if z3.is_int(var):
            return z3.IntVal(int(value))
        elif z3.is_real(var):
            return z3.RealVal(float(value))
        elif z3.is_string(var):
            return z3.StringVal(str(value))
        elif z3.is_bool(var):
            return z3.BoolVal(bool(value))
        else:
            return z3.IntVal(int(value))
    
    def encode_predicate(self, pred: Predicate) -> z3.BoolRef:
        """
        Encode a single SQL predicate as a Z3 constraint.
        
        Examples:
            Age > 40  →  z3.Int('age') > 40
            Name = 'John'  →  z3.String('name') == StringVal('John')
            ID IN (1,2,3)  →  z3.Or(id == 1, id == 2, id == 3)
        """
        var = self._get_variable(pred.attribute, pred.value)
        op = pred.operator
        val = pred.value
        
        if op == PredicateOperator.EQ:
            return var == self._value_to_z3(val, var)
        
        elif op == PredicateOperator.NEQ:
            return var != self._value_to_z3(val, var)
        
        elif op == PredicateOperator.GT:
            return var > self._value_to_z3(val, var)
        
        elif op == PredicateOperator.LT:
            return var < self._value_to_z3(val, var)
        
        elif op == PredicateOperator.GTE:
            return var >= self._value_to_z3(val, var)
        
        elif op == PredicateOperator.LTE:
            return var <= self._value_to_z3(val, var)
        
        elif op == PredicateOperator.IN:
            # IN (v1, v2, v3) → Or(var == v1, var == v2, var == v3)
            if not val:
                return z3.BoolVal(False)  # Empty IN is always false
            clauses = [var == self._value_to_z3(v, var) for v in val]
            return z3.Or(*clauses)
        
        elif op == PredicateOperator.NOT_IN:
            # NOT IN (v1, v2, v3) → And(var != v1, var != v2, var != v3)
            if not val:
                return z3.BoolVal(True)  # Empty NOT IN is always true
            clauses = [var != self._value_to_z3(v, var) for v in val]
            return z3.And(*clauses)
        
        elif op == PredicateOperator.BETWEEN:
            # BETWEEN low AND high → And(var >= low, var <= high)
            low, high = val
            return z3.And(
                var >= self._value_to_z3(low, var),
                var <= self._value_to_z3(high, var)
            )
        
        elif op == PredicateOperator.LIKE:
            # LIKE patterns - simplified handling
            # For now, treat as string containment for simple patterns
            if isinstance(val, str):
                if val.startswith('%') and val.endswith('%'):
                    # %pattern% → Contains
                    pattern = val[1:-1]
                    return z3.Contains(var, z3.StringVal(pattern))
                elif val.endswith('%'):
                    # pattern% → PrefixOf
                    pattern = val[:-1]
                    return z3.PrefixOf(z3.StringVal(pattern), var)
                elif val.startswith('%'):
                    # %pattern → SuffixOf
                    pattern = val[1:]
                    return z3.SuffixOf(z3.StringVal(pattern), var)
                else:
                    # No wildcards → equality
                    return var == z3.StringVal(val)
            return z3.BoolVal(True)  # Fallback
        
        elif op == PredicateOperator.IS_NULL:
            # Model NULL as a special value - use a separate boolean flag
            null_var = z3.Bool(f"{pred.attribute.lower()}_is_null")
            return null_var
        
        elif op == PredicateOperator.IS_NOT_NULL:
            null_var = z3.Bool(f"{pred.attribute.lower()}_is_null")
            return z3.Not(null_var)
        
        else:
            logger.warning(f"Unknown operator {op}, treating as true")
            return z3.BoolVal(True)
    
    def encode_predicate_group(self, group: PredicateGroup) -> z3.BoolRef:
        """
        Encode a group of predicates (with AND/OR) as Z3 constraints.
        
        Examples:
            Age > 40 AND Status = 'Active'  →  z3.And(age > 40, status == 'Active')
            Age > 50 OR VIP = true          →  z3.Or(age > 50, vip == True)
        """
        if group.is_empty():
            return z3.BoolVal(True)  # Empty predicates = no constraint
        
        # Encode individual predicates
        pred_constraints = [self.encode_predicate(p) for p in group.predicates]
        
        # Encode subgroups recursively
        subgroup_constraints = [self.encode_predicate_group(sg) for sg in group.subgroups]
        
        all_constraints = pred_constraints + subgroup_constraints
        
        if not all_constraints:
            return z3.BoolVal(True)
        
        if len(all_constraints) == 1:
            return all_constraints[0]
        
        # Combine with AND or OR
        if group.operator == LogicalOperator.AND:
            return z3.And(*all_constraints)
        else:  # OR
            return z3.Or(*all_constraints)
    
    def reset(self):
        """Reset the variable cache for a new encoding session."""
        self._variables.clear()


class Z3ContainmentChecker:
    """
    Checks if a new query's results are contained within cached query's results
    using Z3 SMT solver.
    
    Containment is verified by checking if:
        ∃ tuple: satisfies(new) ∧ ¬satisfies(cached)
    
    If this is UNSAT, then new ⊆ cached (containment holds).
    If SAT, then there exists a tuple that new matches but cached doesn't.
    """
    
    def __init__(self, timeout_ms: int = 1000):
        """
        Initialize the containment checker.
        
        Args:
            timeout_ms: Z3 solver timeout in milliseconds (default 1000ms)
        """
        if not Z3_AVAILABLE:
            raise ImportError("z3-solver is not installed. Run: pip install z3-solver")
        
        self.timeout_ms = timeout_ms
        self._encoder = Z3QueryEncoder()
    
    def is_contained(
        self, 
        cached_predicates: PredicateGroup, 
        new_predicates: PredicateGroup
    ) -> Tuple[bool, Optional[str]]:
        """
        Check if new query results are contained in cached query results.
        
        Args:
            cached_predicates: Predicates from the cached query
            new_predicates: Predicates from the new query
            
        Returns:
            (is_contained, additional_filter_sql)
            - is_contained: True if new ⊆ cached
            - additional_filter_sql: SQL filter to apply on cached data (if contained)
        """
        start_time = time.time()
        
        # Reset encoder for fresh variable namespace
        self._encoder.reset()
        
        # Handle empty cases
        if cached_predicates.is_empty():
            # Cached has no predicates (all data) → can always filter
            if new_predicates.is_empty():
                return True, None
            else:
                return True, new_predicates.to_sql()
        
        if new_predicates.is_empty():
            # New wants all data, cached has restrictions → cannot serve
            return False, None
        
        # Encode predicates to Z3
        cached_z3 = self._encoder.encode_predicate_group(cached_predicates)
        new_z3 = self._encoder.encode_predicate_group(new_predicates)
        
        # Create solver
        solver = z3.Solver()
        solver.set("timeout", self.timeout_ms)
        
        # Check: ∃ tuple: satisfies(new) ∧ ¬satisfies(cached)
        # If UNSAT → new ⊆ cached (containment holds)
        containment_check = z3.And(new_z3, z3.Not(cached_z3))
        solver.add(containment_check)
        
        result = solver.check()
        check_time = (time.time() - start_time) * 1000
        
        if result == z3.unsat:
            # Containment holds - new is a subset of cached
            # Need to determine the additional filter to apply
            additional_filter = self._compute_additional_filter(cached_predicates, new_predicates)
            
            logger.info(f"Z3 containment CHECK: UNSAT (contained), {check_time:.2f}ms")
            return True, additional_filter
        
        elif result == z3.sat:
            # Counterexample exists - containment does not hold
            logger.info(f"Z3 containment CHECK: SAT (not contained), {check_time:.2f}ms")
            return False, None
        
        else:  # unknown (timeout or other)
            logger.warning(f"Z3 containment CHECK: UNKNOWN, {check_time:.2f}ms")
            # Conservative: assume not contained on timeout
            return False, None
    
    def _compute_additional_filter(
        self, 
        cached_predicates: PredicateGroup, 
        new_predicates: PredicateGroup
    ) -> Optional[str]:
        """
        Compute the SQL filter to apply on cached data to get new query's results.
        
        If new is contained in cached, but new has additional/tighter constraints,
        we need to filter the cached data.
        """
        if new_predicates.is_empty():
            return None
        
        if cached_predicates.is_empty():
            # Cached has all data, apply new's filter directly
            return new_predicates.to_sql()
        
        # Check if predicates are identical (exact match)
        if self._predicates_equal(cached_predicates, new_predicates):
            return None
        
        # New has additional constraints - return new's predicates as filter
        return new_predicates.to_sql()
    
    def _predicates_equal(
        self, 
        p1: PredicateGroup, 
        p2: PredicateGroup
    ) -> bool:
        """
        Check if two predicate groups are semantically equivalent.
        Uses Z3 to verify equivalence.
        """
        self._encoder.reset()
        
        z1 = self._encoder.encode_predicate_group(p1)
        z2 = self._encoder.encode_predicate_group(p2)
        
        solver = z3.Solver()
        solver.set("timeout", self.timeout_ms)
        
        # Check if ∃ tuple: (p1 ∧ ¬p2) ∨ (¬p1 ∧ p2)
        # If UNSAT → p1 ⟺ p2
        difference = z3.Or(
            z3.And(z1, z3.Not(z2)),
            z3.And(z3.Not(z1), z2)
        )
        solver.add(difference)
        
        return solver.check() == z3.unsat


# Module-level functions for easy import

def check_containment(
    cached_predicates: PredicateGroup,
    new_predicates: PredicateGroup,
    timeout_ms: int = 1000
) -> Tuple[bool, Optional[str]]:
    """
    Convenience function to check query containment.
    
    Args:
        cached_predicates: Predicates from cached query
        new_predicates: Predicates from new query
        timeout_ms: Solver timeout in milliseconds
        
    Returns:
        (is_contained, additional_filter_sql)
    """
    checker = Z3ContainmentChecker(timeout_ms=timeout_ms)
    return checker.is_contained(cached_predicates, new_predicates)


def is_z3_available() -> bool:
    """Check if Z3 is available."""
    return Z3_AVAILABLE


# Test the module
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    if not Z3_AVAILABLE:
        print("Z3 not installed. Run: pip install z3-solver")
        exit(1)
    
    print("Testing Z3 Containment Checker\n" + "=" * 40)
    
    checker = Z3ContainmentChecker()
    
    # Test 1: Age > 50 ⊆ Age > 40 → True
    print("\nTest 1: Age > 50 ⊆ Age > 40")
    cached = PredicateGroup(predicates=[Predicate("Age", PredicateOperator.GT, 40)])
    new = PredicateGroup(predicates=[Predicate("Age", PredicateOperator.GT, 50)])
    is_contained, filter_sql = checker.is_contained(cached, new)
    print(f"  Contained: {is_contained} (expected: True)")
    print(f"  Filter: {filter_sql}")
    
    # Test 2: Age > 30 ⊆ Age > 40 → False
    print("\nTest 2: Age > 30 ⊆ Age > 40")
    new = PredicateGroup(predicates=[Predicate("Age", PredicateOperator.GT, 30)])
    is_contained, filter_sql = checker.is_contained(cached, new)
    print(f"  Contained: {is_contained} (expected: False)")
    
    # Test 3: Age = 50 ⊆ Age > 40 → True
    print("\nTest 3: Age = 50 ⊆ Age > 40")
    new = PredicateGroup(predicates=[Predicate("Age", PredicateOperator.EQ, 50)])
    is_contained, filter_sql = checker.is_contained(cached, new)
    print(f"  Contained: {is_contained} (expected: True)")
    print(f"  Filter: {filter_sql}")
    
    # Test 4: ID IN (1,2) ⊆ ID IN (1,2,3) → True
    print("\nTest 4: ID IN (1,2) ⊆ ID IN (1,2,3)")
    cached = PredicateGroup(predicates=[Predicate("ID", PredicateOperator.IN, [1, 2, 3])])
    new = PredicateGroup(predicates=[Predicate("ID", PredicateOperator.IN, [1, 2])])
    is_contained, filter_sql = checker.is_contained(cached, new)
    print(f"  Contained: {is_contained} (expected: True)")
    print(f"  Filter: {filter_sql}")
    
    # Test 5: (Age > 50 OR Status = 'VIP') ⊆ (Age > 40 OR Status = 'VIP') → True
    print("\nTest 5: (Age > 50 OR Status='VIP') ⊆ (Age > 40 OR Status='VIP')")
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
    is_contained, filter_sql = checker.is_contained(cached, new)
    print(f"  Contained: {is_contained} (expected: True)")
    print(f"  Filter: {filter_sql}")
    
    # Test 6: BETWEEN containment
    print("\nTest 6: Age BETWEEN 20 AND 60 ⊆ Age BETWEEN 10 AND 80")
    cached = PredicateGroup(predicates=[Predicate("Age", PredicateOperator.BETWEEN, (10, 80))])
    new = PredicateGroup(predicates=[Predicate("Age", PredicateOperator.BETWEEN, (20, 60))])
    is_contained, filter_sql = checker.is_contained(cached, new)
    print(f"  Contained: {is_contained} (expected: True)")
    print(f"  Filter: {filter_sql}")
    
    print("\n" + "=" * 40)
    print("Tests completed!")
