"""
Query Parser for Semantic Cache
Parses SQL queries into structured components for cache key generation and subset detection.

Supports:
- SELECT queries with WHERE clauses
- Point queries (=, !=)
- Range queries (>, <, >=, <=)
- IN clauses
- BETWEEN clauses
- AND/OR combinations
- JOIN queries
- Aggregations (COUNT, SUM, AVG, MIN, MAX)
- GROUP BY, ORDER BY, LIMIT
"""

import re
import hashlib
import json
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass, field, asdict
from enum import Enum


class PredicateOperator(str, Enum):
    EQ = "="
    NEQ = "!="
    GT = ">"
    LT = "<"
    GTE = ">="
    LTE = "<="
    IN = "IN"
    NOT_IN = "NOT IN"
    BETWEEN = "BETWEEN"
    LIKE = "LIKE"
    IS_NULL = "IS NULL"
    IS_NOT_NULL = "IS NOT NULL"


class LogicalOperator(str, Enum):
    AND = "AND"
    OR = "OR"


@dataclass
class Predicate:
    """Represents a single predicate condition"""
    attribute: str
    operator: PredicateOperator
    value: Any  # Can be single value, list (for IN), or tuple (for BETWEEN)
    table: Optional[str] = None  # For JOIN queries
    
    def to_dict(self) -> dict:
        return {
            "attribute": self.attribute.lower(),
            "operator": self.operator.value if isinstance(self.operator, PredicateOperator) else self.operator,
            "value": self.value,
            "table": self.table.lower() if self.table else None
        }
    
    def to_sql(self) -> str:
        """Convert predicate back to SQL string"""
        attr = f"{self.table}.{self.attribute}" if self.table else self.attribute
        
        if self.operator == PredicateOperator.IN:
            values = ", ".join([f"'{v}'" if isinstance(v, str) else str(v) for v in self.value])
            return f"{attr} IN ({values})"
        elif self.operator == PredicateOperator.BETWEEN:
            return f"{attr} BETWEEN {self.value[0]} AND {self.value[1]}"
        elif self.operator in (PredicateOperator.IS_NULL, PredicateOperator.IS_NOT_NULL):
            return f"{attr} {self.operator.value}"
        elif isinstance(self.value, str):
            return f"{attr} {self.operator.value} '{self.value}'"
        else:
            return f"{attr} {self.operator.value} {self.value}"


@dataclass
class PredicateGroup:
    """Represents a group of predicates combined with AND/OR"""
    predicates: List[Predicate] = field(default_factory=list)
    operator: LogicalOperator = LogicalOperator.AND
    subgroups: List['PredicateGroup'] = field(default_factory=list)
    
    def to_dict(self) -> dict:
        return {
            "predicates": [p.to_dict() for p in self.predicates],
            "operator": self.operator.value,
            "subgroups": [sg.to_dict() for sg in self.subgroups]
        }
    
    def is_empty(self) -> bool:
        return len(self.predicates) == 0 and len(self.subgroups) == 0
    
    def to_sql(self) -> str:
        """Convert predicate group back to SQL WHERE clause"""
        parts = []
        for pred in self.predicates:
            parts.append(pred.to_sql())
        for subgroup in self.subgroups:
            sub_sql = subgroup.to_sql()
            if sub_sql:
                parts.append(f"({sub_sql})")
        
        return f" {self.operator.value} ".join(parts)


@dataclass 
class JoinCondition:
    """Represents a JOIN condition"""
    left_table: str
    left_column: str
    right_table: str
    right_column: str
    join_type: str = "INNER"  # INNER, LEFT, RIGHT, FULL
    
    def to_dict(self) -> dict:
        return {
            "left_table": self.left_table.lower(),
            "left_column": self.left_column.lower(),
            "right_table": self.right_table.lower(),
            "right_column": self.right_column.lower(),
            "join_type": self.join_type.upper()
        }


@dataclass
class ParsedQuery:
    """Complete parsed query structure"""
    original_query: str
    normalized_query: str
    query_type: str  # SELECT, INSERT, UPDATE, DELETE
    tables: List[str]
    columns: List[str]  # ["*"] for SELECT *
    predicates: PredicateGroup  # Combined predicates (CTE + outer)
    join_conditions: List[JoinCondition]
    group_by: List[str]
    order_by: List[Tuple[str, str]]  # [(column, ASC/DESC), ...]
    limit: Optional[int]
    offset: Optional[int]
    aggregations: List[str]  # ["COUNT(*)", "AVG(Age)", ...]
    has_cte: bool  # Common Table Expression (WITH clause)
    cte_name: Optional[str]
    cte_predicates: PredicateGroup = field(default_factory=PredicateGroup)  # Access control predicates from CTE
    outer_predicates: PredicateGroup = field(default_factory=PredicateGroup)  # User filter predicates from outer WHERE
    
    def to_dict(self) -> dict:
        return {
            "query_type": self.query_type,
            "tables": [t.lower() for t in self.tables],
            "columns": [c.lower() for c in self.columns],
            "predicates": self.predicates.to_dict(),
            "join_conditions": [jc.to_dict() for jc in self.join_conditions],
            "group_by": [g.lower() for g in self.group_by],
            "order_by": [(c.lower(), d) for c, d in self.order_by],
            "limit": self.limit,
            "offset": self.offset,
            "aggregations": self.aggregations,
            "has_cte": self.has_cte
        }
    
    def generate_signature(self) -> str:
        """Generate a unique signature for cache key (includes all predicates)"""
        sig_dict = self.to_dict()
        # Sort for consistent hashing
        sig_str = json.dumps(sig_dict, sort_keys=True)
        return hashlib.sha256(sig_str.encode()).hexdigest()[:32]
    
    def generate_base_signature(self) -> str:
        """Generate a base signature for cache lookup (CTE/access control predicates only).
        
        This allows queries with different user filters to match the same cache entry,
        enabling subset detection and filtering on cached data.
        """
        sig_dict = {
            "query_type": self.query_type,
            "tables": [t.lower() for t in self.tables],
            "columns": [c.lower() for c in self.columns],
            "cte_predicates": self.cte_predicates.to_dict() if not self.cte_predicates.is_empty() else self.predicates.to_dict(),
            "join_conditions": [jc.to_dict() for jc in self.join_conditions],
            "has_cte": self.has_cte
        }
        sig_str = json.dumps(sig_dict, sort_keys=True)
        return hashlib.sha256(sig_str.encode()).hexdigest()[:32]
    
    def get_cache_key(self, table_name: str) -> str:
        """Generate cache key for this query"""
        return f"{table_name}_{self.generate_signature()}"


class QueryParser:
    """
    SQL Query Parser for semantic cache.
    Extracts structured information from SQL queries.
    """
    
    # Regex patterns
    WHITESPACE_PATTERN = re.compile(r'\s+')
    SELECT_PATTERN = re.compile(r'SELECT\s+(DISTINCT\s+)?(.*?)\s+FROM', re.IGNORECASE | re.DOTALL)
    FROM_PATTERN = re.compile(r'FROM\s+([^\s,]+(?:\s*,\s*[^\s,]+)*)', re.IGNORECASE)
    WHERE_PATTERN = re.compile(r'WHERE\s+(.+?)(?:GROUP BY|ORDER BY|LIMIT|$)', re.IGNORECASE | re.DOTALL)
    JOIN_PATTERN = re.compile(r'(INNER|LEFT|RIGHT|FULL|CROSS)?\s*JOIN\s+(\w+)\s+(?:AS\s+)?(\w+)?\s*ON\s+(.+?)(?=(?:INNER|LEFT|RIGHT|FULL|CROSS)?\s*JOIN|WHERE|GROUP BY|ORDER BY|LIMIT|$)', re.IGNORECASE | re.DOTALL)
    GROUP_BY_PATTERN = re.compile(r'GROUP\s+BY\s+(.+?)(?:HAVING|ORDER BY|LIMIT|$)', re.IGNORECASE | re.DOTALL)
    ORDER_BY_PATTERN = re.compile(r'ORDER\s+BY\s+(.+?)(?:LIMIT|OFFSET|$)', re.IGNORECASE | re.DOTALL)
    LIMIT_PATTERN = re.compile(r'LIMIT\s+(\d+)', re.IGNORECASE)
    OFFSET_PATTERN = re.compile(r'OFFSET\s+(\d+)', re.IGNORECASE)
    CTE_PATTERN = re.compile(r'WITH\s+(\w+)\s+AS\s*\((.+?)\)\s*SELECT', re.IGNORECASE | re.DOTALL)
    
    # Aggregation functions
    AGG_PATTERN = re.compile(r'(COUNT|SUM|AVG|MIN|MAX|TOTAL)\s*\([^)]+\)', re.IGNORECASE)
    
    def __init__(self):
        pass
    
    def normalize_query(self, query: str) -> str:
        """Normalize query for consistent comparison"""
        # Remove extra whitespace
        normalized = self.WHITESPACE_PATTERN.sub(' ', query.strip())
        # Normalize case for keywords (keep values as-is for now)
        return normalized
    
    def parse(self, query: str) -> ParsedQuery:
        """Parse a SQL query into structured components"""
        normalized = self.normalize_query(query)
        
        # Determine query type
        query_upper = normalized.upper().strip()
        if query_upper.startswith('SELECT') or query_upper.startswith('WITH'):
            query_type = 'SELECT'
        elif query_upper.startswith('INSERT'):
            query_type = 'INSERT'
        elif query_upper.startswith('UPDATE'):
            query_type = 'UPDATE'
        elif query_upper.startswith('DELETE'):
            query_type = 'DELETE'
        else:
            query_type = 'UNKNOWN'
        
        # Parse CTE (WITH clause)
        has_cte = False
        cte_name = None
        cte_match = self.CTE_PATTERN.search(normalized)
        if cte_match:
            has_cte = True
            cte_name = cte_match.group(1)
        
        # Parse SELECT columns
        columns = self._parse_columns(normalized)
        
        # Parse tables (FROM clause)
        tables = self._parse_tables(normalized)
        
        # Parse JOIN conditions
        join_conditions = self._parse_joins(normalized)
        
        # Parse WHERE predicates (returns combined, cte_predicates, outer_predicates)
        predicates, cte_predicates, outer_predicates = self._parse_where(normalized)
        
        # Parse GROUP BY
        group_by = self._parse_group_by(normalized)
        
        # Parse ORDER BY
        order_by = self._parse_order_by(normalized)
        
        # Parse LIMIT/OFFSET
        limit = self._parse_limit(normalized)
        offset = self._parse_offset(normalized)
        
        # Extract aggregations
        aggregations = self._parse_aggregations(normalized)
        
        return ParsedQuery(
            original_query=query,
            normalized_query=normalized,
            query_type=query_type,
            tables=tables,
            columns=columns,
            predicates=predicates,
            join_conditions=join_conditions,
            group_by=group_by,
            order_by=order_by,
            limit=limit,
            offset=offset,
            aggregations=aggregations,
            has_cte=has_cte,
            cte_name=cte_name,
            cte_predicates=cte_predicates,
            outer_predicates=outer_predicates
        )
    
    def _parse_columns(self, query: str) -> List[str]:
        """Extract SELECT columns"""
        match = self.SELECT_PATTERN.search(query)
        if not match:
            return ["*"]
        
        columns_str = match.group(2).strip()
        if columns_str == '*':
            return ["*"]
        
        # Handle column expressions
        columns = []
        depth = 0
        current = ""
        for char in columns_str:
            if char == '(':
                depth += 1
                current += char
            elif char == ')':
                depth -= 1
                current += char
            elif char == ',' and depth == 0:
                columns.append(current.strip())
                current = ""
            else:
                current += char
        if current.strip():
            columns.append(current.strip())
        
        return columns
    
    def _parse_tables(self, query: str) -> List[str]:
        """Extract table names from FROM clause"""
        match = self.FROM_PATTERN.search(query)
        if not match:
            return []
        
        tables_str = match.group(1)
        # Handle table aliases
        tables = []
        for table in tables_str.split(','):
            table = table.strip()
            # Remove alias (e.g., "patient_data p" -> "patient_data")
            parts = table.split()
            if parts:
                tables.append(parts[0])
        
        return tables
    
    def _parse_joins(self, query: str) -> List[JoinCondition]:
        """Extract JOIN conditions"""
        joins = []
        for match in self.JOIN_PATTERN.finditer(query):
            join_type = match.group(1) or "INNER"
            right_table = match.group(2)
            on_clause = match.group(4).strip()
            
            # Parse ON clause (e.g., "p.HospitalID = h.HospitalID")
            on_match = re.search(r'(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)', on_clause)
            if on_match:
                joins.append(JoinCondition(
                    left_table=on_match.group(1),
                    left_column=on_match.group(2),
                    right_table=on_match.group(3),
                    right_column=on_match.group(4),
                    join_type=join_type.upper()
                ))
        
        return joins
    
    def _parse_where(self, query: str) -> PredicateGroup:
        """Extract WHERE clause predicates.
        
        For CTE queries (WITH ... AS (...) SELECT ... WHERE ...):
        - Extract predicates from BOTH the CTE's WHERE clause AND the outer query's WHERE clause
        - Combine them with AND (both must be satisfied)
        """
        # Check if this is a CTE query
        cte_pattern = re.compile(r'WITH\s+(\w+)\s+AS\s*\((.+?)\)\s*(SELECT.+)', re.IGNORECASE | re.DOTALL)
        cte_match = cte_pattern.search(query)
        
        if cte_match:
            # CTE query: extract predicates from both inner and outer queries
            cte_body = cte_match.group(2)
            outer_query = cte_match.group(3)
            
            # Get predicates from CTE body
            cte_where_match = self.WHERE_PATTERN.search(cte_body)
            cte_predicates = PredicateGroup()
            if cte_where_match:
                cte_where_clause = cte_where_match.group(1).strip()
                cte_predicates = self._parse_predicate_group(cte_where_clause)
            
            # Get predicates from outer query
            outer_where_match = self.WHERE_PATTERN.search(outer_query)
            outer_predicates = PredicateGroup()
            if outer_where_match:
                outer_where_clause = outer_where_match.group(1).strip()
                outer_predicates = self._parse_predicate_group(outer_where_clause)
            
            # Combine both predicate groups
            if cte_predicates.is_empty():
                return outer_predicates, cte_predicates, outer_predicates
            elif outer_predicates.is_empty():
                return cte_predicates, cte_predicates, outer_predicates
            else:
                # Combine with AND - both CTE and outer predicates must be satisfied
                combined = PredicateGroup(operator=LogicalOperator.AND)
                combined.subgroups.append(cte_predicates)
                combined.subgroups.append(outer_predicates)
                return combined, cte_predicates, outer_predicates
        
        # Non-CTE query: use existing logic
        match = self.WHERE_PATTERN.search(query)
        if not match:
            return PredicateGroup(), PredicateGroup(), PredicateGroup()
        
        where_clause = match.group(1).strip()
        predicates = self._parse_predicate_group(where_clause)
        return predicates, predicates, PredicateGroup()  # All predicates are "CTE" predicates for non-CTE queries
    
    def _parse_predicate_group(self, clause: str) -> PredicateGroup:
        """Parse a predicate clause into structured predicates"""
        group = PredicateGroup()
        
        if not clause:
            return group
        
        # Check for OR at top level (outside parentheses)
        or_parts = self._split_by_operator(clause, 'OR')
        if len(or_parts) > 1:
            group.operator = LogicalOperator.OR
            for part in or_parts:
                subgroup = self._parse_predicate_group(part.strip())
                if not subgroup.is_empty():
                    if len(subgroup.predicates) == 1 and len(subgroup.subgroups) == 0:
                        group.predicates.append(subgroup.predicates[0])
                    else:
                        group.subgroups.append(subgroup)
            return group
        
        # Check for AND
        and_parts = self._split_by_operator(clause, 'AND')
        if len(and_parts) > 1:
            group.operator = LogicalOperator.AND
            for part in and_parts:
                part = part.strip()
                # Check for nested parentheses
                if part.startswith('(') and part.endswith(')'):
                    subgroup = self._parse_predicate_group(part[1:-1])
                    if not subgroup.is_empty():
                        group.subgroups.append(subgroup)
                else:
                    pred = self._parse_single_predicate(part)
                    if pred:
                        group.predicates.append(pred)
            return group
        
        # Single predicate
        clause = clause.strip()
        if clause.startswith('(') and clause.endswith(')'):
            return self._parse_predicate_group(clause[1:-1])
        
        pred = self._parse_single_predicate(clause)
        if pred:
            group.predicates.append(pred)
        
        return group
    
    def _split_by_operator(self, clause: str, operator: str) -> List[str]:
        """Split clause by operator, respecting parentheses"""
        parts = []
        depth = 0
        current = ""
        i = 0
        op_len = len(operator)
        
        while i < len(clause):
            char = clause[i]
            
            if char == '(':
                depth += 1
                current += char
            elif char == ')':
                depth -= 1
                current += char
            elif depth == 0 and clause[i:i+op_len].upper() == operator:
                # Check it's a word boundary
                before_ok = (i == 0 or not clause[i-1].isalnum())
                after_ok = (i + op_len >= len(clause) or not clause[i+op_len].isalnum())
                if before_ok and after_ok:
                    if current.strip():
                        parts.append(current.strip())
                    current = ""
                    i += op_len
                    continue
                else:
                    current += char
            else:
                current += char
            i += 1
        
        if current.strip():
            parts.append(current.strip())
        
        return parts if len(parts) > 1 else [clause]
    
    def _parse_single_predicate(self, expr: str) -> Optional[Predicate]:
        """Parse a single predicate expression"""
        expr = expr.strip()
        if not expr:
            return None
        
        # Remove surrounding parentheses
        while expr.startswith('(') and expr.endswith(')'):
            expr = expr[1:-1].strip()
        
        # Handle IS NULL / IS NOT NULL
        is_null_match = re.match(r'(\w+(?:\.\w+)?)\s+IS\s+(NOT\s+)?NULL', expr, re.IGNORECASE)
        if is_null_match:
            attr = is_null_match.group(1)
            op = PredicateOperator.IS_NOT_NULL if is_null_match.group(2) else PredicateOperator.IS_NULL
            table, column = self._split_table_column(attr)
            return Predicate(attribute=column, operator=op, value=None, table=table)
        
        # Handle BETWEEN
        between_match = re.match(r'(\w+(?:\.\w+)?)\s+BETWEEN\s+(.+?)\s+AND\s+(.+)', expr, re.IGNORECASE)
        if between_match:
            attr = between_match.group(1)
            low = self._parse_value(between_match.group(2).strip())
            high = self._parse_value(between_match.group(3).strip())
            table, column = self._split_table_column(attr)
            return Predicate(attribute=column, operator=PredicateOperator.BETWEEN, value=(low, high), table=table)
        
        # Handle IN / NOT IN
        in_match = re.match(r'(\w+(?:\.\w+)?)\s+(NOT\s+)?IN\s*\((.+?)\)', expr, re.IGNORECASE)
        if in_match:
            attr = in_match.group(1)
            op = PredicateOperator.NOT_IN if in_match.group(2) else PredicateOperator.IN
            values_str = in_match.group(3)
            values = [self._parse_value(v.strip()) for v in values_str.split(',')]
            table, column = self._split_table_column(attr)
            return Predicate(attribute=column, operator=op, value=values, table=table)
        
        # Handle LIKE
        like_match = re.match(r'(\w+(?:\.\w+)?)\s+LIKE\s+[\'"](.+?)[\'"]', expr, re.IGNORECASE)
        if like_match:
            attr = like_match.group(1)
            value = like_match.group(2)
            table, column = self._split_table_column(attr)
            return Predicate(attribute=column, operator=PredicateOperator.LIKE, value=value, table=table)
        
        # Handle comparison operators (>=, <=, !=, <>, =, >, <)
        comp_match = re.match(r'(\w+(?:\.\w+)?)\s*(>=|<=|!=|<>|=|>|<)\s*(.+)', expr)
        if comp_match:
            attr = comp_match.group(1)
            op_str = comp_match.group(2)
            value = self._parse_value(comp_match.group(3).strip())
            
            op_map = {
                '=': PredicateOperator.EQ,
                '!=': PredicateOperator.NEQ,
                '<>': PredicateOperator.NEQ,
                '>': PredicateOperator.GT,
                '<': PredicateOperator.LT,
                '>=': PredicateOperator.GTE,
                '<=': PredicateOperator.LTE
            }
            op = op_map.get(op_str, PredicateOperator.EQ)
            table, column = self._split_table_column(attr)
            return Predicate(attribute=column, operator=op, value=value, table=table)
        
        return None
    
    def _split_table_column(self, attr: str) -> Tuple[Optional[str], str]:
        """Split table.column into (table, column)"""
        if '.' in attr:
            parts = attr.split('.', 1)
            return parts[0], parts[1]
        return None, attr
    
    def _parse_value(self, value_str: str) -> Any:
        """Parse a value string into appropriate type"""
        value_str = value_str.strip()
        
        # Remove quotes
        if (value_str.startswith("'") and value_str.endswith("'")) or \
           (value_str.startswith('"') and value_str.endswith('"')):
            return value_str[1:-1]
        
        # Try integer
        try:
            return int(value_str)
        except ValueError:
            pass
        
        # Try float
        try:
            return float(value_str)
        except ValueError:
            pass
        
        # Return as string
        return value_str
    
    def _parse_group_by(self, query: str) -> List[str]:
        """Extract GROUP BY columns"""
        match = self.GROUP_BY_PATTERN.search(query)
        if not match:
            return []
        
        columns_str = match.group(1).strip()
        return [c.strip() for c in columns_str.split(',')]
    
    def _parse_order_by(self, query: str) -> List[Tuple[str, str]]:
        """Extract ORDER BY columns with direction"""
        match = self.ORDER_BY_PATTERN.search(query)
        if not match:
            return []
        
        columns_str = match.group(1).strip()
        result = []
        for part in columns_str.split(','):
            part = part.strip()
            if ' DESC' in part.upper():
                col = part.upper().replace(' DESC', '').strip()
                result.append((col, 'DESC'))
            else:
                col = part.upper().replace(' ASC', '').strip()
                result.append((col, 'ASC'))
        
        return result
    
    def _parse_limit(self, query: str) -> Optional[int]:
        """Extract LIMIT value"""
        match = self.LIMIT_PATTERN.search(query)
        return int(match.group(1)) if match else None
    
    def _parse_offset(self, query: str) -> Optional[int]:
        """Extract OFFSET value"""
        match = self.OFFSET_PATTERN.search(query)
        return int(match.group(1)) if match else None
    
    def _parse_aggregations(self, query: str) -> List[str]:
        """Extract aggregation functions"""
        return self.AGG_PATTERN.findall(query)


# Utility functions for cache operations

def predicates_are_subset(cached: PredicateGroup, new: PredicateGroup) -> Tuple[bool, Optional[str]]:
    """
    Check if new query predicates are a subset of cached predicates.
    Returns (is_subset, additional_filter_sql)
    
    If is_subset is True, the new query can be satisfied by filtering the cached results
    with additional_filter_sql.
    """
    # Empty cached predicates means we have all data
    if cached.is_empty():
        if new.is_empty():
            return True, None
        else:
            # New has predicates, cached has all data - can filter
            return True, new.to_sql()
    
    # If cached has predicates but new doesn't, we don't have all data
    if new.is_empty():
        return False, None
    
    # For AND predicates, check if each cached predicate is satisfied
    # and if new has additional predicates
    if cached.operator == LogicalOperator.AND and new.operator == LogicalOperator.AND:
        # All new predicates must be subsets of (or exactly match) some cached predicate
        additional_filters = []
        
        for new_pred in new.predicates:
            matched = False
            for cached_pred in cached.predicates:
                is_subset, extra = predicate_is_subset(cached_pred, new_pred)
                if is_subset:
                    matched = True
                    if extra:
                        additional_filters.append(extra)
                    break
            
            if not matched:
                # New predicate is NOT a subset of any cached predicate
                # This means we can't derive the result from cache
                return False, None
        
        # All new predicates are subsets of cached predicates
        if additional_filters:
            return True, " AND ".join(additional_filters)
        return True, None
    
    # For OR predicates, it's more complex - for now, require exact match
    # TODO: Implement OR subset detection
    
    return False, None


def predicate_is_subset(cached: Predicate, new: Predicate) -> Tuple[bool, Optional[str]]:
    """
    Check if a single new predicate is a subset of cached predicate.
    Returns (is_subset, additional_filter_sql)
    """
    # Must be same attribute
    if cached.attribute.lower() != new.attribute.lower():
        return False, None
    
    # Same operator and value = exact match
    if cached.operator == new.operator and cached.value == new.value:
        return True, None
    
    # Range subset checks
    c_op, c_val = cached.operator, cached.value
    n_op, n_val = new.operator, new.value
    
    # > subset: cached > 40 contains new > 50 (if 50 >= 40)
    if c_op == PredicateOperator.GT and n_op == PredicateOperator.GT:
        if n_val >= c_val:
            return True, new.to_sql() if n_val > c_val else None
    
    if c_op == PredicateOperator.GT and n_op == PredicateOperator.GTE:
        if n_val > c_val:
            return True, new.to_sql()
    
    if c_op == PredicateOperator.GTE and n_op == PredicateOperator.GTE:
        if n_val >= c_val:
            return True, new.to_sql() if n_val > c_val else None
    
    if c_op == PredicateOperator.GTE and n_op == PredicateOperator.GT:
        if n_val >= c_val:
            return True, new.to_sql()
    
    # < subset: cached < 60 contains new < 50 (if 50 <= 60)
    if c_op == PredicateOperator.LT and n_op == PredicateOperator.LT:
        if n_val <= c_val:
            return True, new.to_sql() if n_val < c_val else None
    
    if c_op == PredicateOperator.LT and n_op == PredicateOperator.LTE:
        if n_val < c_val:
            return True, new.to_sql()
    
    if c_op == PredicateOperator.LTE and n_op == PredicateOperator.LTE:
        if n_val <= c_val:
            return True, new.to_sql() if n_val < c_val else None
    
    if c_op == PredicateOperator.LTE and n_op == PredicateOperator.LT:
        if n_val <= c_val:
            return True, new.to_sql()
    
    # > contains =: cached > 40 contains new = 50 (if 50 > 40)
    if c_op == PredicateOperator.GT and n_op == PredicateOperator.EQ:
        if n_val > c_val:
            return True, new.to_sql()
    
    if c_op == PredicateOperator.GTE and n_op == PredicateOperator.EQ:
        if n_val >= c_val:
            return True, new.to_sql()
    
    if c_op == PredicateOperator.LT and n_op == PredicateOperator.EQ:
        if n_val < c_val:
            return True, new.to_sql()
    
    if c_op == PredicateOperator.LTE and n_op == PredicateOperator.EQ:
        if n_val <= c_val:
            return True, new.to_sql()
    
    # IN subset: cached IN (1,2,3) contains new IN (1,2)
    if c_op == PredicateOperator.IN and n_op == PredicateOperator.IN:
        if set(n_val).issubset(set(c_val)):
            return True, new.to_sql() if set(n_val) != set(c_val) else None
    
    # IN contains =: cached IN (1,2,3) contains new = 2
    if c_op == PredicateOperator.IN and n_op == PredicateOperator.EQ:
        if n_val in c_val:
            return True, new.to_sql()
    
    # BETWEEN subset
    if c_op == PredicateOperator.BETWEEN and n_op == PredicateOperator.BETWEEN:
        c_low, c_high = c_val
        n_low, n_high = n_val
        if n_low >= c_low and n_high <= c_high:
            return True, new.to_sql() if (n_low > c_low or n_high < c_high) else None
    
    return False, None


# Test the parser
if __name__ == "__main__":
    parser = QueryParser()
    
    test_queries = [
        "SELECT * FROM patient_data WHERE PatientID = 101",
        "SELECT * FROM patient_data WHERE Age > 40 AND HospitalID = 'HOSP-001'",
        "SELECT * FROM patient_data WHERE Age > 40 OR Condition = 'Diabetes'",
        "SELECT * FROM patient_data WHERE PatientID IN (1, 2, 3)",
        "SELECT * FROM patient_data WHERE Age BETWEEN 30 AND 50",
        "SELECT COUNT(*), AVG(Age) FROM patient_data WHERE HospitalID = 'HOSP-001' GROUP BY HospitalID",
        "WITH accessible_part AS (SELECT * FROM patient_data WHERE Age > 98) SELECT * FROM accessible_part",
    ]
    
    for query in test_queries:
        print(f"\nQuery: {query}")
        parsed = parser.parse(query)
        print(f"Tables: {parsed.tables}")
        print(f"Columns: {parsed.columns}")
        print(f"Predicates: {parsed.predicates.to_dict()}")
        print(f"Signature: {parsed.generate_signature()}")
