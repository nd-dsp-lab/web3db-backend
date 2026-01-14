"""
Utility Scorer for Semantic Sieve

Computes utility score for cache entries to determine the U (utility) bit
in the Semantic Sieve eviction algorithm.

Formula: Utility(Q) = (Cost / Size) × Retain(Q) × Breadth(Q)

For NSDI paper on Semantic Sieve.
"""

from dataclasses import dataclass, field
from typing import List
import logging

from query_parser import ParsedQuery, PredicateOperator, PredicateGroup

logger = logging.getLogger(__name__)


@dataclass
class UtilityConfig:
    """Configuration for utility scoring"""
    utility_threshold: float = 0.5  # Threshold for high-utility bit (U=1)
    cost_weight: float = 1.0  # Weight for cost component
    size_weight: float = 1.0  # Weight for size penalty
    min_cost_ms: float = 1.0  # Minimum cost to avoid division issues
    max_score: float = 100.0  # Cap score to avoid outliers


@dataclass  
class UtilityScorer:
    """
    Utility Scorer for Semantic Sieve
    
    Computes utility score based on:
    - Cost: Query execution time (higher = more valuable to cache)
    - Size: Memory footprint (higher = more costly to cache)
    - Retain: Information preservation (SELECT * vs aggregations)
    - Breadth: Containment probability (range vs point queries)
    
    Used to set the U (utility) bit in Semantic Sieve eviction.
    """
    config: UtilityConfig = field(default_factory=UtilityConfig)
    
    def compute_score(
        self, 
        parsed: ParsedQuery, 
        cost_ms: float, 
        size_bytes: int
    ) -> float:
        """
        Compute utility score for a query.
        
        Utility = (Cost / Size) × Retain × Breadth
        
        Higher score = higher utility, more protection from eviction.
        """
        # Avoid division by zero
        cost = max(cost_ms, self.config.min_cost_ms)
        size = max(size_bytes, 1)
        
        # Density component: Cost / Size
        # Higher cost and smaller size = better
        density = (cost * self.config.cost_weight) / (size * self.config.size_weight)
        # Normalize to reasonable range (cost in ms, size in bytes)
        density = density * 1000  # Scale factor
        
        # Information retention score
        retain = self.compute_retain_score(parsed)
        
        # Containment breadth score
        breadth = self.compute_breadth_score(parsed)
        
        # Final score
        score = density * retain * breadth
        
        # Cap score
        score = min(score, self.config.max_score)
        
        logger.debug(
            f"Utility Score: {score:.3f} "
            f"(density={density:.3f}, retain={retain:.2f}, breadth={breadth:.2f})"
        )
        
        return score
    
    def is_high_utility(self, score: float) -> bool:
        """Determine if query has high utility (U=1)."""
        return score >= self.config.utility_threshold
    
    def compute_retain_score(self, parsed: ParsedQuery) -> float:
        """
        Compute information retention score.
        
        SELECT * = 1.0 (full info)
        SELECT col1, col2 = 0.6-0.9 (partial)
        Aggregations = 0.05 (lossy)
        """
        # Check for aggregations
        if parsed.aggregations:
            agg_types = [agg.upper() for agg in parsed.aggregations]
            
            # COUNT, SUM, AVG are lossy - cannot answer SELECT *
            lossy_aggs = {'COUNT', 'SUM', 'AVG', 'TOTAL'}
            has_lossy = any(
                any(la in agg for la in lossy_aggs) 
                for agg in agg_types
            )
            
            if has_lossy:
                return 0.05
            
            # MIN, MAX preserve some info
            return 0.3
        
        # Check columns
        if parsed.columns == ["*"] or "*" in parsed.columns:
            return 1.0
        
        # Partial columns - some reuse potential
        num_cols = len(parsed.columns)
        if num_cols > 10:
            return 0.9
        elif num_cols > 5:
            return 0.8
        elif num_cols > 1:
            return 0.7
        else:
            return 0.6
    
    def compute_breadth_score(self, parsed: ParsedQuery) -> float:
        """
        Compute containment breadth score based on predicate structure.
        
        No WHERE = 1.0 (universal superset)
        Open range (x > 10) = 0.9 (high containment)
        Closed range (BETWEEN) = 0.5 (limited)
        Point query (x = 5) = 0.1 (rarely contains others)
        """
        predicates = parsed.predicates
        
        # No predicates = universal superset
        if predicates.is_empty():
            return 1.0
        
        # Analyze predicate types
        scores = []
        self._analyze_predicate_group(predicates, scores)
        
        if not scores:
            return 1.0
        
        # Combine scores - use minimum (most restrictive predicate dominates)
        return min(scores)
    
    def _analyze_predicate_group(
        self, 
        group: PredicateGroup, 
        scores: List[float]
    ) -> None:
        """Recursively analyze predicate group for breadth scores."""
        for pred in group.predicates:
            score = self._predicate_breadth_score(pred.operator)
            scores.append(score)
        
        for subgroup in group.subgroups:
            self._analyze_predicate_group(subgroup, scores)
    
    def _predicate_breadth_score(self, operator: PredicateOperator) -> float:
        """Get breadth score for a single predicate operator."""
        # Open ranges - high containment potential
        if operator in {
            PredicateOperator.GT,
            PredicateOperator.GTE,
            PredicateOperator.LT,
            PredicateOperator.LTE
        }:
            return 0.9
        
        # Closed range - moderate containment
        if operator == PredicateOperator.BETWEEN:
            return 0.5
        
        # IN list - limited to specific values
        if operator == PredicateOperator.IN:
            return 0.3
        
        # Point query - rarely contains other queries
        if operator == PredicateOperator.EQ:
            return 0.1
        
        # NOT EQUAL - excludes one value, still broad
        if operator == PredicateOperator.NEQ:
            return 0.8
        
        # LIKE - depends on pattern, assume moderate
        if operator == PredicateOperator.LIKE:
            return 0.4
        
        # IS NULL / IS NOT NULL - moderate
        if operator in {PredicateOperator.IS_NULL, PredicateOperator.IS_NOT_NULL}:
            return 0.5
        
        # Default
        return 0.5


def create_utility_scorer(
    threshold: float = 0.5,
    cost_weight: float = 1.0,
    size_weight: float = 1.0
) -> UtilityScorer:
    """Create a utility scorer with custom configuration."""
    config = UtilityConfig(
        utility_threshold=threshold,
        cost_weight=cost_weight,
        size_weight=size_weight
    )
    return UtilityScorer(config=config)
