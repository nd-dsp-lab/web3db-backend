"""
Containment-Aware Admission (CAA) Policy

A novel cache admission policy that prioritizes queries with high containment
potential - the likelihood that cached results can serve future queries via
subset matching.

Formula: Score(Q) = (Cost / Size) × Retain(Q) × Breadth(Q)

For VLDB paper on semantic caching.
"""

from dataclasses import dataclass, field
from typing import Optional, List
import logging

from query_parser import ParsedQuery, PredicateOperator, PredicateGroup

logger = logging.getLogger(__name__)


@dataclass
class CAAConfig:
    """Configuration for CAA policy"""
    admission_threshold: float = 0.5  # Minimum score for admission
    cost_weight: float = 1.0  # Weight for cost component
    size_weight: float = 1.0  # Weight for size penalty
    min_cost_ms: float = 1.0  # Minimum cost to avoid division issues
    max_score: float = 100.0  # Cap score to avoid outliers


@dataclass  
class CAAScorer:
    """
    Containment-Aware Admission Scorer
    
    Computes admission score based on:
    - Cost: Query execution time (higher = more valuable to cache)
    - Size: Memory footprint (higher = more costly to cache)
    - Retain: Information preservation (SELECT * vs aggregations)
    - Breadth: Containment probability (range vs point queries)
    """
    config: CAAConfig = field(default_factory=CAAConfig)
    
    def compute_score(
        self, 
        parsed: ParsedQuery, 
        cost_ms: float, 
        size_bytes: int
    ) -> float:
        """
        Compute CAA score for a query.
        
        Score = (Cost / Size) × Retain × Breadth
        
        Higher score = higher priority for caching.
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
            f"CAA Score: {score:.3f} "
            f"(density={density:.3f}, retain={retain:.2f}, breadth={breadth:.2f})"
        )
        
        return score
    
    def compute_retain_score(self, parsed: ParsedQuery) -> float:
        """
        Compute information retention score.
        
        SELECT * = 1.0 (full info)
        SELECT col1, col2 = 0.8 (partial)
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
        
        # Combine scores - use geometric mean for AND, arithmetic for OR
        # For simplicity, use minimum (most restrictive predicate dominates)
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
    
    def should_admit(self, score: float) -> bool:
        """Determine if query should be admitted to cache."""
        return score >= self.config.admission_threshold


# Convenience function
def create_caa_scorer(
    threshold: float = 0.5,
    cost_weight: float = 1.0,
    size_weight: float = 1.0
) -> CAAScorer:
    """Create a CAA scorer with custom configuration."""
    config = CAAConfig(
        admission_threshold=threshold,
        cost_weight=cost_weight,
        size_weight=size_weight
    )
    return CAAScorer(config=config)
