"""
PageRank implementation for both conference and team rankings
Two-stage power iteration with configurable damping and tolerance
"""

import numpy as np
import networkx as nx
from typing import Dict, Optional
import logging

class PageRankCalculator:
    def __init__(self, config: Dict):
        self.config = config
        self.damping = float(config['pagerank']['damping'])
        self.tolerance = float(config['pagerank']['tolerance'])
        self.max_iterations = int(config['pagerank']['max_iterations'])
        self.logger = logging.getLogger(__name__)
    
    def pagerank(self, G: nx.DiGraph, personalization: Optional[Dict] = None,
                initial_ratings: Optional[Dict] = None) -> Dict:
        """
        Calculate PageRank using power iteration method
        
        Args:
            G: Directed graph with weighted edges
            personalization: Optional personalization vector
            initial_ratings: Optional starting ratings
            
        Returns:
            Dictionary mapping nodes to PageRank scores
        """
        if len(G.nodes) == 0:
            return {}
        
        # Convert to matrices for computation
        nodes = list(G.nodes())
        n = len(nodes)
        node_to_idx = {node: i for i, node in enumerate(nodes)}
        
        # Build transition matrix
        M = np.zeros((n, n))
        
        for node in nodes:
            out_edges = list(G.out_edges(node, data=True))
            if not out_edges:
                # Dead end - distribute equally to all nodes
                M[:, node_to_idx[node]] = 1.0 / n
            else:
                # Calculate total outgoing weight
                total_weight = sum(data['weight'] for _, _, data in out_edges)
                if total_weight == 0:
                    M[:, node_to_idx[node]] = 1.0 / n
                else:
                    # Distribute weight proportionally
                    for _, target, data in out_edges:
                        weight = data['weight']
                        M[node_to_idx[target], node_to_idx[node]] = weight / total_weight
        
        # Initialize PageRank vector
        if initial_ratings:
            pr = np.array([initial_ratings.get(node, 1.0/n) for node in nodes])
            pr = pr / pr.sum()  # Normalize
        else:
            pr = np.ones(n) / n
        
        # Personalization vector (uniform if not specified)
        if personalization:
            pers = np.array([personalization.get(node, 1.0/n) for node in nodes])
            pers = pers / pers.sum()  # Normalize
        else:
            pers = np.ones(n) / n
        
        # Power iteration
        for iteration in range(self.max_iterations):
            pr_new = self.damping * M.dot(pr) + (1 - self.damping) * pers
            
            # Check convergence
            diff = float(np.abs(pr_new - pr).max())
            if diff < self.tolerance:
                self.logger.debug(f"PageRank converged in {iteration + 1} iterations")
                break
                
            pr = pr_new
        else:
            self.logger.warning(f"PageRank did not converge after {self.max_iterations} iterations")
        
        # Convert back to dictionary
        result = {nodes[i]: pr[i] for i in range(n)}
        
        self.logger.debug(f"PageRank computed for {n} nodes")
        return result
    
    def pagerank_weighted(self, G: nx.DiGraph, weight_attr: str = 'weight',
                         **kwargs) -> Dict:
        """
        PageRank with explicit weight attribute handling
        Wrapper around main pagerank method
        """
        return self.pagerank(G, **kwargs)
    
    def validate_pagerank(self, rankings: Dict) -> bool:
        """
        Validate PageRank results
        Check that scores sum to 1 and are all positive
        """
        if not rankings:
            return False
            
        scores = list(rankings.values())
        total = sum(scores)
        
        # Check sum approximately equals 1
        if abs(total - 1.0) > 1e-6:
            self.logger.warning(f"PageRank scores sum to {total}, not 1.0")
            return False
        
        # Check all scores are positive
        if any(score <= 0 for score in scores):
            self.logger.warning("Some PageRank scores are non-positive")
            return False
        
        return True
    
    def normalize_rankings(self, rankings: Dict) -> Dict:
        """
        Normalize rankings to sum to 1
        Useful for correcting small numerical errors
        """
        if not rankings:
            return rankings
            
        total = sum(rankings.values())
        if total == 0:
            # All zero - return uniform distribution
            n = len(rankings)
            return {node: 1.0/n for node in rankings}
        
        return {node: score/total for node, score in rankings.items()}

def pagerank(G: nx.DiGraph, damping: Optional[float] = None, 
            tolerance: Optional[float] = None, config: Dict = None) -> Dict:
    """
    Convenience function for PageRank calculation
    Uses config defaults if parameters not specified
    """
    if config is None:
        import yaml
        with open('config.yaml', 'r') as f:
            config = yaml.safe_load(f)
    
    calc = PageRankCalculator(config)
    
    # Override config parameters if provided
    if damping is not None:
        calc.damping = damping
    if tolerance is not None:
        calc.tolerance = tolerance
    
    return calc.pagerank(G)

def pagerank_scipy(G: nx.DiGraph, **kwargs) -> Dict:
    """
    Alternative PageRank implementation using NetworkX's built-in method
    Falls back to this if custom implementation has issues
    """
    try:
        return nx.pagerank(G, weight='weight', **kwargs)
    except Exception as e:
        logging.error(f"NetworkX PageRank failed: {e}")
        return {}

class PowerIteration:
    """
    Custom power iteration implementation for educational purposes
    and fine-grained control over the algorithm
    """
    
    @staticmethod
    def power_iteration(A: np.ndarray, num_iterations: int = 1000, 
                       tolerance: float = 1e-9) -> np.ndarray:
        """
        Basic power iteration for dominant eigenvalue/eigenvector
        
        Args:
            A: Square matrix
            num_iterations: Maximum iterations
            tolerance: Convergence tolerance
            
        Returns:
            Dominant eigenvector
        """
        n = A.shape[0]
        v = np.random.random(n)
        v = v / np.linalg.norm(v)
        
        for i in range(num_iterations):
            v_new = A.dot(v)
            v_new = v_new / np.linalg.norm(v_new)
            
            if np.linalg.norm(v_new - v) < tolerance:
                break
                
            v = v_new
        
        return v
    
    @staticmethod
    def pagerank_power_iteration(transition_matrix: np.ndarray, 
                               damping: float = 0.85,
                               num_iterations: int = 1000,
                               tolerance: float = 1e-9) -> np.ndarray:
        """
        PageRank using custom power iteration
        
        Args:
            transition_matrix: Column-stochastic transition matrix
            damping: Damping factor (0 < d < 1)
            num_iterations: Maximum iterations
            tolerance: Convergence tolerance
            
        Returns:
            PageRank vector
        """
        n = transition_matrix.shape[0]
        
        # PageRank matrix: d*M + (1-d)/n * ones
        ones_matrix = np.ones((n, n)) / n
        pagerank_matrix = damping * transition_matrix + (1 - damping) * ones_matrix
        
        # Power iteration
        v = np.ones(n) / n  # Start with uniform distribution
        
        for i in range(num_iterations):
            v_new = pagerank_matrix.dot(v)
            
            if np.linalg.norm(v_new - v) < tolerance:
                break
                
            v = v_new
        
        return v
