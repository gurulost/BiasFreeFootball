"""
Weight calculation module for margin, venue, recency, and multipliers
Implements all the mathematical formulas from the blueprint
"""

import numpy as np
import pandas as pd
from typing import Dict, Tuple
import logging
from datetime import datetime, timedelta

class WeightCalculator:
    def __init__(self, config: Dict):
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Extract parameters from config
        self.margin_cap = config['margin']['cap']
        self.venue_factors = config['venue']
        self.recency_lambda = config['recency']['lambda']
        self.shrinkage_k = config['shrinkage']['k']
        self.win_prob_C = config['win_probability']['C']
        self.risk_B = config['risk']['B']
        self.surprise_gamma = config['surprise']['gamma']
        self.surprise_cap = config['surprise']['cap']
        self.bowl_bump = config['bowl']['weight_bump']
    
    def margin_factor(self, margin: float) -> float:
        """
        Calculate margin factor: log_2(1 + max(margin, 1))
        Capped at margin_cap to prevent style points dominance
        """
        effective_margin = min(max(margin, 1), 2**self.margin_cap - 1)
        return np.log2(1 + effective_margin)
    
    def venue_factor(self, venue: str) -> float:
        """Calculate venue factor based on game location"""
        return {
            'home': self.venue_factors['home_factor'],
            'neutral': self.venue_factors['neutral_factor'], 
            'road': self.venue_factors['road_factor']
        }.get(venue, self.venue_factors['neutral_factor'])
    
    def recency_factor(self, weeks_ago: float) -> float:
        """
        Calculate recency decay factor: exp(-lambda * weeks_ago)
        Gives ~25% discount to Labor Day games by Thanksgiving
        """
        return np.exp(-self.recency_lambda * weeks_ago)
    
    def shrinkage_weight(self, games_played: int) -> float:
        """
        Calculate shrinkage weight: games / (games + k)
        Blends team rating with global mean for early weeks
        """
        return games_played / (games_played + self.shrinkage_k)
    
    def win_probability(self, rating_a: float, rating_b: float) -> float:
        """
        Calculate expected win probability for team A vs team B
        Formula: 1 / (1 + 10^(-(Ra - Rb) / C))
        """
        rating_diff = rating_a - rating_b
        return 1 / (1 + 10**(-rating_diff / self.win_prob_C))
    
    def risk_multipliers(self, base_weight: float, p_expected: float) -> Tuple[float, float]:
        """
        Calculate risk-based credit and penalty multipliers
        Credit: (1 - p) / 0.5^B, Penalty: (p / 0.5)^B
        """
        # Avoid division by zero
        p_expected = max(0.001, min(0.999, p_expected))
        
        credit = base_weight * ((1 - p_expected) / (0.5**self.risk_B))
        penalty = base_weight * ((p_expected / 0.5)**self.risk_B)
        
        return credit, penalty
    
    def surprise_multiplier(self, p_expected: float) -> float:
        """
        Calculate surprise multiplier for cross-conference games
        Formula: 1 + gamma * I, where I = -log_2(p_expected)
        Capped at surprise_cap
        """
        # Avoid log of zero
        p_expected = max(0.001, min(0.999, p_expected))
        
        information_bits = -np.log2(p_expected)
        multiplier = 1 + self.surprise_gamma * information_bits
        
        return min(multiplier, self.surprise_cap)
    
    def calculate_game_weights(self, game: Dict, current_week: int, 
                             winner_rating: float, loser_rating: float) -> Dict:
        """
        Calculate all weights for a single game
        Returns dictionary with all weight components
        """
        # Base weight components
        margin_weight = self.margin_factor(game['margin'])
        venue_weight = self.venue_factor(game['venue'])
        
        # Calculate weeks ago (for recency)
        weeks_ago = max(0, current_week - game['week'])
        recency_weight = self.recency_factor(weeks_ago)
        
        # Base weight before risk/surprise
        base_weight = margin_weight * venue_weight * recency_weight
        
        # Expected win probability (winner perspective)
        p_expected = self.win_probability(winner_rating, loser_rating)
        
        # Risk multipliers
        credit, penalty = self.risk_multipliers(base_weight, p_expected)
        
        # Surprise multiplier (for cross-conference games)
        surprise = self.surprise_multiplier(p_expected) if game.get('cross_conf', False) else 1.0
        
        # Bowl game bump
        bowl_multiplier = self.bowl_bump if game.get('is_bowl', False) else 1.0
        
        # Final weights
        final_credit = credit * bowl_multiplier
        final_penalty = penalty * bowl_multiplier
        
        # Cross-conference weights include surprise
        cross_conf_credit = final_credit * surprise if game.get('cross_conf', False) else 0
        
        return {
            'margin_factor': margin_weight,
            'venue_factor': venue_weight,
            'recency_factor': recency_weight,
            'base_weight': base_weight,
            'p_expected': p_expected,
            'risk_credit': credit,
            'risk_penalty': penalty,
            'surprise_multiplier': surprise,
            'bowl_multiplier': bowl_multiplier,
            'final_credit': final_credit,
            'final_penalty': final_penalty,
            'cross_conf_credit': cross_conf_credit,
            'weeks_ago': weeks_ago
        }
    
    def blended_rating(self, team_rating: float, games_played: int, 
                      global_mean: float = 0.5) -> float:
        """
        Calculate blended rating using shrinkage
        Blends team rating with global mean based on games played
        """
        omega = self.shrinkage_weight(games_played)
        return omega * team_rating + (1 - omega) * global_mean

def margin_factor(game: Dict, config: Dict = None) -> float:
    """Convenience function for margin factor calculation"""
    if config is None:
        import yaml
        with open('config.yaml', 'r') as f:
            config = yaml.safe_load(f)
    
    calc = WeightCalculator(config)
    return calc.margin_factor(game['margin'])

def venue_factor(game: Dict, config: Dict = None) -> float:
    """Convenience function for venue factor calculation"""
    if config is None:
        import yaml
        with open('config.yaml', 'r') as f:
            config = yaml.safe_load(f)
    
    calc = WeightCalculator(config)
    return calc.venue_factor(game['venue'])

def decay_factor(game: Dict, current_week: int, config: Dict = None) -> float:
    """Convenience function for recency decay calculation"""
    if config is None:
        import yaml
        with open('config.yaml', 'r') as f:
            config = yaml.safe_load(f)
    
    calc = WeightCalculator(config)
    weeks_ago = max(0, current_week - game['week'])
    return calc.recency_factor(weeks_ago)

def risk_edges(base_weight: float, game: Dict, p_expected: float, 
              config: Dict = None) -> Tuple[float, float]:
    """Convenience function for risk multiplier calculation"""
    if config is None:
        import yaml
        with open('config.yaml', 'r') as f:
            config = yaml.safe_load(f)
    
    calc = WeightCalculator(config)
    return calc.risk_multipliers(base_weight, p_expected)

def surprise_multiplier(p_expected: float, gamma: float = None, 
                       cap: float = None, config: Dict = None) -> float:
    """Convenience function for surprise multiplier calculation"""
    if config is None:
        import yaml
        with open('config.yaml', 'r') as f:
            config = yaml.safe_load(f)
    
    calc = WeightCalculator(config)
    # Override config values if provided
    if gamma is not None:
        calc.surprise_gamma = gamma
    if cap is not None:
        calc.surprise_cap = cap
    
    return calc.surprise_multiplier(p_expected)

def win_prob(rating_a: float, rating_b: float, config: Dict = None) -> float:
    """Convenience function for win probability calculation"""
    if config is None:
        import yaml
        with open('config.yaml', 'r') as f:
            config = yaml.safe_load(f)
    
    calc = WeightCalculator(config)
    return calc.win_probability(rating_a, rating_b)

def blended_rating(team_rating: float, games_played: int = 1, 
                  global_mean: float = 0.5, config: Dict = None) -> float:
    """Convenience function for blended rating calculation"""
    if config is None:
        import yaml
        with open('config.yaml', 'r') as f:
            config = yaml.safe_load(f)
    
    calc = WeightCalculator(config)
    return calc.blended_rating(team_rating, games_played, global_mean)
