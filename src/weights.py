"""
Edge weight calculation module implementing exact blueprint formulas
Handles margin, venue, recency decay, risk/surprise multipliers
"""

import math
import numpy as np
from typing import Dict, Any, Optional


class WeightCalculator:
    def __init__(self, config: Dict):
        self.config = config
        self.margin_cap = config.get('margin_cap', 5)
        self.lambda_decay = config.get('lambda_decay', 0.05)
        self.venue_factors = config.get('venue_factors', {
            'home': 1.1,
            'neutral': 1.0,
            'away': 0.9
        })
        self.shrinkage_k = config.get('shrinkage_k', 4)
        self.win_prob_c = config.get('win_prob_c', 0.40)
        self.risk_b = config.get('risk_b', 1.0)
        self.gamma = config.get('gamma', 0.75)
        self.surprise_cap = config.get('surprise_cap', 3)
        self.bowl_bump = config.get('bowl_bump', 1.10)

    def margin_factor(self, game_data: Dict) -> float:
        """
        Margin factor M = log_2(1 + max(margin, 1))
        Capped at margin_cap value
        """
        margin = abs(game_data.get('points_winner', 0) - game_data.get('points_loser', 0))
        margin = max(margin, 1)  # minimum margin of 1
        if margin > self.margin_cap:
            margin = self.margin_cap
        return math.log2(1 + margin)

    def venue_factor(self, game_data: Dict) -> float:
        """
        Venue factor F: 1.1 (home), 1.0 (neutral), 0.9 (road)
        From winner's perspective
        """
        venue = game_data.get('venue', 'neutral')
        winner_home = game_data.get('winner_home', False)
        
        if venue == 'neutral':
            return self.venue_factors['neutral']
        elif winner_home:
            return self.venue_factors['home']
        else:
            return self.venue_factors['away']

    def decay_factor(self, game_data: Dict, current_week: int) -> float:
        """
        Recency factor R = exp(-lambda * delta_weeks)
        """
        game_week = game_data.get('week', current_week)
        delta_weeks = current_week - game_week
        return math.exp(-self.lambda_decay * delta_weeks)

    def blended_rating(self, rating: float, games_played: int) -> float:
        """
        Shrinkage weight omega_i = g_i / (g_i + k)
        Blends rating with prior (0.5) based on games played
        """
        omega = games_played / (games_played + self.shrinkage_k)
        prior = 0.5  # neutral prior rating
        return omega * rating + (1 - omega) * prior

    def win_prob(self, rating_a: float, rating_b: float) -> float:
        """
        Win probability p_exp = 1 / (1 + 10^(-(R_a - R_b) / C))
        """
        diff = rating_a - rating_b
        return 1 / (1 + 10**(-diff / self.win_prob_c))

    def risk_multipliers(self, base_weight: float, game_data: Dict, p_exp: float) -> tuple:
        """
        Risk multipliers:
        - Credit: (1-p) / 0.5^B for upset wins
        - Penalty: (p / 0.5)^B for expected losses
        
        Returns: (credit_multiplier, penalty_multiplier)
        """
        # Credit for winner (upset factor)
        credit = (1 - p_exp) / (0.5**self.risk_b)
        
        # Penalty for loser (expected loss factor)
        penalty = (p_exp / 0.5)**self.risk_b
        
        return credit, penalty

    def surprise_multiplier(self, p_exp: float) -> float:
        """
        Surprise multiplier = 1 + gamma * I
        where I = -log_2(p_exp) (information content)
        Capped at surprise_cap
        """
        information = -math.log2(max(p_exp, 1e-10))  # avoid log(0)
        multiplier = 1 + self.gamma * information
        return min(multiplier, self.surprise_cap)

    def is_bowl_game(self, game_data: Dict) -> bool:
        """Check if game is a bowl game"""
        return (game_data.get('season_type', 'regular') == 'postseason' or 
                game_data.get('is_bowl', False))

    def calculate_edge_weights(self, game_data: Dict, rating_winner: float, 
                             rating_loser: float, current_week: int,
                             games_winner: int, games_loser: int) -> Dict:
        """
        Calculate all edge weights for a game following exact blueprint formulas
        
        Returns:
            Dictionary with credit_weight, penalty_weight, conf_weight, is_cross_conf
        """
        # Base weight components
        margin = self.margin_factor(game_data)
        venue = self.venue_factor(game_data)
        decay = self.decay_factor(game_data, current_week)
        base = margin * venue * decay
        
        # Blended ratings for expectation
        ra_blend = self.blended_rating(rating_winner, games_winner)
        rb_blend = self.blended_rating(rating_loser, games_loser)
        p_exp = self.win_prob(ra_blend, rb_blend)
        
        # Risk multipliers
        credit_mult, penalty_mult = self.risk_multipliers(base, game_data, p_exp)
        
        # Final edge weights
        credit_weight = base * credit_mult
        penalty_weight = base * penalty_mult
        
        # Bowl game bump for credit edge
        if self.is_bowl_game(game_data):
            credit_weight *= self.bowl_bump
        
        # Conference classification and bowl detection
        winner_conf = game_data.get('winner_conference')
        loser_conf = game_data.get('loser_conference')
        is_bowl = self.is_bowl_game(game_data)
        is_cross_conf = (winner_conf != loser_conf)
        
        # Special handling for intra-conference bowls
        is_intra_conf_bowl = (is_bowl and not is_cross_conf and 
                             winner_conf is not None and loser_conf is not None)
        
        # Conference graph weight
        conf_weight = 0
        if is_cross_conf:
            # Cross-conference games contribute to conference graph
            surprise = self.surprise_multiplier(p_exp)
            conf_weight = credit_weight * surprise
        # Note: Intra-conference bowls do NOT contribute to conference graph
        
        return {
            'credit_weight': credit_weight,
            'penalty_weight': penalty_weight,
            'conf_weight': conf_weight,
            'is_cross_conf': is_cross_conf,
            'is_bowl': is_bowl,
            'is_intra_conf_bowl': is_intra_conf_bowl,
            'p_exp': p_exp,
            'base_weight': base,
            'margin_factor': margin,
            'venue_factor': venue,
            'decay_factor': decay
        }


def margin_factor(game_data: Dict, config: Dict = None) -> float:
    """Convenience function for margin factor calculation"""
    calc = WeightCalculator(config if config is not None else {})
    return calc.margin_factor(game_data)


def venue_factor(game_data: Dict, config: Dict = None) -> float:
    """Convenience function for venue factor calculation"""
    calc = WeightCalculator(config if config is not None else {})
    return calc.venue_factor(game_data)


def decay_factor(game_data: Dict, current_week: int, config: Dict = None) -> float:
    """Convenience function for decay factor calculation"""
    calc = WeightCalculator(config if config is not None else {})
    return calc.decay_factor(game_data, current_week)


def risk_edges(base_weight: float, game_data: Dict, p_exp: float, 
               config: Dict = None) -> tuple:
    """Convenience function for risk multiplier calculation"""
    calc = WeightCalculator(config if config is not None else {})
    return calc.risk_multipliers(base_weight, game_data, p_exp)


def surprise_multiplier(p_exp: float, gamma: float = 0.75, cap: float = 3) -> float:
    """Convenience function for surprise multiplier calculation"""
    information = -math.log2(max(p_exp, 1e-10))
    multiplier = 1 + gamma * information
    return min(multiplier, cap)


def calculate_all_weights(game_data: Dict, rating_winner: float, rating_loser: float,
                         current_week: int, games_winner: int, games_loser: int,
                         config: Dict = None) -> Dict:
    """Convenience function for complete weight calculation"""
    calc = WeightCalculator(config if config is not None else {})
    return calc.calculate_edge_weights(game_data, rating_winner, rating_loser,
                                     current_week, games_winner, games_loser)