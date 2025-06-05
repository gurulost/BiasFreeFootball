"""
Quality Wins Calculator
Computes real quality wins based on actual game results using the team graph
"""

import logging
from typing import Dict, List, Tuple
import networkx as nx

class QualityWinsCalculator:
    """Calculates quality wins from team graph and final ratings"""
    
    def __init__(self, config: Dict = None):
        self.config = config or {}
        self.logger = logging.getLogger(__name__)
        
    def calculate_quality_wins(self, team_graph: nx.DiGraph, team_ratings: Dict[str, float], 
                             max_wins: int = 3) -> Dict[str, List[str]]:
        """
        Calculate quality wins for each team based on actual game results
        
        Args:
            team_graph: Directed graph with teams and game edges
            team_ratings: Final PageRank ratings for all teams
            max_wins: Maximum number of quality wins to return per team
            
        Returns:
            Dictionary mapping team name to list of quality wins (opponent names)
        """
        quality_wins = {}
        
        for team in team_graph.nodes():
            # Get all wins for this team (outgoing edges where team defeated opponent)
            wins = []
            
            # Iterate through all outgoing edges to find wins
            for winner, loser, edge_data in team_graph.out_edges(team, data=True):
                if winner == team:  # Confirm this team won
                    edge_weight = edge_data.get('weight', 0.0)
                    opponent_rating = team_ratings.get(loser, 0.0)
                    
                    # Use opponent rating as primary quality metric
                    # Edge weight provides additional context for game importance
                    quality_score = opponent_rating + (edge_weight * 0.1)  # Weight adjustment factor
                    
                    wins.append((quality_score, loser, opponent_rating, edge_weight))
            
            # Sort wins by quality score (opponent rating + edge weight factor)
            wins.sort(reverse=True, key=lambda x: x[0])
            
            # Extract top quality opponents
            quality_opponents = [opponent for _, opponent, _, _ in wins[:max_wins]]
            quality_wins[team] = quality_opponents
            
            # Log quality wins for top teams
            if team_ratings.get(team, 0) > 0.010:  # Top-tier teams
                win_details = []
                for i, (score, opponent, opp_rating, weight) in enumerate(wins[:max_wins]):
                    win_details.append(f"{opponent} ({opp_rating:.6f})")
                
                if win_details:
                    self.logger.info(f"Quality wins for {team}: {', '.join(win_details)}")
                else:
                    self.logger.info(f"No quality wins found for {team}")
        
        return quality_wins
    
    def calculate_enhanced_quality_wins(self, team_graph: nx.DiGraph, team_ratings: Dict[str, float],
                                      conference_ratings: Dict[str, float] = None, 
                                      max_wins: int = 3) -> Dict[str, Dict]:
        """
        Calculate enhanced quality wins with detailed metrics
        
        Args:
            team_graph: Directed graph with teams and game edges
            team_ratings: Final PageRank ratings for all teams
            conference_ratings: Conference strength ratings (optional)
            max_wins: Maximum number of quality wins to return per team
            
        Returns:
            Dictionary with detailed quality wins information
        """
        enhanced_quality_wins = {}
        
        for team in team_graph.nodes():
            wins = []
            
            # Get all wins for this team (outgoing edges where team defeated opponent)
            for winner, loser, edge_data in team_graph.out_edges(team, data=True):
                if winner == team:
                    edge_weight = edge_data.get('weight', 0.0)
                    opponent_rating = team_ratings.get(loser, 0.0)
                    
                    # Enhanced quality score incorporating multiple factors
                    quality_score = opponent_rating
                    
                    # Add conference strength bonus if available
                    if conference_ratings:
                        opponent_conf = self._get_team_conference(loser)
                        if opponent_conf and opponent_conf in conference_ratings:
                            conf_bonus = conference_ratings[opponent_conf] * 0.05
                            quality_score += conf_bonus
                    
                    # Add edge weight factor for game context
                    quality_score += edge_weight * 0.1
                    
                    wins.append({
                        'opponent': loser,
                        'opponent_rating': opponent_rating,
                        'edge_weight': edge_weight,
                        'quality_score': quality_score
                    })
            
            # Sort by quality score
            wins.sort(reverse=True, key=lambda x: x['quality_score'])
            
            # Prepare enhanced output
            quality_wins_list = [win['opponent'] for win in wins[:max_wins]]
            
            enhanced_quality_wins[team] = {
                'quality_wins': quality_wins_list,
                'win_count': len(wins),
                'top_win_rating': wins[0]['opponent_rating'] if wins else 0.0,
                'quality_win_details': wins[:max_wins]
            }
        
        return enhanced_quality_wins
    
    def _get_team_conference(self, team_name: str) -> str:
        """Get conference for team (placeholder - would use actual team data)"""
        # In production, this would lookup from team data
        return None
    
    def validate_quality_wins(self, quality_wins: Dict[str, List[str]], 
                            team_ratings: Dict[str, float]) -> Dict:
        """
        Validate quality wins calculation results
        
        Returns validation report with statistics
        """
        validation_stats = {
            'teams_with_quality_wins': 0,
            'teams_without_wins': 0,
            'average_quality_wins': 0.0,
            'top_teams_with_wins': 0,
            'quality_win_distribution': {}
        }
        
        total_quality_wins = 0
        top_tier_threshold = sorted(team_ratings.values(), reverse=True)[20] if len(team_ratings) >= 20 else 0.010
        
        for team, wins in quality_wins.items():
            win_count = len(wins)
            total_quality_wins += win_count
            
            if win_count > 0:
                validation_stats['teams_with_quality_wins'] += 1
            else:
                validation_stats['teams_without_wins'] += 1
            
            # Track distribution
            if win_count not in validation_stats['quality_win_distribution']:
                validation_stats['quality_win_distribution'][win_count] = 0
            validation_stats['quality_win_distribution'][win_count] += 1
            
            # Check top teams
            if team_ratings.get(team, 0) >= top_tier_threshold and win_count > 0:
                validation_stats['top_teams_with_wins'] += 1
        
        validation_stats['average_quality_wins'] = total_quality_wins / len(quality_wins) if quality_wins else 0
        
        self.logger.info(f"Quality wins validation: {validation_stats['teams_with_quality_wins']} teams have wins, "
                        f"avg = {validation_stats['average_quality_wins']:.2f}")
        
        return validation_stats

def calculate_quality_wins(team_graph: nx.DiGraph, team_ratings: Dict[str, float], 
                         max_wins: int = 3, config: Dict = None) -> Dict[str, List[str]]:
    """Convenience function for quality wins calculation"""
    calculator = QualityWinsCalculator(config)
    return calculator.calculate_quality_wins(team_graph, team_ratings, max_wins)