"""
Graph construction module for two-layer PageRank
Builds team and conference directed graphs with weighted edges
Following exact blueprint formulas
"""

import networkx as nx
import pandas as pd
import numpy as np
from typing import Dict, Tuple, Optional
import logging
from src.weights import WeightCalculator

logger = logging.getLogger(__name__)


class GraphBuilder:
    def __init__(self, config: Dict):
        self.config = config
        self.weight_calc = WeightCalculator(config)

    def build_graphs(self, games_df: pd.DataFrame, prev_ratings: Dict = None,
                    current_week: int = 1) -> Tuple[nx.DiGraph, nx.DiGraph]:
        """
        Build both conference and team graphs from game data
        Returns (conference_graph, team_graph)
        Following exact blueprint: base = margin * venue * decay
        """
        G_conf = nx.DiGraph()
        G_team = nx.DiGraph()
        
        # Get all teams and conferences
        teams = set(games_df['winner'].tolist() + games_df['loser'].tolist())
        conferences = set()
        for conf in games_df['winner_conference'].dropna().tolist() + games_df['loser_conference'].dropna().tolist():
            if conf and pd.notna(conf):
                conferences.add(conf)
        
        # Add nodes
        G_team.add_nodes_from(teams)
        G_conf.add_nodes_from(conferences)
        
        # Initialize previous ratings if not provided
        if prev_ratings is None:
            prev_ratings = {team: 0.5 for team in teams}
        
        # Calculate games played for shrinkage weight calculation
        games_played = {}
        for team in teams:
            team_games = games_df[(games_df['winner'] == team) | (games_df['loser'] == team)]
            games_played[team] = len(team_games)
        
        # Process each game following exact blueprint formulas
        for idx, game in games_df.iterrows():
            winner = game['winner']
            loser = game['loser']
            winner_conf = game.get('winner_conference')
            loser_conf = game.get('loser_conference')
            
            if pd.isna(winner_conf):
                winner_conf = None
            if pd.isna(loser_conf):
                loser_conf = None
            
            # Get ratings for expectation calculation
            rating_winner = prev_ratings.get(winner, 0.5)
            rating_loser = prev_ratings.get(loser, 0.5)
            
            # Calculate edge weights using exact blueprint formulas
            weights = self.weight_calc.calculate_edge_weights(
                game.to_dict(), rating_winner, rating_loser, current_week,
                games_played.get(winner, 0), games_played.get(loser, 0)
            )
            
            # Team graph edges (both directions as per blueprint)
            credit_weight = weights['credit_weight']
            penalty_weight = weights['penalty_weight']
            
            # Blueprint exact implementation: loser -> winner (credit), winner -> loser (penalty)
            # Credit edge: loser points to winner (creates incoming edges to winners)
            if G_team.has_edge(loser, winner):
                G_team[loser][winner]['weight'] += credit_weight
            else:
                G_team.add_edge(loser, winner, weight=credit_weight)
                
            # Penalty edge: winner points to loser (penalty for expected wins)
            if G_team.has_edge(winner, loser):
                G_team[winner][loser]['weight'] += penalty_weight
            else:
                G_team.add_edge(winner, loser, weight=penalty_weight)
            
            # Conference graph edge (cross-conference only, loser -> winner)
            if (weights['is_cross_conf'] and winner_conf and loser_conf):
                conf_weight = weights['conf_weight']
                
                if G_conf.has_edge(loser_conf, winner_conf):
                    G_conf[loser_conf][winner_conf]['weight'] += conf_weight
                else:
                    G_conf.add_edge(loser_conf, winner_conf, weight=conf_weight)
        
        logger.info(f"Built team graph: {G_team.number_of_nodes()} nodes, {G_team.number_of_edges()} edges")
        logger.info(f"Built conference graph: {G_conf.number_of_nodes()} nodes, {G_conf.number_of_edges()} edges")
        
        return G_conf, G_team

    def inject_conf_strength(self, G_team: nx.DiGraph, conf_ratings: Dict, 
                           prev_conf_ratings: Dict = None) -> None:
        """
        Inject conference strength into intra-conference team edges
        Multiplies intra-conference edge weights by sqrt(conference_strength)
        """
        if prev_conf_ratings is None:
            prev_conf_ratings = conf_ratings
            
        # Get team-to-conference mapping
        team_to_conf = self._get_team_conference_mapping(G_team)
        
        # Multiply intra-conference edges by sqrt(conf_strength)
        edges_modified = 0
        for u, v, data in G_team.edges(data=True):
            if u in team_to_conf and v in team_to_conf:
                conf_u = team_to_conf[u]
                conf_v = team_to_conf[v]
                
                # Only modify intra-conference edges
                if conf_u == conf_v and conf_u in conf_ratings:
                    conf_strength = conf_ratings[conf_u]
                    multiplier = np.sqrt(conf_strength)
                    data['weight'] *= multiplier
                    edges_modified += 1
        
        logger.info(f"Applied conference strength to {edges_modified} intra-conference edges")

    def _get_team_conference_mapping(self, G_team: nx.DiGraph) -> Dict[str, str]:
        """
        Extract team-to-conference mapping from team names or external data
        This is a simplified version - in production, you'd load this from the API
        """
        # For now, return a simplified mapping for major conferences
        mapping = {}
        
        # Major conference mappings (simplified)
        sec_teams = ['Alabama', 'Georgia', 'Florida', 'LSU', 'Auburn', 'Tennessee', 
                    'Kentucky', 'South Carolina', 'Mississippi State', 'Ole Miss',
                    'Arkansas', 'Missouri', 'Vanderbilt', 'Texas A&M']
        
        big10_teams = ['Ohio State', 'Michigan', 'Penn State', 'Wisconsin', 
                      'Iowa', 'Minnesota', 'Illinois', 'Indiana', 'Maryland',
                      'Michigan State', 'Nebraska', 'Northwestern', 'Purdue', 'Rutgers']
        
        big12_teams = ['Oklahoma', 'Texas', 'Baylor', 'Oklahoma State', 'TCU',
                      'Kansas State', 'West Virginia', 'Iowa State', 'Kansas', 'Texas Tech']
        
        pac12_teams = ['USC', 'UCLA', 'Oregon', 'Washington', 'Stanford', 'Cal',
                      'Arizona State', 'Arizona', 'Utah', 'Colorado', 'Oregon State', 'Washington State']
        
        acc_teams = ['Clemson', 'Florida State', 'Miami', 'Virginia Tech', 'North Carolina',
                    'Duke', 'NC State', 'Wake Forest', 'Virginia', 'Pittsburgh', 'Syracuse',
                    'Boston College', 'Georgia Tech', 'Louisville']
        
        for team in G_team.nodes():
            if team in sec_teams:
                mapping[team] = 'SEC'
            elif team in big10_teams:
                mapping[team] = 'Big Ten'
            elif team in big12_teams:
                mapping[team] = 'Big 12'
            elif team in pac12_teams:
                mapping[team] = 'Pac-12'
            elif team in acc_teams:
                mapping[team] = 'ACC'
            else:
                mapping[team] = 'Other'
        
        return mapping

    def rebuild_full_graph(self, games_df: pd.DataFrame, current_ratings: Dict,
                          hindsight: bool = True) -> Tuple[nx.DiGraph, nx.DiGraph]:
        """
        Rebuild graphs for retro pipeline with hindsight ratings
        No shrinkage applied, uses final ratings for all calculations
        """
        G_conf = nx.DiGraph()
        G_team = nx.DiGraph()
        
        # Get all teams and conferences
        teams = set(games_df['winner'].tolist() + games_df['loser'].tolist())
        conferences = set()
        for conf in games_df['winner_conference'].dropna().tolist() + games_df['loser_conference'].dropna().tolist():
            if conf and pd.notna(conf):
                conferences.add(conf)
        
        # Add nodes
        G_team.add_nodes_from(teams)
        G_conf.add_nodes_from(conferences)
        
        # For retro, no shrinkage - use current ratings directly
        for idx, game in games_df.iterrows():
            winner = game['winner']
            loser = game['loser']
            winner_conf = game.get('winner_conference')
            loser_conf = game.get('loser_conference')
            
            if pd.isna(winner_conf):
                winner_conf = None
            if pd.isna(loser_conf):
                loser_conf = None
            
            # Use current (converged) ratings for expectation
            rating_winner = current_ratings.get(winner, 0.5)
            rating_loser = current_ratings.get(loser, 0.5)
            
            # Calculate weights with no shrinkage (large games_played)
            weights = self.weight_calc.calculate_edge_weights(
                game.to_dict(), rating_winner, rating_loser, 
                game.get('week', 1), 999, 999  # Large games to disable shrinkage
            )
            
            # Add edges exactly as in live pipeline
            credit_weight = weights['credit_weight']
            penalty_weight = weights['penalty_weight']
            
            if G_team.has_edge(loser, winner):
                G_team[loser][winner]['weight'] += credit_weight
            else:
                G_team.add_edge(loser, winner, weight=credit_weight)
                
            if G_team.has_edge(winner, loser):
                G_team[winner][loser]['weight'] += penalty_weight
            else:
                G_team.add_edge(winner, loser, weight=penalty_weight)
            
            # Conference edges
            if (weights['is_cross_conf'] and winner_conf and loser_conf):
                conf_weight = weights['conf_weight']
                
                if G_conf.has_edge(loser_conf, winner_conf):
                    G_conf[loser_conf][winner_conf]['weight'] += conf_weight
                else:
                    G_conf.add_edge(loser_conf, winner_conf, weight=conf_weight)
        
        return G_conf, G_team


def inject_conf_strength(G_team: nx.DiGraph, S: Dict, S_prev: Dict, config: Dict = None):
    """Convenience function for conference strength injection"""
    builder = GraphBuilder(config or {})
    builder.inject_conf_strength(G_team, S, S_prev)


def rebuild_full_graph(games_df: pd.DataFrame, R: Dict, hindsight: bool = True, 
                      config: Dict = None) -> Tuple[nx.DiGraph, nx.DiGraph]:
    """Convenience function for full graph rebuilding"""
    builder = GraphBuilder(config or {})
    return builder.rebuild_full_graph(games_df, R, hindsight)