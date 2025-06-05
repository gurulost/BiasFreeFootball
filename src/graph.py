"""
Graph construction module for two-layer PageRank
Builds team and conference directed graphs with weighted edges
"""

import networkx as nx
import numpy as np
import pandas as pd
from typing import Dict, List, Tuple, Set
import logging
from src.weights import WeightCalculator

class GraphBuilder:
    def __init__(self, config: Dict):
        self.config = config
        self.weight_calc = WeightCalculator(config)
        self.logger = logging.getLogger(__name__)
    
    def build_graphs(self, games_df: pd.DataFrame, prev_ratings: Dict = None,
                    current_week: int = 1) -> Tuple[nx.DiGraph, nx.DiGraph]:
        """
        Build both conference and team graphs from game data
        Returns (conference_graph, team_graph)
        """
        # Initialize graphs
        G_conf = nx.DiGraph()
        G_team = nx.DiGraph()
        
        # Get all teams and conferences
        teams = set(games_df['winner'].tolist() + games_df['loser'].tolist())
        conferences = set()
        for conf in games_df['winner_conference'].dropna().tolist() + games_df['loser_conference'].dropna().tolist():
            if conf:
                conferences.add(conf)
        
        # Add nodes
        G_team.add_nodes_from(teams)
        G_conf.add_nodes_from(conferences)
        
        # Initialize previous ratings if not provided
        if prev_ratings is None:
            prev_ratings = {team: 0.5 for team in teams}
        
        # Calculate games played for each team (for shrinkage)
        games_played = {}
        for team in teams:
            team_games = games_df[(games_df['winner'] == team) | (games_df['loser'] == team)]
            games_played[team] = len(team_games)
        
        # Process each game
        for _, game in games_df.iterrows():
            winner = game['winner']
            loser = game['loser']
            winner_conf = game['winner_conference']
            loser_conf = game['loser_conference']
            
            # Get blended ratings for expectation calculation
            winner_rating = self.weight_calc.blended_rating(
                prev_ratings.get(winner, 0.5), 
                games_played.get(winner, 1)
            )
            loser_rating = self.weight_calc.blended_rating(
                prev_ratings.get(loser, 0.5),
                games_played.get(loser, 1)
            )
            
            # Calculate all weights for this game
            game_weights = self.weight_calc.calculate_game_weights(
                game.to_dict(), current_week, winner_rating, loser_rating
            )
            
            # Add team graph edges (both directions)
            credit_weight = game_weights['final_credit']
            penalty_weight = game_weights['final_penalty']
            
            # Loser -> Winner (credit edge)
            if G_team.has_edge(loser, winner):
                G_team[loser][winner]['weight'] += credit_weight
            else:
                G_team.add_edge(loser, winner, weight=credit_weight)
            
            # Winner -> Loser (penalty edge)  
            if G_team.has_edge(winner, loser):
                G_team[winner][loser]['weight'] += penalty_weight
            else:
                G_team.add_edge(winner, loser, weight=penalty_weight)
            
            # Add conference graph edges (cross-conference only)
            if (game.get('cross_conf', False) and 
                winner_conf and loser_conf and 
                winner_conf != loser_conf):
                
                conf_weight = game_weights['cross_conf_credit']
                
                # Only loser_conf -> winner_conf edge
                if G_conf.has_edge(loser_conf, winner_conf):
                    G_conf[loser_conf][winner_conf]['weight'] += conf_weight
                else:
                    G_conf.add_edge(loser_conf, winner_conf, weight=conf_weight)
        
        self.logger.info(f"Built graphs: {len(G_team.nodes)} teams, {len(G_conf.nodes)} conferences")
        self.logger.info(f"Team edges: {len(G_team.edges)}, Conference edges: {len(G_conf.edges)}")
        
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
            u_conf = team_to_conf.get(u)
            v_conf = team_to_conf.get(v)
            
            # Check if this is an intra-conference edge
            if u_conf and v_conf and u_conf == v_conf:
                conf_strength = conf_ratings.get(u_conf, 0.5)
                sqrt_strength = np.sqrt(conf_strength)
                
                # Multiply existing weight by sqrt(conference strength)
                data['weight'] *= sqrt_strength
                edges_modified += 1
        
        self.logger.info(f"Injected conference strength into {edges_modified} intra-conference edges")
    
    def _get_team_conference_mapping(self, G_team: nx.DiGraph) -> Dict[str, str]:
        """
        Extract team-to-conference mapping from team names or external data
        This is a simplified version - in production, you'd load this from the API
        """
        # This would typically come from the teams API data
        # For now, return empty dict - conference injection will be handled differently
        return {}
    
    def rebuild_full_graph(self, games_df: pd.DataFrame, current_ratings: Dict,
                          hindsight: bool = True) -> Tuple[nx.DiGraph, nx.DiGraph]:
        """
        Rebuild graphs for retro pipeline with hindsight ratings
        No shrinkage applied, uses final ratings for all calculations
        """
        # Initialize graphs
        G_conf = nx.DiGraph()
        G_team = nx.DiGraph()
        
        # Get all teams and conferences
        teams = set(games_df['winner'].tolist() + games_df['loser'].tolist())
        conferences = set()
        for conf in games_df['winner_conference'].dropna().tolist() + games_df['loser_conference'].dropna().tolist():
            if conf:
                conferences.add(conf)
        
        # Add nodes
        G_team.add_nodes_from(teams)
        G_conf.add_nodes_from(conferences)
        
        # Process each game with hindsight ratings (no shrinkage)
        for _, game in games_df.iterrows():
            winner = game['winner']
            loser = game['loser']
            winner_conf = game['winner_conference']
            loser_conf = game['loser_conference']
            
            # Use current ratings directly (no shrinkage in hindsight)
            winner_rating = current_ratings.get(winner, 0.5)
            loser_rating = current_ratings.get(loser, 0.5)
            
            # Calculate weights (assuming current week is final week)
            game_weights = self.weight_calc.calculate_game_weights(
                game.to_dict(), 15, winner_rating, loser_rating  # Using week 15 as "current"
            )
            
            # Add team graph edges
            credit_weight = game_weights['final_credit']
            penalty_weight = game_weights['final_penalty']
            
            if G_team.has_edge(loser, winner):
                G_team[loser][winner]['weight'] += credit_weight
            else:
                G_team.add_edge(loser, winner, weight=credit_weight)
            
            if G_team.has_edge(winner, loser):
                G_team[winner][loser]['weight'] += penalty_weight
            else:
                G_team.add_edge(winner, loser, weight=penalty_weight)
            
            # Add conference graph edges (cross-conference only)
            if (game.get('cross_conf', False) and 
                winner_conf and loser_conf and 
                winner_conf != loser_conf):
                
                conf_weight = game_weights['cross_conf_credit']
                
                if G_conf.has_edge(loser_conf, winner_conf):
                    G_conf[loser_conf][winner_conf]['weight'] += conf_weight
                else:
                    G_conf.add_edge(loser_conf, winner_conf, weight=conf_weight)
        
        return G_conf, G_team

def inject_conf_strength(G_team: nx.DiGraph, S: Dict, S_prev: Dict, config: Dict = None):
    """Convenience function for conference strength injection"""
    if config is None:
        import yaml
        with open('config.yaml', 'r') as f:
            config = yaml.safe_load(f)
    
    builder = GraphBuilder(config)
    builder.inject_conf_strength(G_team, S, S_prev)

def rebuild_full_graph(games_df: pd.DataFrame, R: Dict, hindsight: bool = True, 
                      config: Dict = None) -> Tuple[nx.DiGraph, nx.DiGraph]:
    """Convenience function for full graph rebuilding"""
    if config is None:
        import yaml
        with open('config.yaml', 'r') as f:
            config = yaml.safe_load(f)
    
    builder = GraphBuilder(config)
    return builder.rebuild_full_graph(games_df, R, hindsight)
