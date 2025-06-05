"""
Deep analysis of BYU ranking issue
Examine specific games, edge weights, and PageRank flow
"""

import json
import yaml
import pandas as pd
from src.ingest import CFBDataIngester
from src.validation import DataValidator
from src.graph import GraphBuilder
from src.pagerank import PageRankCalculator

def analyze_byu_detailed():
    """Detailed analysis of BYU's 10-2 season and low ranking"""
    
    print("=== DETAILED BYU ANALYSIS ===\n")
    
    # Load config
    with open('config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    # Get processed data
    ingester = CFBDataIngester(config)
    games_data = ingester.fetch_results_upto_bowls(2024)
    
    validator = DataValidator(config)
    canonical_teams = ingester._load_canonical_teams()
    games_df = validator.validate_complete_dataset(games_data, canonical_teams, 2024)
    
    # Filter BYU games
    byu_games = games_df[
        (games_df['home_team'] == 'BYU') | (games_df['away_team'] == 'BYU')
    ].copy()
    
    print(f"BYU played {len(byu_games)} games in 2024:")
    print("-" * 80)
    
    wins = 0
    losses = 0
    
    for _, game in byu_games.iterrows():
        if game['winner'] == 'BYU':
            wins += 1
            outcome = "W"
            opponent = game['loser']
            score = f"{game['winner_score']}-{game['loser_score']}"
        else:
            losses += 1
            outcome = "L"
            opponent = game['winner']
            score = f"{game['loser_score']}-{game['winner_score']}"
        
        venue = "vs" if game['home_team'] == 'BYU' else "@"
        if game['neutral_site']:
            venue = "N"
        
        print(f"Week {game['week']:2d}: {outcome} {venue} {opponent:20s} {score:8s} (margin: {game['margin']:2d})")
    
    print(f"\nBYU Record: {wins}-{losses}")
    print(f"Expected ranking for 10-2: Top 20-25")
    print(f"Actual ranking: #95")
    
    # Analyze opponent strength
    print(f"\n=== OPPONENT ANALYSIS ===")
    
    # Load current rankings to see opponent strength
    try:
        with open('exports/2024_authentic.json', 'r') as f:
            rankings_data = json.load(f)
        
        team_rankings = {}
        for rank, team_data in enumerate(rankings_data['team_rankings'], 1):
            team_rankings[team_data['team']] = rank
        
        print("BYU's opponents and their rankings:")
        for _, game in byu_games.iterrows():
            if game['winner'] == 'BYU':
                opponent = game['loser']
                result = "W"
            else:
                opponent = game['winner']
                result = "L"
            
            opp_rank = team_rankings.get(opponent, "NR")
            print(f"  {result} vs {opponent:20s} (Rank: {opp_rank})")
        
    except FileNotFoundError:
        print("Rankings file not found")
    
    # Analyze graph structure around BYU
    print(f"\n=== GRAPH ANALYSIS ===")
    
    graph_builder = GraphBuilder(config)
    conf_graph, team_graph = graph_builder.build_graphs(games_df)
    
    # Get BYU's incoming and outgoing edges
    print("BYU's graph connections:")
    
    byu_in_edges = list(team_graph.in_edges('BYU', data=True))
    byu_out_edges = list(team_graph.out_edges('BYU', data=True))
    
    print(f"Incoming edges (teams that lost to BYU): {len(byu_in_edges)}")
    for source, target, data in sorted(byu_in_edges, key=lambda x: x[2]['weight'], reverse=True):
        weight = data['weight']
        print(f"  {source:20s} -> BYU: weight={weight:.4f}")
    
    print(f"\nOutgoing edges (teams BYU lost to): {len(byu_out_edges)}")
    for source, target, data in sorted(byu_out_edges, key=lambda x: x[2]['weight'], reverse=True):
        weight = data['weight']
        print(f"  BYU -> {target:20s}: weight={weight:.4f}")
    
    # Calculate PageRank and examine distribution
    ranker = PageRankCalculator(config)
    team_ratings = ranker.pagerank(team_graph)
    
    print(f"\n=== PAGERANK ANALYSIS ===")
    print(f"BYU PageRank score: {team_ratings['BYU']:.6f}")
    
    # Compare with other 10-2 teams if any
    byu_rating = team_ratings['BYU']
    sorted_ratings = sorted(team_ratings.items(), key=lambda x: x[1], reverse=True)
    
    print(f"\nTeams with similar PageRank scores to BYU:")
    for i, (team, rating) in enumerate(sorted_ratings):
        if abs(rating - byu_rating) < 0.0005:  # Close ratings
            rank = i + 1
            print(f"  {rank:3d}. {team:20s} {rating:.6f}")
    
    # Look for algorithmic issues
    print(f"\n=== POTENTIAL ISSUES ===")
    
    # Check if BYU is hurt by playing weak opponents
    avg_in_weight = sum(data['weight'] for _, _, data in byu_in_edges) / len(byu_in_edges) if byu_in_edges else 0
    avg_out_weight = sum(data['weight'] for _, _, data in byu_out_edges) / len(byu_out_edges) if byu_out_edges else 0
    
    print(f"Average incoming edge weight: {avg_in_weight:.4f}")
    print(f"Average outgoing edge weight: {avg_out_weight:.4f}")
    
    if avg_out_weight > avg_in_weight:
        print("⚠️  BYU loses more PageRank than it gains (outgoing > incoming)")
        print("   This suggests losses are weighted too heavily vs wins")
    
    # Check total PageRank flow
    total_in_flow = sum(data['weight'] for _, _, data in byu_in_edges)
    total_out_flow = sum(data['weight'] for _, _, data in byu_out_edges)
    
    print(f"Total PageRank inflow: {total_in_flow:.4f}")
    print(f"Total PageRank outflow: {total_out_flow:.4f}")
    print(f"Net PageRank flow: {total_in_flow - total_out_flow:.4f}")
    
    if total_out_flow > total_in_flow:
        print("❌ BYU has negative net PageRank flow despite 10-2 record")
        print("   This indicates fundamental algorithm bias against teams with any losses")

if __name__ == "__main__":
    analyze_byu_detailed()