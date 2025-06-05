"""
Debug ranking calculation issues - particularly BYU at #95 with 10-2 record
"""

import json
import yaml
import logging
from src.ingest import CFBDataIngester
from src.validation import DataValidator
from src.graph import GraphBuilder
from src.pagerank import PageRankCalculator

def debug_byu_ranking():
    """Debug why BYU is ranked so low"""
    
    print("=== BYU RANKING DEBUG ===\n")
    
    # Load config
    with open('config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    # Get raw games data
    ingester = CFBDataIngester(config)
    games_data = ingester.fetch_results_upto_bowls(2024)
    
    print(f"Raw games retrieved: {len(games_data)}")
    
    # Find BYU in raw data
    byu_raw_games = [g for g in games_data if g.get('homeTeam') == 'BYU' or g.get('awayTeam') == 'BYU']
    print(f"BYU raw games found: {len(byu_raw_games)}")
    
    # Process games through validator
    validator = DataValidator(config)
    canonical_teams = ingester._load_canonical_teams()
    
    try:
        games_df = validator.validate_complete_dataset(games_data, canonical_teams, 2024)
        print(f"Processed games DataFrame: {games_df.shape}")
        print(f"Columns: {list(games_df.columns)}")
        
        # Check if BYU exists in processed data
        byu_processed = games_df[
            (games_df['home_team'] == 'BYU') | (games_df['away_team'] == 'BYU')
        ]
        print(f"BYU processed games: {len(byu_processed)}")
        
        if len(byu_processed) == 0:
            print("ERROR: BYU lost during data processing!")
            
            # Check what happened to BYU
            all_teams = set(games_df['home_team'].unique()) | set(games_df['away_team'].unique())
            print(f"Total teams in processed data: {len(all_teams)}")
            
            # Look for BYU variants
            byu_variants = [team for team in all_teams if 'BYU' in team or 'Brigham' in team.upper()]
            print(f"BYU-like teams: {byu_variants}")
            
            return False
        
        # Build graph and check BYU's connections
        graph_builder = GraphBuilder(config)
        conf_graph, team_graph = graph_builder.build_graphs(games_df)
        
        print(f"\nGraph built:")
        print(f"Team graph nodes: {team_graph.number_of_nodes()}")
        print(f"Team graph edges: {team_graph.number_of_edges()}")
        
        # Check BYU's graph position
        if 'BYU' in team_graph.nodes():
            byu_edges = list(team_graph.edges('BYU', data=True))
            print(f"BYU graph edges: {len(byu_edges)}")
            
            # Show BYU's edge weights
            print("\nBYU edge analysis:")
            for source, target, data in byu_edges[:5]:  # Show first 5
                weight = data.get('weight', 0)
                print(f"  {source} -> {target}: weight={weight:.4f}")
        else:
            print("ERROR: BYU not found in team graph!")
            return False
        
        # Run PageRank and check intermediate results
        ranker = PageRankCalculator(config)
        team_ratings = ranker.pagerank(team_graph)
        
        if 'BYU' in team_ratings:
            byu_rating = team_ratings['BYU']
            print(f"\nBYU PageRank rating: {byu_rating:.6f}")
            
            # Compare to other teams
            sorted_ratings = sorted(team_ratings.items(), key=lambda x: x[1], reverse=True)
            byu_rank = next(i for i, (team, _) in enumerate(sorted_ratings, 1) if team == 'BYU')
            print(f"BYU rank: #{byu_rank}")
            
            # Show teams around BYU's ranking
            print(f"\nTeams around BYU's rank:")
            start_idx = max(0, byu_rank - 6)
            end_idx = min(len(sorted_ratings), byu_rank + 5)
            
            for i in range(start_idx, end_idx):
                team, rating = sorted_ratings[i]
                rank = i + 1
                marker = " <-- BYU" if team == 'BYU' else ""
                print(f"  {rank:3d}. {team:20s} {rating:.6f}{marker}")
        
        else:
            print("ERROR: BYU not found in PageRank results!")
            return False
        
        return True
        
    except Exception as e:
        print(f"Error during processing: {e}")
        import traceback
        traceback.print_exc()
        return False

def check_data_integrity():
    """Check if data processing is losing games or teams"""
    
    print("\n=== DATA INTEGRITY CHECK ===\n")
    
    with open('config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    ingester = CFBDataIngester(config)
    
    # Raw data check
    games_data = ingester.fetch_results_upto_bowls(2024)
    print(f"Raw games: {len(games_data)}")
    
    raw_teams = set()
    for game in games_data:
        raw_teams.add(game.get('homeTeam'))
        raw_teams.add(game.get('awayTeam'))
    
    print(f"Raw unique teams: {len(raw_teams)}")
    print(f"BYU in raw teams: {'BYU' in raw_teams}")
    
    # Processed data check
    try:
        validator = DataValidator(config)
        canonical_teams = ingester._load_canonical_teams()
        games_df = validator.validate_complete_dataset(games_data, canonical_teams, 2024)
        
        processed_teams = set(games_df['home_team'].unique()) | set(games_df['away_team'].unique())
        print(f"Processed unique teams: {len(processed_teams)}")
        print(f"BYU in processed teams: {'BYU' in processed_teams}")
        
        # Teams lost during processing
        lost_teams = raw_teams - processed_teams
        if lost_teams:
            print(f"Teams lost during processing: {len(lost_teams)}")
            print(f"Lost teams sample: {list(lost_teams)[:10]}")
        
        return len(processed_teams) > 100  # Should have ~130 FBS teams
        
    except Exception as e:
        print(f"Processing error: {e}")
        return False

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Check data integrity first
    data_ok = check_data_integrity()
    
    if data_ok:
        # Debug BYU ranking
        byu_debug_ok = debug_byu_ranking()
        
        if not byu_debug_ok:
            print("\n❌ BYU ranking debug failed")
        else:
            print("\n✅ BYU ranking debug completed")
    else:
        print("\n❌ Data integrity issues detected")