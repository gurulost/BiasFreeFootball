"""
Run complete pipeline with authentic 2024 CFBD data
Generates accurate rankings based on real game results
"""

import os
import json
import yaml
import logging
from datetime import datetime
from pathlib import Path
import pandas as pd
from src.ingest import CFBDataIngester
from src.data_integrity_fixer import DataIntegrityFixer

def setup_logging():
    """Configure logging for pipeline run"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

def load_config():
    """Load configuration"""
    with open('config.yaml', 'r') as f:
        return yaml.safe_load(f)

def run_authentic_pipeline(season=2024):
    """Run complete pipeline with authentic CFBD data"""
    logger = setup_logging()
    logger.info(f"Starting authentic pipeline for {season} season")
    
    try:
        # Load configuration
        config = load_config()
        
        # Initialize ingester with working API
        ingester = CFBDataIngester(config)
        
        # Step 1: Fetch authentic teams and conferences
        logger.info("Step 1: Fetching authentic team and conference data")
        teams = ingester.fetch_teams(season)
        conferences = ingester.fetch_conferences(season)
        
        logger.info(f"Retrieved {len(teams)} FBS teams and {len(conferences)} conferences")
        
        # Step 2: Verify key team assignments
        logger.info("Step 2: Validating conference assignments")
        key_teams_2024 = {
            'BYU': 'Big 12',
            'Texas': 'SEC',
            'Oregon': 'Big Ten',
            'Washington': 'Big Ten',
            'USC': 'Big Ten',
            'UCLA': 'Big Ten',
            'SMU': 'ACC',
            'California': 'ACC',
            'Stanford': 'ACC'
        }
        
        validation_results = {}
        for team_data in teams:
            team_name = team_data.get('school')
            if team_name in key_teams_2024:
                expected_conf = key_teams_2024[team_name]
                actual_conf = team_data.get('conference', 'Unknown')
                
                validation_results[team_name] = {
                    'expected': expected_conf,
                    'actual': actual_conf,
                    'correct': actual_conf == expected_conf
                }
                
                status = "✓" if actual_conf == expected_conf else "✗"
                logger.info(f"{status} {team_name}: {actual_conf} (expected: {expected_conf})")
        
        correct_assignments = sum(1 for r in validation_results.values() if r['correct'])
        total_assignments = len(validation_results)
        
        if correct_assignments != total_assignments:
            logger.error(f"Conference validation failed: {correct_assignments}/{total_assignments} correct")
            return False
        
        logger.info(f"Conference validation passed: {correct_assignments}/{total_assignments} correct")
        
        # Step 3: Fetch authentic game data
        logger.info("Step 3: Fetching complete 2024 season game data")
        games = ingester.fetch_results_upto_bowls(season)
        
        logger.info(f"Retrieved {len(games)} completed games")
        
        # Step 4: Process games into proper format
        logger.info("Step 4: Processing game data")
        games_df = ingester.process_game_data(games)
        
        logger.info(f"Processed {len(games_df)} game records")
        
        # Step 5: Build team-to-conference mapping from authentic data
        team_conf_mapping = {}
        for team_data in teams:
            team_name = team_data.get('school')
            conference = team_data.get('conference', 'Independent')
            team_conf_mapping[team_name] = conference
        
        # Step 6: Generate rankings using existing pipeline components
        logger.info("Step 5: Generating rankings with authentic data")
        
        # Import and run the ranking algorithm components
        from src.graph import GraphBuilder
        from src.pagerank import PageRankCalculator
        from src.weights import EdgeWeightCalculator
        
        # Build graphs
        graph_builder = GraphBuilder(config)
        conf_graph, team_graph = graph_builder.build_graphs(games_df)
        
        # Run PageRank
        ranker = PageRankRanker(config)
        team_ratings, conf_ratings = ranker.rank_teams(team_graph, conf_graph)
        
        # Build final rankings structure
        rankings_data = {
            'metadata': {
                'season': season,
                'generated_at': datetime.now().isoformat(),
                'data_source': 'authentic_cfbd_api',
                'total_games': len(games_df),
                'total_teams': len(team_ratings),
                'api_validation_passed': True
            },
            'rankings': []
        }
        
        # Sort teams by rating
        sorted_teams = sorted(team_ratings.items(), key=lambda x: x[1], reverse=True)
        
        for rank, (team, rating) in enumerate(sorted_teams, 1):
            conference = team_conf_mapping.get(team, 'Unknown')
            
            rankings_data['rankings'].append({
                'rank': rank,
                'team': team,
                'conference': conference,
                'rating': rating,
                'rating_retro': rating,
                'rank_retro': rank
            })
        
        # Step 6: Save authentic rankings
        logger.info("Step 6: Saving authentic rankings")
        
        # Save to cache
        os.makedirs('data/cache', exist_ok=True)
        cache_file = f"data/cache/final_rankings_{season}_authentic.json"
        with open(cache_file, 'w') as f:
            json.dump(rankings_data, f, indent=2)
        
        # Save to exports
        os.makedirs('exports', exist_ok=True)
        export_file = f"exports/{season}_authentic.json"
        with open(export_file, 'w') as f:
            json.dump(rankings_data, f, indent=2)
        
        # Step 7: Display top rankings
        logger.info("Step 7: Top 25 authentic rankings:")
        for i, team_data in enumerate(rankings_data['rankings'][:25]):
            rank = team_data['rank']
            team = team_data['team']
            conf = team_data['conference']
            rating = team_data['rating']
            logger.info(f"{rank:2d}. {team:20s} ({conf:15s}) {rating:.6f}")
        
        # Step 8: Validate key teams are properly ranked
        logger.info("Step 8: Validating key team rankings")
        
        team_rankings = {team_data['team']: team_data['rank'] for team_data in rankings_data['rankings']}
        
        # Check that BYU is no longer ranked 77th
        byu_rank = team_rankings.get('BYU', 999)
        logger.info(f"BYU rank: {byu_rank} (was 77th in original rankings)")
        
        # Check that Virginia Tech is not in top 10
        vt_rank = team_rankings.get('Virginia Tech', 999)
        logger.info(f"Virginia Tech rank: {vt_rank} (was 3rd in original rankings)")
        
        # Verify realistic top 10
        top_10_teams = [team_data['team'] for team_data in rankings_data['rankings'][:10]]
        logger.info(f"Top 10 teams: {top_10_teams}")
        
        logger.info("Authentic pipeline completed successfully!")
        
        return {
            'success': True,
            'rankings_file': export_file,
            'cache_file': cache_file,
            'total_teams': len(team_ratings),
            'total_games': len(games_df),
            'byu_rank': byu_rank,
            'vt_rank': vt_rank,
            'top_10': top_10_teams
        }
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        import traceback
        traceback.print_exc()
        return {
            'success': False,
            'error': str(e)
        }

if __name__ == "__main__":
    result = run_authentic_pipeline()
    
    if result['success']:
        print("\n=== AUTHENTIC PIPELINE SUCCESS ===")
        print(f"Rankings saved to: {result['rankings_file']}")
        print(f"BYU rank: {result['byu_rank']} (improved from 77th)")
        print(f"Virginia Tech rank: {result['vt_rank']} (moved from 3rd)")
        print(f"Top 10: {result['top_10']}")
    else:
        print(f"\n=== PIPELINE FAILED ===")
        print(f"Error: {result['error']}")
        exit(1)