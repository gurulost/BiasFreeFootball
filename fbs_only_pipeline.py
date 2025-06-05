"""
FBS-only pipeline for authentic 2024 college football rankings
Filters College Football Data API for FBS teams and games only
"""

import logging
import yaml
from datetime import datetime
from src.ingest import CFBDataIngester
from src.retro_pipeline import run_retro
from src.live_pipeline import run_live
from src.season_utils import get_pipeline_recommendation, should_use_retro_rankings

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_fbs_only_pipeline(season=2024, week=15):
    """Run pipeline with FBS teams and games only"""
    
    logger.info(f"Starting FBS-only pipeline for season {season}")
    
    # Load configuration
    with open('config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    # Initialize data ingester with FBS filtering
    ingester = CFBDataIngester(config)
    
    # Get intelligent pipeline recommendation
    recommendation = get_pipeline_recommendation()
    
    try:
        if should_use_retro_rankings():
            # Between seasons - use RETRO pipeline for definitive rankings
            logger.info(f"Off-season detected - running RETRO pipeline for {recommendation['season']}")
            result = run_retro(season=recommendation['season'], max_outer=6)
            
            if result['success']:
                logger.info(f"RETRO pipeline completed successfully")
                logger.info(f"EM Convergence: {result['em_convergence']}")
                logger.info(f"Teams ranked: {len(result.get('team_ratings', {}))}")
                logger.info(f"Neutrality metric: {result['metrics'].get('neutrality_metric', 'N/A')}")
                return result
            else:
                logger.error(f"RETRO pipeline failed: {result['error']}")
                return result
                
        else:
            # Active season - use live pipeline
            logger.info(f"Active season detected - running live pipeline for week {recommendation['week']}")
            result = run_live(week=recommendation['week'], season=recommendation['season'])
            
            if result['success']:
                logger.info(f"Live pipeline completed successfully")
                logger.info(f"Teams ranked: {len(result.get('team_ratings', {}))}")
                logger.info(f"Games processed: {result['metrics'].get('games_processed', 'N/A')}")
                logger.info(f"Neutrality metric: {result['metrics'].get('neutrality_metric', 'N/A')}")
                return result
            else:
                logger.error(f"Live pipeline failed: {result['error']}")
                return result
                
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        return {
            'success': False,
            'error': str(e)
        }

if __name__ == "__main__":
    # Run the FBS-only pipeline
    result = run_fbs_only_pipeline()
    
    if result['success']:
        print("\n=== FBS-Only Pipeline Completed Successfully ===")
        print(f"Pipeline type: {result.get('pipeline_type', 'Unknown')}")
        print(f"Season: {result.get('season', 'Unknown')}")
        
        if 'team_ratings' in result:
            # Display top 25 FBS teams
            team_ratings = result['team_ratings']
            sorted_teams = sorted(team_ratings.items(), key=lambda x: x[1], reverse=True)
            
            print("\n=== Top 25 FBS Teams ===")
            for i, (team, rating) in enumerate(sorted_teams[:25], 1):
                print(f"{i:2d}. {team:<25} {rating:.6f}")
        
        if 'metrics' in result:
            metrics = result['metrics']
            print(f"\nNeutrality Metric: {metrics.get('neutrality_metric', 'N/A')}")
            print(f"Games Processed: {metrics.get('games_processed', 'N/A')}")
            print(f"Teams Ranked: {metrics.get('teams_ranked', 'N/A')}")
            
    else:
        print(f"\n=== Pipeline Failed ===")
        print(f"Error: {result['error']}")