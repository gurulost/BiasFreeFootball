"""
Live weekly pipeline implementation
Runs every Sunday to update rankings with latest results
"""

import yaml
import logging
from datetime import datetime
from typing import Dict, Tuple, List
import pandas as pd

from src.ingest import fetch_results_upto_week
from src.graph import GraphBuilder, inject_conf_strength
from src.pagerank import pagerank
from src.bias_audit import BiasAudit
from src.storage import Storage
from src.publish import Publisher

class LivePipeline:
    def __init__(self, config_path: str = 'config.yaml'):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.logger = logging.getLogger(__name__)
        self.storage = Storage()
        self.bias_audit = BiasAudit(self.config)
        self.publisher = Publisher(self.config)
        self.graph_builder = GraphBuilder(self.config)
    
    def run_live(self, week: int, season: int) -> Dict:
        """
        Run the complete live pipeline for a given week
        
        Args:
            week: Week number (1-15 for regular season)
            season: Year (e.g., 2023)
            
        Returns:
            Dictionary with pipeline results and metrics
        """
        self.logger.info(f"Starting live pipeline for Week {week}, {season}")
        
        try:
            # 1. Ingest data
            self.logger.info("Step 1: Ingesting game data")
            games_df = fetch_results_upto_week(week, season, self.config)
            self.logger.info(f"Loaded {len(games_df)} completed games")
            
            if games_df.empty:
                self.logger.warning("No game data available")
                return {'success': False, 'error': 'No game data'}
            
            # 2. Load previous ratings
            self.logger.info("Step 2: Loading previous ratings")
            S_prev, R_prev = self.storage.load_prev_ratings(week, season)
            
            # 3. Build graphs
            self.logger.info("Step 3: Building team and conference graphs")
            G_conf, G_team = self.graph_builder.build_graphs(
                games_df, R_prev, week
            )
            
            # 4. Stage-1 PageRank (conference)
            self.logger.info("Step 4: Computing conference PageRank")
            S = pagerank(G_conf, 
                        damping=self.config['pagerank']['damping'],
                        config=self.config)
            
            if not S:
                self.logger.warning("Conference PageRank returned empty results")
                S = {conf: 0.5 for conf in G_conf.nodes()}
            
            # 5. Stage-2 PageRank (team) with conference strength injection
            self.logger.info("Step 5: Injecting conference strength and computing team PageRank")
            inject_conf_strength(G_team, S, S_prev, self.config)
            
            R = pagerank(G_team,
                        damping=self.config['pagerank']['damping'],
                        config=self.config)
            
            if not R:
                self.logger.error("Team PageRank returned empty results")
                return {'success': False, 'error': 'PageRank computation failed'}
            
            # 6. Bias audit
            self.logger.info("Step 6: Running bias audit")
            bias_metrics = self.bias_audit.compute_detailed_metrics(R, S, week)
            B = bias_metrics['neutrality_metric']
            
            self.logger.info(f"Neutrality metric B = {B:.4f}")
            
            # Auto-tune if bias is too high
            if B > self.config['bias_audit']['auto_tune_threshold']:
                self.logger.warning(f"Bias threshold exceeded: {B:.4f} > {self.config['bias_audit']['auto_tune_threshold']}")
                new_lambda = self.bias_audit.auto_tune_lambda()
                self.logger.info(f"Suggested lambda adjustment: {new_lambda:.4f}")
            
            # 7. Persist data
            self.logger.info("Step 7: Persisting ratings and metrics")
            self.storage.save_ratings(S, R, week, season)
            self.storage.save_bias_metrics(bias_metrics, week, season)
            
            # 8. Publish results
            self.logger.info("Step 8: Publishing results")
            publish_results = self.publisher.weekly_csv_json(S, R, week, season, B)
            
            # Prepare result summary
            result = {
                'success': True,
                'week': week,
                'season': season,
                'timestamp': datetime.now().isoformat(),
                'metrics': {
                    'games_processed': len(games_df),
                    'teams_ranked': len(R),
                    'conferences_ranked': len(S),
                    'neutrality_metric': B,
                    'bias_threshold_passed': B <= self.config['bias_audit']['threshold']
                },
                'top_teams': self._get_top_teams(R, 25),
                'bias_metrics': bias_metrics,
                'published_files': publish_results
            }
            
            self.logger.info(f"Live pipeline completed successfully for Week {week}, {season}")
            return result
            
        except Exception as e:
            self.logger.error(f"Live pipeline failed: {e}", exc_info=True)
            return {
                'success': False,
                'error': str(e),
                'week': week,
                'season': season,
                'timestamp': datetime.now().isoformat()
            }
    
    def _get_top_teams(self, ratings: Dict, n: int = 25) -> List[Dict]:
        """Get top N teams with rankings"""
        if not ratings:
            return []
        
        # Sort teams by rating (descending)
        sorted_teams = sorted(ratings.items(), key=lambda x: x[1], reverse=True)
        
        top_teams = []
        for rank, (team, rating) in enumerate(sorted_teams[:n], 1):
            top_teams.append({
                'rank': rank,
                'team': team,
                'rating': rating
            })
        
        return top_teams

def run_live(week: int, season: int, config_path: str = 'config.yaml') -> Dict:
    """
    Convenience function to run live pipeline
    
    Args:
        week: Week number
        season: Season year
        config_path: Path to configuration file
        
    Returns:
        Pipeline execution results
    """
    pipeline = LivePipeline(config_path)
    return pipeline.run_live(week, season)

def run_current_week(season: int = None, config_path: str = 'config.yaml') -> Dict:
    """
    Run pipeline for current week (auto-detect)
    
    Args:
        season: Season year (defaults to current year)
        config_path: Path to configuration file
        
    Returns:
        Pipeline execution results
    """
    if season is None:
        season = datetime.now().year
    
    # Simple week detection - in production, this would be more sophisticated
    current_date = datetime.now()
    
    # College football season roughly runs from late August to early December
    # Week 1 is typically the first Saturday in September
    if current_date.month >= 9 or (current_date.month == 8 and current_date.day > 25):
        # Rough week calculation
        if current_date.month == 8:
            week = 1
        elif current_date.month == 9:
            week = min(4, current_date.day // 7)
        elif current_date.month == 10:
            week = 4 + min(5, current_date.day // 7)
        elif current_date.month == 11:
            week = 9 + min(4, current_date.day // 7)
        else:  # December
            week = 13
    else:
        # Off-season
        week = 1
    
    return run_live(week, season, config_path)

if __name__ == "__main__":
    # Command-line execution
    import sys
    
    if len(sys.argv) >= 3:
        week = int(sys.argv[1])
        season = int(sys.argv[2])
    else:
        # Run current week
        result = run_current_week()
        print(f"Pipeline result: {result}")
        sys.exit(0)
    
    result = run_live(week, season)
    
    if result['success']:
        print(f"✅ Live pipeline completed successfully")
        print(f"Week {result['week']}, {result['season']}")
        print(f"Neutrality metric: {result['metrics']['neutrality_metric']:.4f}")
        print(f"Top 5 teams:")
        for team_data in result['top_teams'][:5]:
            print(f"  {team_data['rank']}. {team_data['team']} ({team_data['rating']:.4f})")
    else:
        print(f"❌ Live pipeline failed: {result['error']}")
        sys.exit(1)
