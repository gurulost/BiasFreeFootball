"""
Retro (retrospective) pipeline implementation
Runs once per season after CFP championship for definitive rankings
"""

import yaml
import logging
import numpy as np
from datetime import datetime
from typing import Dict, Tuple

from src.ingest import fetch_results_upto_bowls
from src.graph import GraphBuilder, rebuild_full_graph
from src.pagerank import pagerank
from src.storage import Storage
from src.publish import Publisher

class RetroPipeline:
    def __init__(self, config_path: str = 'config.yaml'):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.logger = logging.getLogger(__name__)
        self.storage = Storage()
        self.publisher = Publisher(self.config)
        self.graph_builder = GraphBuilder(self.config)
    
    def run_retro(self, season: int, max_outer: int = None) -> Dict:
        """
        Run the complete retrospective pipeline for a season
        Uses EM-style iteration until convergence
        
        Args:
            season: Season year (e.g., 2023)
            max_outer: Maximum outer iterations (default from config)
            
        Returns:
            Dictionary with pipeline results and final ratings
        """
        if max_outer is None:
            max_outer = self.config['pipeline']['retro']['max_outer_iterations']
        
        convergence_threshold = self.config['pipeline']['retro']['convergence_threshold']
        
        self.logger.info(f"Starting retro pipeline for {season} season")
        
        try:
            # 1. Ingest all season data including bowls
            self.logger.info("Step 1: Ingesting complete season data")
            games_df = fetch_results_upto_bowls(season, self.config)
            self.logger.info(f"Loaded {len(games_df)} completed games including bowls")
            
            if games_df.empty:
                self.logger.warning("No game data available")
                return {'success': False, 'error': 'No game data'}
            
            # 2. Initialize with final live ratings for faster convergence
            self.logger.info("Step 2: Loading final live ratings as starting point")
            try:
                _, initial_R = self.storage.load_ratings(week="post_cfp", season=season)
            except:
                # Fallback to uniform ratings if no live ratings available
                teams = set(games_df['winner'].tolist() + games_df['loser'].tolist())
                initial_R = {team: 0.5 for team in teams}
                self.logger.warning("Using uniform initial ratings")
            
            R = initial_R.copy()
            
            # 3. EM-style iteration loop
            self.logger.info("Step 3: Starting EM convergence loop")
            
            iteration_history = []
            
            for outer_iter in range(max_outer):
                self.logger.info(f"EM iteration {outer_iter + 1}/{max_outer}")
                
                # Rebuild graphs with current ratings (hindsight mode)
                G_conf, G_team = rebuild_full_graph(games_df, R, hindsight=True, config=self.config)
                
                # Stage-1: Conference PageRank
                S = pagerank(G_conf, 
                           damping=self.config['pagerank']['damping'],
                           config=self.config)
                
                if not S:
                    # Handle empty conference graph
                    conferences = set(games_df['winner_conference'].dropna().tolist() + 
                                    games_df['loser_conference'].dropna().tolist())
                    S = {conf: 0.5 for conf in conferences if conf}
                
                # Stage-2: Team PageRank with conference injection
                # Use same S for both current and previous (no temporal distinction in hindsight)
                self.graph_builder.inject_conf_strength(G_team, S, S)
                
                R_new = pagerank(G_team,
                               damping=self.config['pagerank']['damping'],
                               config=self.config)
                
                if not R_new:
                    self.logger.error("Team PageRank failed in retro pipeline")
                    break
                
                # Check convergence
                if R:  # Not first iteration
                    # Calculate maximum change in ratings
                    common_teams = set(R.keys()) & set(R_new.keys())
                    if common_teams:
                        max_change = max(abs(R_new[team] - R[team]) for team in common_teams)
                        
                        iteration_history.append({
                            'iteration': outer_iter + 1,
                            'max_change': max_change,
                            'teams_count': len(common_teams),
                            'converged': max_change < convergence_threshold
                        })
                        
                        self.logger.info(f"Max rating change: {max_change:.8f}")
                        
                        if max_change < convergence_threshold:
                            self.logger.info(f"Converged after {outer_iter + 1} iterations")
                            break
                
                R = R_new.copy()
            else:
                self.logger.warning(f"Did not converge after {max_outer} iterations")
            
            # 4. Final bias analysis (optional for retro)
            from src.bias_audit import BiasAudit
            bias_audit = BiasAudit(self.config)
            final_bias_metrics = bias_audit.compute_detailed_metrics(R, S)
            
            # 5. Persist final results
            self.logger.info("Step 4: Persisting final retro ratings")
            self.storage.save_retro_ratings(S, R, season)
            
            # 6. Publish retro results
            self.logger.info("Step 5: Publishing retro results")
            publish_results = self.publisher.retro_csv_json(S, R, season)
            
            # 7. Optional: Bootstrap uncertainty analysis
            uncertainty_results = self._bootstrap_uncertainty(games_df, R, bootstrap_samples=100)
            
            # Prepare final results
            result = {
                'success': True,
                'season': season,
                'timestamp': datetime.now().isoformat(),
                'convergence': {
                    'iterations': len(iteration_history),
                    'converged': len(iteration_history) > 0 and iteration_history[-1]['converged'],
                    'final_max_change': iteration_history[-1]['max_change'] if iteration_history else None,
                    'history': iteration_history
                },
                'metrics': {
                    'games_processed': len(games_df),
                    'teams_ranked': len(R),
                    'conferences_ranked': len(S),
                    'neutrality_metric': final_bias_metrics['neutrality_metric']
                },
                'final_rankings': self._get_final_rankings(R),
                'conference_rankings': self._get_conference_rankings(S),
                'bias_metrics': final_bias_metrics,
                'uncertainty': uncertainty_results,
                'published_files': publish_results
            }
            
            self.logger.info(f"Retro pipeline completed successfully for {season}")
            return result
            
        except Exception as e:
            self.logger.error(f"Retro pipeline failed: {e}", exc_info=True)
            return {
                'success': False,
                'error': str(e),
                'season': season,
                'timestamp': datetime.now().isoformat()
            }
    
    def _bootstrap_uncertainty(self, games_df, final_ratings: Dict, 
                             bootstrap_samples: int = 100) -> Dict:
        """
        Bootstrap sampling to estimate ranking uncertainty
        
        Args:
            games_df: Complete game data
            final_ratings: Final team ratings
            bootstrap_samples: Number of bootstrap samples
            
        Returns:
            Dictionary with uncertainty metrics
        """
        if bootstrap_samples <= 0:
            return {'enabled': False}
        
        self.logger.info(f"Running bootstrap uncertainty analysis with {bootstrap_samples} samples")
        
        # Store rankings from each bootstrap sample
        bootstrap_rankings = []
        
        try:
            for sample in range(bootstrap_samples):
                # Sample games with replacement
                sampled_games = games_df.sample(n=len(games_df), replace=True)
                
                # Run abbreviated pipeline on sampled data
                G_conf, G_team = rebuild_full_graph(sampled_games, final_ratings, 
                                                  hindsight=True, config=self.config)
                
                S_sample = pagerank(G_conf, damping=self.config['pagerank']['damping'], 
                                  config=self.config)
                if not S_sample:
                    continue
                
                self.graph_builder.inject_conf_strength(G_team, S_sample, S_sample)
                R_sample = pagerank(G_team, damping=self.config['pagerank']['damping'], 
                                  config=self.config)
                
                if R_sample:
                    # Convert to rankings
                    ranked_teams = sorted(R_sample.items(), key=lambda x: x[1], reverse=True)
                    sample_rankings = {team: rank + 1 for rank, (team, _) in enumerate(ranked_teams)}
                    bootstrap_rankings.append(sample_rankings)
            
            # Calculate uncertainty metrics
            if bootstrap_rankings:
                uncertainty_metrics = self._calculate_uncertainty_metrics(bootstrap_rankings)
                uncertainty_metrics['bootstrap_samples'] = len(bootstrap_rankings)
                uncertainty_metrics['enabled'] = True
                return uncertainty_metrics
            else:
                return {'enabled': False, 'error': 'No valid bootstrap samples'}
                
        except Exception as e:
            self.logger.warning(f"Bootstrap analysis failed: {e}")
            return {'enabled': False, 'error': str(e)}
    
    def _calculate_uncertainty_metrics(self, bootstrap_rankings: list) -> Dict:
        """Calculate uncertainty metrics from bootstrap samples"""
        all_teams = set()
        for rankings in bootstrap_rankings:
            all_teams.update(rankings.keys())
        
        team_uncertainties = {}
        
        for team in all_teams:
            team_ranks = [rankings.get(team, len(all_teams)) for rankings in bootstrap_rankings]
            
            team_uncertainties[team] = {
                'mean_rank': np.mean(team_ranks),
                'std_rank': np.std(team_ranks),
                'min_rank': min(team_ranks),
                'max_rank': max(team_ranks),
                'confidence_interval_95': (
                    np.percentile(team_ranks, 2.5),
                    np.percentile(team_ranks, 97.5)
                )
            }
        
        # Overall uncertainty metrics
        all_stds = [data['std_rank'] for data in team_uncertainties.values()]
        
        return {
            'team_uncertainties': team_uncertainties,
            'overall_uncertainty': {
                'mean_std': np.mean(all_stds),
                'median_std': np.median(all_stds),
                'max_std': max(all_stds),
                'teams_with_low_uncertainty': sum(1 for std in all_stds if std <= 0.8)
            }
        }
    
    def _get_final_rankings(self, ratings: Dict, n: int = None) -> list:
        """Get final team rankings"""
        if not ratings:
            return []
        
        sorted_teams = sorted(ratings.items(), key=lambda x: x[1], reverse=True)
        
        if n:
            sorted_teams = sorted_teams[:n]
        
        rankings = []
        for rank, (team, rating) in enumerate(sorted_teams, 1):
            rankings.append({
                'rank': rank,
                'team': team,
                'rating': rating
            })
        
        return rankings
    
    def _get_conference_rankings(self, conf_ratings: Dict) -> list:
        """Get conference strength rankings"""
        if not conf_ratings:
            return []
        
        sorted_confs = sorted(conf_ratings.items(), key=lambda x: x[1], reverse=True)
        
        rankings = []
        for rank, (conf, rating) in enumerate(sorted_confs, 1):
            rankings.append({
                'rank': rank,
                'conference': conf,
                'strength': rating
            })
        
        return rankings

def run_retro(season: int, max_outer: int = None, config_path: str = 'config.yaml') -> Dict:
    """
    Convenience function to run retro pipeline
    
    Args:
        season: Season year
        max_outer: Maximum outer iterations (optional)
        config_path: Path to configuration file
        
    Returns:
        Pipeline execution results
    """
    pipeline = RetroPipeline(config_path)
    return pipeline.run_retro(season, max_outer)

if __name__ == "__main__":
    # Command-line execution
    import sys
    
    if len(sys.argv) >= 2:
        season = int(sys.argv[1])
    else:
        season = datetime.now().year - 1  # Previous season by default
    
    result = run_retro(season)
    
    if result['success']:
        print(f"✅ Retro pipeline completed successfully for {season}")
        print(f"Converged: {result['convergence']['converged']}")
        print(f"Iterations: {result['convergence']['iterations']}")
        print(f"Final neutrality metric: {result['metrics']['neutrality_metric']:.4f}")
        print(f"Top 10 final rankings:")
        for team_data in result['final_rankings'][:10]:
            print(f"  {team_data['rank']}. {team_data['team']} ({team_data['rating']:.4f})")
    else:
        print(f"❌ Retro pipeline failed: {result['error']}")
        sys.exit(1)
