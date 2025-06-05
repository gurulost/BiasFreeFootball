"""
Retro (retrospective) pipeline implementation
Runs once per season after CFP championship for definitive rankings
Uses EM-style iteration until convergence with bootstrap uncertainty analysis
"""

import yaml
import logging
import numpy as np
from datetime import datetime
from typing import Dict, List, Tuple
import pandas as pd

from src.ingest import fetch_results_upto_bowls
from src.graph import GraphBuilder
from src.pagerank import pagerank
from src.storage import Storage
from src.publish import Publisher
from src.bias_audit import BiasAudit

class RetroPipeline:
    def __init__(self, config_path: str = 'config.yaml'):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.logger = logging.getLogger(__name__)
        self.storage = Storage()
        self.publisher = Publisher(self.config)
        self.graph_builder = GraphBuilder(self.config)
        self.bias_audit = BiasAudit(self.config)
    
    def run_retro(self, season: int, max_outer: int = None) -> Dict:
        """
        Run complete retrospective pipeline with EM convergence
        Blueprint: "Run EM hindsight loop until weights & ratings converge"
        
        Args:
            season: Season year (e.g., 2023)
            max_outer: Maximum outer iterations (default from config)
            
        Returns:
            Dictionary with complete pipeline results and uncertainty analysis
        """
        if max_outer is None:
            max_outer = self.config['pipeline']['retro']['max_outer_iterations']
        
        convergence_threshold = float(self.config['pipeline']['retro']['convergence_threshold'])
        
        self.logger.info(f"Starting retro pipeline for {season} season")
        
        try:
            # 1. Ingest complete season data including bowls
            self.logger.info("Step 1: Ingesting complete season data including bowls")
            games_df = fetch_results_upto_bowls(season, self.config)
            self.logger.info(f"Loaded {len(games_df)} completed games including bowls")
            
            if games_df.empty:
                self.logger.warning("No game data available for retro analysis")
                return {'success': False, 'error': 'No game data', 'season': season}
            
            # 2. Initialize from final live ratings for faster convergence
            self.logger.info("Step 2: Initializing ratings for EM convergence")
            try:
                # Load final week live ratings as starting point
                _, R_init = self.storage.load_ratings(15, season)
                self.logger.info("Loaded final live ratings as EM starting point")
            except:
                # Start from uniform distribution
                teams = set(games_df['winner'].tolist() + games_df['loser'].tolist())
                R_init = {team: 0.5 for team in teams}
                self.logger.info("Starting EM from uniform distribution")
            
            R = R_init.copy()
            S = {}
            
            # 3. EM-style convergence loop (exact blueprint implementation)
            self.logger.info("Step 3: Running EM convergence loop")
            converged = False
            final_diff = None
            iteration_history = []
            
            for outer_iter in range(max_outer):
                self.logger.info(f"EM iteration {outer_iter + 1}/{max_outer}")
                
                # Rebuild graphs with current ratings (hindsight mode, no shrinkage)
                G_conf, G_team = self.graph_builder.rebuild_full_graph(games_df, R, hindsight=True)
                
                # Stage-1: Conference PageRank (cross-conference games only)
                S_new = pagerank(G_conf, 
                               damping=self.config['pagerank']['damping'],
                               config=self.config)
                
                if not S_new:
                    # Handle empty conference graph
                    conferences = set()
                    for conf in games_df['winner_conference'].dropna().tolist() + games_df['loser_conference'].dropna().tolist():
                        if conf and pd.notna(conf):
                            conferences.add(conf)
                    S_new = {conf: 0.5 for conf in conferences}
                    self.logger.info("Using uniform conference ratings (no cross-conference games)")
                
                # Stage-2: Team PageRank with √S injection
                self.graph_builder.inject_conf_strength(G_team, S_new, S_new)
                R_new = pagerank(G_team,
                               damping=self.config['pagerank']['damping'],
                               config=self.config)
                
                if not R_new:
                    self.logger.error("Team PageRank failed in retro pipeline")
                    break
                
                # Convergence check (blueprint: max |R_new - R| < 1e-6)
                team_diffs = []
                for team in R_new:
                    old_val = R.get(team, 0.5)
                    new_val = R_new[team]
                    team_diffs.append(abs(float(new_val) - float(old_val)))
                
                max_diff = max(team_diffs) if team_diffs else 0.0
                
                iteration_metrics = {
                    'iteration': outer_iter + 1,
                    'max_difference': float(max_diff),
                    'num_teams': len(R_new),
                    'num_conferences': len(S_new),
                    'convergence_threshold': convergence_threshold
                }
                iteration_history.append(iteration_metrics)
                
                self.logger.info(f"EM iteration {outer_iter + 1}: max_diff = {max_diff:.8f} (threshold = {convergence_threshold})")
                
                if max_diff < convergence_threshold:
                    self.logger.info(f"EM converged after {outer_iter + 1} iterations")
                    converged = True
                    final_diff = max_diff
                    R, S = R_new, S_new
                    break
                
                R, S = R_new, S_new
                final_diff = max_diff
            
            if not converged:
                self.logger.warning(f"EM did not converge after {max_outer} iterations")
            
            # 4. Bootstrap uncertainty analysis (reduced samples for faster completion)
            self.logger.info("Step 4: Computing bootstrap uncertainty analysis")
            uncertainty_metrics = self._bootstrap_uncertainty(games_df, R, bootstrap_samples=25)
            
            # 5. Final bias audit
            self.logger.info("Step 5: Computing final bias metrics")
            final_bias_metrics = self.bias_audit.compute_detailed_metrics(R, S)
            
            # 6. Persist final retro ratings
            self.logger.info("Step 6: Persisting retro ratings")
            self.storage.save_retro_ratings(S, R, season)
            
            # 7. Publish definitive rankings
            self.logger.info("Step 7: Publishing retro results")
            publish_results = self.publisher.retro_csv_json(S, R, season)
            
            # Prepare comprehensive result summary
            result = {
                'success': True,
                'season': season,
                'timestamp': datetime.now().isoformat(),
                'em_convergence': {
                    'iterations': len(iteration_history),
                    'converged': converged,
                    'final_difference': final_diff,
                    'convergence_threshold': convergence_threshold,
                    'iteration_history': iteration_history
                },
                'metrics': {
                    'games_processed': len(games_df),
                    'teams_ranked': len(R),
                    'conferences_ranked': len(S),
                    'neutrality_metric': final_bias_metrics.get('neutrality_metric', None)
                },
                'final_rankings': self._get_final_rankings(R, 25),
                'conference_rankings': self._get_conference_rankings(S),
                'bias_metrics': final_bias_metrics,
                'uncertainty_analysis': uncertainty_metrics,
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
    
    def _bootstrap_uncertainty(self, games_df: pd.DataFrame, final_ratings: Dict, 
                             bootstrap_samples: int = 100) -> Dict:
        """
        Bootstrap sampling to estimate ranking uncertainty
        Provides confidence intervals for final rankings
        
        Args:
            games_df: Complete game data
            final_ratings: Final team ratings
            bootstrap_samples: Number of bootstrap samples
            
        Returns:
            Dictionary with uncertainty metrics and confidence intervals
        """
        self.logger.info(f"Running bootstrap uncertainty analysis with {bootstrap_samples} samples")
        
        bootstrap_rankings = []
        bootstrap_ratings = []
        
        # Get baseline ranking
        baseline_ranking = self._get_rankings_from_ratings(final_ratings)
        
        for sample_idx in range(bootstrap_samples):
            if sample_idx % 25 == 0:
                self.logger.info(f"Bootstrap sample {sample_idx + 1}/{bootstrap_samples}")
            
            try:
                # Sample games with replacement
                sampled_games = games_df.sample(n=len(games_df), replace=True, random_state=sample_idx)
                
                # Run abbreviated pipeline on sampled data
                G_conf, G_team = self.graph_builder.rebuild_full_graph(sampled_games, final_ratings, hindsight=True)
                
                # Conference PageRank
                S_boot = pagerank(G_conf, config=self.config)
                if not S_boot:
                    # Fallback to uniform conference ratings
                    conferences = set()
                    for conf in sampled_games['winner_conference'].dropna().tolist() + sampled_games['loser_conference'].dropna().tolist():
                        if conf and pd.notna(conf):
                            conferences.add(conf)
                    S_boot = {conf: 0.5 for conf in conferences}
                
                # Team PageRank with conference injection
                self.graph_builder.inject_conf_strength(G_team, S_boot, S_boot)
                R_boot = pagerank(G_team, config=self.config)
                
                if R_boot and len(R_boot) > 0:
                    # Store ratings and rankings
                    bootstrap_ratings.append(R_boot)
                    rankings = self._get_rankings_from_ratings(R_boot)
                    bootstrap_rankings.append(rankings)
                    
            except Exception as e:
                self.logger.debug(f"Bootstrap sample {sample_idx} failed: {e}")
                continue
        
        if not bootstrap_rankings:
            self.logger.warning("No successful bootstrap samples")
            return {
                'samples_completed': 0,
                'samples_requested': bootstrap_samples,
                'uncertainty_available': False
            }
        
        self.logger.info(f"Completed {len(bootstrap_rankings)} successful bootstrap samples")
        
        # Calculate comprehensive uncertainty metrics
        uncertainty_metrics = self._calculate_uncertainty_metrics(bootstrap_rankings, baseline_ranking)
        uncertainty_metrics.update({
            'samples_completed': len(bootstrap_rankings),
            'samples_requested': bootstrap_samples,
            'uncertainty_available': True,
            'baseline_ranking': baseline_ranking
        })
        
        return uncertainty_metrics
    
    def _get_rankings_from_ratings(self, ratings: Dict) -> Dict:
        """Convert ratings to rankings (1 = best)"""
        sorted_teams = sorted(ratings.items(), key=lambda x: x[1], reverse=True)
        return {team: rank + 1 for rank, (team, _) in enumerate(sorted_teams)}
    
    def _calculate_uncertainty_metrics(self, bootstrap_rankings: List[Dict], baseline_ranking: Dict) -> Dict:
        """Calculate comprehensive uncertainty metrics from bootstrap samples"""
        if not bootstrap_rankings:
            return {}
        
        # Get all teams that appear in bootstrap samples
        all_teams = set()
        for rankings in bootstrap_rankings:
            all_teams.update(rankings.keys())
        
        team_uncertainty = {}
        
        for team in all_teams:
            # Get rankings for this team across all bootstrap samples
            team_ranks = []
            for rankings in bootstrap_rankings:
                rank = rankings.get(team, len(all_teams) + 1)  # Worst possible rank if missing
                team_ranks.append(rank)
            
            # Calculate confidence intervals and stability metrics
            team_uncertainty[team] = {
                'baseline_rank': baseline_ranking.get(team, len(all_teams) + 1),
                'mean_rank': float(np.mean(team_ranks)),
                'median_rank': float(np.median(team_ranks)),
                'std_rank': float(np.std(team_ranks)),
                'confidence_interval_90': [
                    float(np.percentile(team_ranks, 5)),
                    float(np.percentile(team_ranks, 95))
                ],
                'confidence_interval_95': [
                    float(np.percentile(team_ranks, 2.5)),
                    float(np.percentile(team_ranks, 97.5))
                ],
                'rank_volatility': float(np.max(team_ranks) - np.min(team_ranks)),
                'samples_present': sum(1 for r in bootstrap_rankings if team in r)
            }
        
        # Calculate overall stability metrics
        top_10_stability = self._calculate_top_n_stability(bootstrap_rankings, baseline_ranking, n=10)
        top_25_stability = self._calculate_top_n_stability(bootstrap_rankings, baseline_ranking, n=25)
        
        return {
            'team_uncertainty': team_uncertainty,
            'stability_metrics': {
                'top_10_stability': top_10_stability,
                'top_25_stability': top_25_stability,
                'mean_rank_volatility': float(np.mean([metrics['rank_volatility'] for metrics in team_uncertainty.values()])),
                'median_rank_volatility': float(np.median([metrics['rank_volatility'] for metrics in team_uncertainty.values()]))
            }
        }
    
    def _calculate_top_n_stability(self, bootstrap_rankings: List[Dict], baseline_ranking: Dict, n: int) -> Dict:
        """Calculate stability of top N rankings"""
        baseline_top_n = set([team for team, rank in baseline_ranking.items() if rank <= n])
        
        overlap_counts = []
        for rankings in bootstrap_rankings:
            bootstrap_top_n = set([team for team, rank in rankings.items() if rank <= n])
            overlap = len(baseline_top_n.intersection(bootstrap_top_n))
            overlap_counts.append(overlap)
        
        return {
            'mean_overlap': float(np.mean(overlap_counts)),
            'median_overlap': float(np.median(overlap_counts)),
            'min_overlap': int(np.min(overlap_counts)),
            'max_overlap': int(np.max(overlap_counts)),
            'stability_percentage': float(np.mean(overlap_counts)) / n * 100
        }
    
    def _get_final_rankings(self, ratings: Dict, n: int = None) -> List[Dict]:
        """Get final team rankings"""
        sorted_teams = sorted(ratings.items(), key=lambda x: x[1], reverse=True)
        
        if n:
            sorted_teams = sorted_teams[:n]
        
        rankings = []
        for rank, (team, rating) in enumerate(sorted_teams, 1):
            rankings.append({
                'rank': rank,
                'team': team,
                'rating': float(rating)
            })
        
        return rankings
    
    def _get_conference_rankings(self, conf_ratings: Dict) -> List[Dict]:
        """Get conference strength rankings"""
        sorted_confs = sorted(conf_ratings.items(), key=lambda x: x[1], reverse=True)
        
        rankings = []
        for rank, (conf, rating) in enumerate(sorted_confs, 1):
            rankings.append({
                'rank': rank,
                'conference': conf,
                'strength': float(rating)
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
        Pipeline execution results with EM convergence and uncertainty analysis
    """
    pipeline = RetroPipeline(config_path)
    return pipeline.run_retro(season, max_outer)