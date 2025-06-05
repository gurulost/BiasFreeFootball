"""
Bias audit module for measuring conference neutrality
Implements the neutrality metric B and auto-tuning capabilities
"""

import numpy as np
import pandas as pd
from typing import Dict, List, Tuple, Optional
import logging
from datetime import datetime
import json

class BiasAudit:
    def __init__(self, config: Dict = None):
        if config is None:
            import yaml
            with open('config.yaml', 'r') as f:
                config = yaml.safe_load(f)
                
        self.config = config
        self.threshold = config['bias_audit']['threshold']
        self.auto_tune_threshold = config['bias_audit']['auto_tune_threshold']
        self.logger = logging.getLogger(__name__)
        
        # Storage for bias metrics history
        self.metrics_history = []
    
    def compute_neutrality_metric(self, team_ratings: Dict, 
                                 conference_ratings: Dict = None) -> float:
        """
        Compute neutrality metric B = max_c |mean(R_c) - global_mean|
        
        Args:
            team_ratings: Dictionary of team -> rating
            conference_ratings: Optional conference strength ratings
            
        Returns:
            Bias metric B (0 = perfectly neutral, higher = more biased)
        """
        if not team_ratings:
            return 0.0
        
        # Get team-to-conference mapping (simplified version)
        team_to_conf = self._get_team_conference_mapping()
        
        # Calculate global mean rating
        global_mean = np.mean(list(team_ratings.values()))
        
        # Calculate conference mean ratings
        conf_means = {}
        for team, rating in team_ratings.items():
            conf = team_to_conf.get(team)
            if conf:
                if conf not in conf_means:
                    conf_means[conf] = []
                conf_means[conf].append(rating)
        
        # Convert to means and find maximum deviation
        max_deviation = 0.0
        conf_deviations = {}
        
        for conf, ratings in conf_means.items():
            if ratings:  # Avoid empty conferences
                conf_mean = np.mean(ratings)
                deviation = abs(conf_mean - global_mean)
                conf_deviations[conf] = {
                    'mean_rating': conf_mean,
                    'deviation': deviation,
                    'team_count': len(ratings)
                }
                max_deviation = max(max_deviation, deviation)
        
        self.logger.debug(f"Neutrality metric B = {max_deviation:.4f}")
        self.logger.debug(f"Conference deviations: {conf_deviations}")
        
        return max_deviation
    
    def compute_detailed_metrics(self, team_ratings: Dict, 
                               conference_ratings: Dict = None,
                               week: int = None) -> Dict:
        """
        Compute detailed bias audit metrics including per-conference analysis
        
        Returns:
            Dictionary with comprehensive bias metrics
        """
        if not team_ratings:
            return {'neutrality_metric': 0.0, 'conferences': {}}
        
        team_to_conf = self._get_team_conference_mapping()
        global_mean = np.mean(list(team_ratings.values()))
        
        # Analyze each conference
        conference_analysis = {}
        conf_ratings_by_conf = {}
        
        for team, rating in team_ratings.items():
            conf = team_to_conf.get(team, 'Independent')
            if conf not in conf_ratings_by_conf:
                conf_ratings_by_conf[conf] = []
            conf_ratings_by_conf[conf].append(rating)
        
        max_deviation = 0.0
        
        for conf, ratings in conf_ratings_by_conf.items():
            if ratings:
                conf_mean = np.mean(ratings)
                conf_std = np.std(ratings) if len(ratings) > 1 else 0.0
                deviation = abs(conf_mean - global_mean)
                
                conference_analysis[conf] = {
                    'mean_rating': conf_mean,
                    'std_rating': conf_std,
                    'team_count': len(ratings),
                    'deviation_from_global': deviation,
                    'min_rating': min(ratings),
                    'max_rating': max(ratings),
                    'teams': [team for team in team_ratings.keys() 
                             if team_to_conf.get(team, 'Independent') == conf]
                }
                
                max_deviation = max(max_deviation, deviation)
        
        # Overall metrics
        metrics = {
            'neutrality_metric': max_deviation,
            'global_mean': global_mean,
            'week': week,
            'timestamp': datetime.now().isoformat(),
            'conferences': conference_analysis,
            'bias_threshold': self.threshold,
            'passes_audit': max_deviation <= self.threshold,
            'total_teams': len(team_ratings),
            'total_conferences': len(conference_analysis)
        }
        
        # Store in history
        self.metrics_history.append(metrics)
        
        return metrics
    
    def auto_tune_lambda(self, current_lambda: float = None) -> float:
        """
        Auto-tune the recency decay parameter lambda to reduce bias
        
        Args:
            current_lambda: Current lambda value
            
        Returns:
            Suggested new lambda value
        """
        if current_lambda is None:
            current_lambda = self.config['recency']['lambda']
        
        # Simple adjustment strategy
        # If bias is too high, reduce lambda (give more weight to recent games)
        # This is a simplified heuristic - production version would be more sophisticated
        
        adjustment_factor = 0.9  # Reduce lambda by 10%
        new_lambda = current_lambda * adjustment_factor
        
        # Ensure lambda stays within reasonable bounds
        new_lambda = max(0.01, min(0.1, new_lambda))
        
        self.logger.info(f"Auto-tuning lambda: {current_lambda:.4f} -> {new_lambda:.4f}")
        
        return new_lambda
    
    def get_metrics_history(self) -> List[Dict]:
        """Get historical bias metrics"""
        return self.metrics_history
    
    def get_latest_metrics(self) -> Dict:
        """Get most recent bias metrics"""
        if self.metrics_history:
            return self.metrics_history[-1]
        return {'neutrality_metric': 0.0, 'conferences': {}}
    
    def get_conference_trajectories(self) -> Dict:
        """
        Get conference rating trajectories over time for visualization
        
        Returns:
            Dictionary with conference trajectories
        """
        if not self.metrics_history:
            return {}
        
        trajectories = {}
        
        for metrics in self.metrics_history:
            week = metrics.get('week', 0)
            for conf, data in metrics.get('conferences', {}).items():
                if conf not in trajectories:
                    trajectories[conf] = {
                        'weeks': [],
                        'mean_ratings': [],
                        'deviations': [],
                        'team_counts': []
                    }
                
                trajectories[conf]['weeks'].append(week)
                trajectories[conf]['mean_ratings'].append(data['mean_rating'])
                trajectories[conf]['deviations'].append(data['deviation_from_global'])
                trajectories[conf]['team_counts'].append(data['team_count'])
        
        return trajectories
    
    def _get_team_conference_mapping(self) -> Dict[str, str]:
        """
        Get team-to-conference mapping
        In production, this would come from the teams API data
        For now, return a simplified mapping for major conferences
        """
        # This is a simplified mapping - in production, load from API data
        mapping = {}
        
        # Major conferences (this would come from API in real implementation)
        conferences = {
            'SEC': ['Alabama', 'Georgia', 'LSU', 'Florida', 'Auburn', 'Tennessee', 
                   'Texas A&M', 'South Carolina', 'Kentucky', 'Vanderbilt',
                   'Mississippi State', 'Ole Miss', 'Arkansas', 'Missouri'],
            'Big Ten': ['Ohio State', 'Michigan', 'Penn State', 'Wisconsin', 
                       'Iowa', 'Minnesota', 'Illinois', 'Northwestern',
                       'Michigan State', 'Indiana', 'Purdue', 'Nebraska',
                       'Maryland', 'Rutgers'],
            'Big 12': ['Oklahoma', 'Texas', 'Oklahoma State', 'Baylor',
                      'TCU', 'West Virginia', 'Kansas State', 'Iowa State',
                      'Texas Tech', 'Kansas'],
            'ACC': ['Clemson', 'North Carolina', 'NC State', 'Virginia Tech',
                   'Virginia', 'Miami', 'Florida State', 'Georgia Tech',
                   'Duke', 'Wake Forest', 'Pittsburgh', 'Syracuse',
                   'Boston College', 'Louisville'],
            'Pac-12': ['USC', 'UCLA', 'Oregon', 'Washington', 'Stanford',
                      'California', 'Oregon State', 'Washington State',
                      'Utah', 'Colorado', 'Arizona', 'Arizona State']
        }
        
        for conf, teams in conferences.items():
            for team in teams:
                mapping[team] = conf
        
        return mapping
    
    def save_metrics(self, filepath: str = None) -> None:
        """Save bias metrics history to file"""
        if filepath is None:
            filepath = f"data/processed/bias_metrics_{datetime.now().strftime('%Y%m%d')}.json"
        
        with open(filepath, 'w') as f:
            json.dump(self.metrics_history, f, indent=2)
    
    def load_metrics(self, filepath: str) -> None:
        """Load bias metrics history from file"""
        try:
            with open(filepath, 'r') as f:
                self.metrics_history = json.load(f)
        except FileNotFoundError:
            self.logger.warning(f"Bias metrics file not found: {filepath}")

def compute(team_ratings: Dict, conference_ratings: Dict = None, 
           config: Dict = None) -> float:
    """Convenience function for bias computation"""
    if config is None:
        import yaml
        with open('config.yaml', 'r') as f:
            config = yaml.safe_load(f)
    
    audit = BiasAudit(config)
    return audit.compute_neutrality_metric(team_ratings, conference_ratings)

def auto_tune_lambda(config: Dict = None) -> float:
    """Convenience function for lambda auto-tuning"""
    if config is None:
        import yaml
        with open('config.yaml', 'r') as f:
            config = yaml.safe_load(f)
    
    audit = BiasAudit(config)
    return audit.auto_tune_lambda()
