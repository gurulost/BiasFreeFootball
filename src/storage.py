"""
Data storage and persistence module
Handles saving/loading ratings, metrics, and pipeline state
"""

import os
import json
import pickle
import pandas as pd
from datetime import datetime
from typing import Dict, Tuple, Optional, Any
import logging

class Storage:
    def __init__(self, config: Dict = None):
        if config is None:
            import yaml
            with open('config.yaml', 'r') as f:
                config = yaml.safe_load(f)
        
        self.config = config
        self.processed_dir = config['paths']['data_processed']
        self.logger = logging.getLogger(__name__)
        
        # Ensure directories exist
        os.makedirs(self.processed_dir, exist_ok=True)
    
    def save_ratings(self, S: Dict, R: Dict, week: int, season: int) -> str:
        """
        Save conference and team ratings for a specific week
        
        Args:
            S: Conference ratings dictionary
            R: Team ratings dictionary  
            week: Week number
            season: Season year
            
        Returns:
            Path to saved file
        """
        ratings_data = {
            'conference_ratings': S,
            'team_ratings': R,
            'week': week,
            'season': season,
            'timestamp': datetime.now().isoformat(),
            'metadata': {
                'total_teams': len(R),
                'total_conferences': len(S),
                'pipeline_type': 'live'
            }
        }
        
        filename = f"ratings_{season}_week{week:02d}.json"
        filepath = os.path.join(self.processed_dir, filename)
        
        with open(filepath, 'w') as f:
            json.dump(ratings_data, f, indent=2)
        
        self.logger.info(f"Saved ratings to {filepath}")
        return filepath
    
    def save_retro_ratings(self, S: Dict, R: Dict, season: int) -> str:
        """
        Save final retrospective ratings for a season
        
        Args:
            S: Conference ratings dictionary
            R: Team ratings dictionary
            season: Season year
            
        Returns:
            Path to saved file
        """
        ratings_data = {
            'conference_ratings': S,
            'team_ratings': R,
            'season': season,
            'timestamp': datetime.now().isoformat(),
            'metadata': {
                'total_teams': len(R),
                'total_conferences': len(S),
                'pipeline_type': 'retro'
            }
        }
        
        filename = f"ratings_{season}_retro.json"
        filepath = os.path.join(self.processed_dir, filename)
        
        with open(filepath, 'w') as f:
            json.dump(ratings_data, f, indent=2)
        
        self.logger.info(f"Saved retro ratings to {filepath}")
        return filepath
    
    def load_ratings(self, week: int, season: int) -> Tuple[Dict, Dict]:
        """
        Load ratings for a specific week
        
        Args:
            week: Week number (or "post_cfp" for final)
            season: Season year
            
        Returns:
            Tuple of (conference_ratings, team_ratings)
        """
        if week == "post_cfp":
            filename = f"ratings_{season}_retro.json"
        else:
            filename = f"ratings_{season}_week{week:02d}.json"
        
        filepath = os.path.join(self.processed_dir, filename)
        
        try:
            with open(filepath, 'r') as f:
                data = json.load(f)
            
            S = data.get('conference_ratings', {})
            R = data.get('team_ratings', {})
            
            self.logger.debug(f"Loaded ratings from {filepath}")
            return S, R
            
        except FileNotFoundError:
            self.logger.warning(f"Ratings file not found: {filepath}")
            return {}, {}
        except Exception as e:
            self.logger.error(f"Error loading ratings: {e}")
            return {}, {}
    
    def load_prev_ratings(self, current_week: int, season: int) -> Tuple[Dict, Dict]:
        """
        Load previous week's ratings for initialization
        
        Args:
            current_week: Current week number
            season: Season year
            
        Returns:
            Tuple of (conference_ratings, team_ratings)
        """
        if current_week <= 1:
            # No previous week - return empty/uniform ratings
            return {}, {}
        
        # Try to load from previous week
        prev_week = current_week - 1
        S_prev, R_prev = self.load_ratings(prev_week, season)
        
        if not R_prev:
            # No previous ratings found - initialize with uniform
            self.logger.info("No previous ratings found, using uniform initialization")
            return {}, {}
        
        return S_prev, R_prev
    
    def get_latest_ratings(self, season: int = None) -> Dict:
        """
        Get the most recent ratings available
        
        Args:
            season: Season year (defaults to current year)
            
        Returns:
            Dictionary with latest ratings data
        """
        if season is None:
            season = datetime.now().year
        
        # Look for latest week's data
        latest_data = None
        latest_week = 0
        
        # Check weeks 1-15 for latest data
        for week in range(15, 0, -1):
            try:
                S, R = self.load_ratings(week, season)
                if R:  # Found data
                    latest_data = {
                        'conference_ratings': S,
                        'team_ratings': R,
                        'week': week,
                        'season': season,
                        'type': 'live'
                    }
                    latest_week = week
                    break
            except:
                continue
        
        # Also check for retro data
        try:
            S_retro, R_retro = self.load_ratings("post_cfp", season)
            if R_retro:
                retro_data = {
                    'conference_ratings': S_retro,
                    'team_ratings': R_retro,
                    'week': 'final',
                    'season': season,
                    'type': 'retro'
                }
                
                # Return retro if available and more recent than live
                if not latest_data:
                    latest_data = retro_data
        except:
            pass
        
        return latest_data or {}
    
    def save_bias_metrics(self, metrics: Dict, week: int, season: int) -> str:
        """
        Save bias audit metrics
        
        Args:
            metrics: Bias metrics dictionary
            week: Week number
            season: Season year
            
        Returns:
            Path to saved file
        """
        filename = f"bias_metrics_{season}_week{week:02d}.json"
        filepath = os.path.join(self.processed_dir, filename)
        
        with open(filepath, 'w') as f:
            json.dump(metrics, f, indent=2)
        
        self.logger.debug(f"Saved bias metrics to {filepath}")
        return filepath
    
    def load_bias_metrics(self, week: int, season: int) -> Dict:
        """Load bias metrics for a specific week"""
        filename = f"bias_metrics_{season}_week{week:02d}.json"
        filepath = os.path.join(self.processed_dir, filename)
        
        try:
            with open(filepath, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            self.logger.warning(f"Bias metrics file not found: {filepath}")
            return {}
        except Exception as e:
            self.logger.error(f"Error loading bias metrics: {e}")
            return {}
    
    def save_graph_snapshot(self, G_conf, G_team, week: int, season: int) -> str:
        """
        Save graph snapshots for debugging/analysis
        
        Args:
            G_conf: Conference graph
            G_team: Team graph
            week: Week number
            season: Season year
            
        Returns:
            Path to saved file
        """
        graph_data = {
            'conference_graph': {
                'nodes': list(G_conf.nodes()),
                'edges': [(u, v, d) for u, v, d in G_conf.edges(data=True)]
            },
            'team_graph': {
                'nodes': list(G_team.nodes()),
                'edges': [(u, v, d) for u, v, d in G_team.edges(data=True)]
            },
            'week': week,
            'season': season,
            'timestamp': datetime.now().isoformat()
        }
        
        filename = f"graphs_{season}_week{week:02d}.pkl"
        filepath = os.path.join(self.processed_dir, filename)
        
        with open(filepath, 'wb') as f:
            pickle.dump(graph_data, f)
        
        self.logger.debug(f"Saved graph snapshot to {filepath}")
        return filepath
    
    def list_available_ratings(self, season: int) -> list:
        """
        List all available rating files for a season
        
        Args:
            season: Season year
            
        Returns:
            List of available week/type combinations
        """
        available = []
        
        # Check live ratings (weeks 1-15)
        for week in range(1, 16):
            filename = f"ratings_{season}_week{week:02d}.json"
            filepath = os.path.join(self.processed_dir, filename)
            if os.path.exists(filepath):
                available.append({
                    'type': 'live',
                    'week': week,
                    'season': season,
                    'filepath': filepath
                })
        
        # Check retro ratings
        filename = f"ratings_{season}_retro.json"
        filepath = os.path.join(self.processed_dir, filename)
        if os.path.exists(filepath):
            available.append({
                'type': 'retro',
                'week': 'final',
                'season': season,
                'filepath': filepath
            })
        
        return available
    
    def cleanup_old_data(self, keep_seasons: int = 3) -> None:
        """
        Clean up old data files, keeping only recent seasons
        
        Args:
            keep_seasons: Number of seasons to keep
        """
        current_year = datetime.now().year
        cutoff_year = current_year - keep_seasons
        
        files_removed = 0
        
        for filename in os.listdir(self.processed_dir):
            if filename.startswith(('ratings_', 'bias_metrics_', 'graphs_')):
                try:
                    # Extract year from filename
                    parts = filename.split('_')
                    year = int(parts[1])
                    
                    if year < cutoff_year:
                        filepath = os.path.join(self.processed_dir, filename)
                        os.remove(filepath)
                        files_removed += 1
                        
                except (ValueError, IndexError):
                    # Skip files that don't match expected format
                    continue
        
        if files_removed > 0:
            self.logger.info(f"Cleaned up {files_removed} old data files")

# Convenience functions for backwards compatibility
def save_ratings(S: Dict, R: Dict, week: int, season: int = None) -> str:
    """Convenience function for saving ratings"""
    if season is None:
        season = datetime.now().year
    
    storage = Storage()
    return storage.save_ratings(S, R, week, season)

def load_prev_ratings(week: int = None, season: int = None) -> Tuple[Dict, Dict]:
    """Convenience function for loading previous ratings"""
    if season is None:
        season = datetime.now().year
    if week is None:
        week = 1
    
    storage = Storage()
    return storage.load_prev_ratings(week, season)
