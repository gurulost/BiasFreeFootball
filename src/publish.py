"""
Publication and export module
Handles CSV/JSON exports and dashboard publishing
"""

import os
import csv
import json
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional
import logging

class Publisher:
    def __init__(self, config: Dict = None):
        if config is None:
            import yaml
            with open('config.yaml', 'r') as f:
                config = yaml.safe_load(f)
        
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Create exports directory
        self.exports_dir = config.get('paths', {}).get('exports', 'exports')
        os.makedirs(self.exports_dir, exist_ok=True)
    
    def weekly_csv_json(self, S: Dict, R: Dict, week: int, season: int, 
                       bias_metric: float) -> Dict:
        """
        Export weekly LIVE rankings to CSV and JSON
        
        Args:
            S: Conference ratings
            R: Team ratings
            week: Week number
            season: Season year
            bias_metric: Neutrality metric B
            
        Returns:
            Dictionary with paths to exported files
        """
        # Calculate rankings and deltas
        ranked_data = self._prepare_live_rankings(R, S, week, season)
        
        # Prepare metadata
        metadata = {
            'week': week,
            'season': season,
            'timestamp': datetime.now().isoformat(),
            'neutrality_metric': bias_metric,
            'total_teams': len(R),
            'total_conferences': len(S),
            'pipeline_type': 'live'
        }
        
        # Export CSV
        csv_path = self._export_csv(ranked_data, f"{season}_Wk{week:02d}_live", metadata)
        
        # Export JSON
        json_data = {
            'metadata': metadata,
            'rankings': ranked_data,
            'conference_ratings': S
        }
        json_path = self._export_json(json_data, f"{season}_Wk{week:02d}_live")
        
        self.logger.info(f"Published weekly rankings: CSV={csv_path}, JSON={json_path}")
        
        return {
            'csv_path': csv_path,
            'json_path': json_path,
            'metadata': metadata
        }
    
    def retro_csv_json(self, S: Dict, R: Dict, season: int) -> Dict:
        """
        Export final RETRO rankings to CSV and JSON
        
        Args:
            S: Conference ratings
            R: Team ratings  
            season: Season year
            
        Returns:
            Dictionary with paths to exported files
        """
        # Calculate final rankings
        ranked_data = self._prepare_retro_rankings(R, S, season)
        
        # Prepare metadata
        metadata = {
            'season': season,
            'timestamp': datetime.now().isoformat(),
            'total_teams': len(R),
            'total_conferences': len(S),
            'pipeline_type': 'retro'
        }
        
        # Export CSV
        csv_path = self._export_csv(ranked_data, f"{season}_retro", metadata)
        
        # Export JSON
        json_data = {
            'metadata': metadata,
            'rankings': ranked_data,
            'conference_ratings': S
        }
        json_path = self._export_json(json_data, f"{season}_retro")
        
        self.logger.info(f"Published retro rankings: CSV={csv_path}, JSON={json_path}")
        
        return {
            'csv_path': csv_path,
            'json_path': json_path,
            'metadata': metadata
        }
    
    def _prepare_live_rankings(self, R: Dict, S: Dict, week: int, season: int) -> List[Dict]:
        """Prepare live rankings data for export"""
        if not R:
            return []
        
        # Sort teams by rating (descending)
        sorted_teams = sorted(R.items(), key=lambda x: x[1], reverse=True)
        
        # Load previous week's rankings for delta calculation
        try:
            from src.storage import Storage
            storage = Storage()
            _, R_prev = storage.load_ratings(week - 1, season)
            prev_rankings = self._get_team_rankings(R_prev) if R_prev else {}
        except:
            prev_rankings = {}
        
        rankings_data = []
        
        for current_rank, (team, rating) in enumerate(sorted_teams, 1):
            # Calculate rank delta
            prev_rank = prev_rankings.get(team, current_rank)
            delta_rank = prev_rank - current_rank  # Positive = moved up
            
            # Get conference and conference strength
            team_conf = self._get_team_conference(team)
            conf_weight = S.get(team_conf, 0.5) if team_conf else 0.5
            
            # Get quality wins (simplified - top 3 opponents by rating)
            quality_wins = self._get_quality_wins(team, R)
            
            team_data = {
                'rank_live': current_rank,
                'team': team,
                'conference': team_conf,
                'rating_live': rating,
                'delta_rank': delta_rank,
                'conf_weight': conf_weight,
                'quality_wins': quality_wins
            }
            
            rankings_data.append(team_data)
        
        return rankings_data
    
    def _prepare_retro_rankings(self, R: Dict, S: Dict, season: int) -> List[Dict]:
        """Prepare retro rankings data for export"""
        if not R:
            return []
        
        # Sort teams by rating (descending)
        sorted_teams = sorted(R.items(), key=lambda x: x[1], reverse=True)
        
        rankings_data = []
        
        for rank, (team, rating) in enumerate(sorted_teams, 1):
            # Get conference and conference strength
            team_conf = self._get_team_conference(team)
            conf_weight = S.get(team_conf, 0.5) if team_conf else 0.5
            
            # Get quality wins
            quality_wins = self._get_quality_wins(team, R)
            
            team_data = {
                'rank_retro': rank,
                'team': team,
                'conference': team_conf,
                'rating_retro': rating,
                'conf_weight': conf_weight,
                'quality_wins': quality_wins,
                'uncertainty': 0.0  # Placeholder for bootstrap uncertainty
            }
            
            rankings_data.append(team_data)
        
        return rankings_data
    
    def _export_csv(self, data: List[Dict], filename: str, metadata: Dict = None) -> str:
        """Export data to CSV format"""
        if not data:
            return ""
        
        filepath = os.path.join(self.exports_dir, f"{filename}.csv")
        
        # Get field names from first row
        fieldnames = list(data[0].keys())
        
        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            # Write header comment with metadata
            if metadata:
                csvfile.write(f"# CFB Rankings Export\n")
                csvfile.write(f"# Generated: {metadata.get('timestamp', 'Unknown')}\n")
                csvfile.write(f"# Season: {metadata.get('season', 'Unknown')}\n")
                if 'week' in metadata:
                    csvfile.write(f"# Week: {metadata['week']}\n")
                if 'neutrality_metric' in metadata:
                    csvfile.write(f"# Neutrality Metric: {metadata['neutrality_metric']:.4f}\n")
                csvfile.write(f"#\n")
            
            writer.writeheader()
            writer.writerows(data)
        
        return filepath
    
    def _export_json(self, data: Dict, filename: str) -> str:
        """Export data to JSON format"""
        filepath = os.path.join(self.exports_dir, f"{filename}.json")
        
        with open(filepath, 'w', encoding='utf-8') as jsonfile:
            json.dump(data, jsonfile, indent=2, ensure_ascii=False)
        
        return filepath
    
    def _get_team_rankings(self, ratings: Dict) -> Dict:
        """Convert ratings to rankings (team -> rank mapping)"""
        if not ratings:
            return {}
        
        sorted_teams = sorted(ratings.items(), key=lambda x: x[1], reverse=True)
        return {team: rank for rank, (team, _) in enumerate(sorted_teams, 1)}
    
    def _get_team_conference(self, team: str) -> Optional[str]:
        """
        Get conference for a team
        This is simplified - in production, would come from API data
        """
        # Simplified mapping - in production, load from teams API
        major_conferences = {
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
        
        for conf, teams in major_conferences.items():
            if team in teams:
                return conf
        
        return 'Independent'
    
    def _get_quality_wins(self, team: str, all_ratings: Dict, top_n: int = 3) -> List[str]:
        """
        Get quality wins for a team (simplified version)
        In production, this would analyze actual game results
        """
        # This is a placeholder - in production, would analyze actual games
        # and return top opponents beaten with their ratings
        
        # Sort all teams by rating to identify quality opponents
        sorted_teams = sorted(all_ratings.items(), key=lambda x: x[1], reverse=True)
        top_teams = [t[0] for t in sorted_teams[:25]]  # Top 25 teams
        
        # Simplified - just return some top teams (not actually beaten)
        # In production, would check actual game results
        quality_opponents = [t for t in top_teams if t != team][:top_n]
        
        return quality_opponents
    
    def export_csv(self, data: Dict, filename: str = None) -> str:
        """
        Generic CSV export function
        
        Args:
            data: Dictionary or list of data to export
            filename: Optional filename (auto-generated if not provided)
            
        Returns:
            Path to exported CSV file
        """
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"cfb_export_{timestamp}"
        
        # Handle different data formats
        if isinstance(data, dict):
            if 'rankings' in data:
                # Structured export data
                export_data = data['rankings']
                metadata = data.get('metadata', {})
            else:
                # Convert dict to list of dicts
                export_data = [{'key': k, 'value': v} for k, v in data.items()]
                metadata = {}
        else:
            export_data = data
            metadata = {}
        
        return self._export_csv(export_data, filename, metadata)

# Convenience functions
def weekly_csv_json(S: Dict, R: Dict, week: int, season: int, B: float, 
                   config: Dict = None) -> Dict:
    """Convenience function for weekly publishing"""
    if config is None:
        import yaml
        with open('config.yaml', 'r') as f:
            config = yaml.safe_load(f)
    
    publisher = Publisher(config)
    return publisher.weekly_csv_json(S, R, week, season, B)

def retro_csv_json(S: Dict, R: Dict, season: int, config: Dict = None) -> Dict:
    """Convenience function for retro publishing"""
    if config is None:
        import yaml
        with open('config.yaml', 'r') as f:
            config = yaml.safe_load(f)
    
    publisher = Publisher(config)
    return publisher.retro_csv_json(S, R, season)
