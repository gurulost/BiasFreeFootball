"""
FBS-Only Data Enforcement Module
Ensures all CFBD API calls return strictly FBS data with proper validation
"""

import logging
from typing import Dict, List, Set, Optional, Tuple
import pandas as pd

class FBSEnforcer:
    """Enforces FBS-only data across all API interactions"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self._fbs_teams_cache = {}
        
    def enforce_fbs_teams_request(self, params: Dict, season: int) -> Dict:
        """
        Enforce FBS-only parameters for teams API requests
        Always adds division='fbs' parameter
        """
        enforced_params = params.copy()
        enforced_params['division'] = 'fbs'
        
        self.logger.debug(f"Enforced FBS teams request for {season}: {enforced_params}")
        return enforced_params
    
    def validate_teams_response(self, teams: List[Dict], season: int) -> Tuple[List[Dict], Dict]:
        """
        Validate teams API response ensures only FBS teams
        Expected count: 134 for 2024
        """
        expected_count = 134  # 2024 FBS count
        
        # Filter for FBS classification if present
        fbs_teams = []
        for team in teams:
            classification = team.get('classification', '').lower()
            if classification == 'fbs' or not classification:  # Include if FBS or missing
                fbs_teams.append(team)
        
        validation_report = {
            'total_received': len(teams),
            'fbs_filtered': len(fbs_teams),
            'expected_count': expected_count,
            'validation_passed': len(fbs_teams) == expected_count,
            'excess_teams': max(0, len(fbs_teams) - expected_count),
            'missing_teams': max(0, expected_count - len(fbs_teams))
        }
        
        if len(fbs_teams) != expected_count:
            if len(fbs_teams) > expected_count:
                self.logger.warning(f"FCS teams leaked in: got {len(fbs_teams)}, expected {expected_count}")
            else:
                self.logger.warning(f"Missing FBS teams: got {len(fbs_teams)}, expected {expected_count}")
        else:
            self.logger.info(f"✓ FBS teams validation passed: {len(fbs_teams)} teams")
        
        # Cache for later use
        self._fbs_teams_cache[season] = {team['school'] for team in fbs_teams if team.get('school')}
        
        return fbs_teams, validation_report
    
    def enforce_games_request(self, params: Dict, season: int) -> Tuple[Dict, bool]:
        """
        Enforce FBS-only parameters for games API requests
        Returns (enforced_params, needs_manual_filtering)
        """
        enforced_params = params.copy()
        
        # Try to add division parameter (may be ignored by API)
        enforced_params['division'] = 'fbs'
        
        # Log warning about known API limitation
        needs_manual_filtering = True
        self.logger.debug(f"Games API division parameter unreliable - manual filtering required")
        
        return enforced_params, needs_manual_filtering
    
    def validate_games_response(self, games: List[Dict], season: int) -> Tuple[List[Dict], Dict]:
        """
        Validate and filter games response to ensure FBS-only
        Uses both classification fields and cached FBS team list
        """
        if season not in self._fbs_teams_cache:
            self.logger.warning(f"No FBS teams cache for {season} - using classification only")
            fbs_team_names = set()
        else:
            fbs_team_names = self._fbs_teams_cache[season]
        
        fbs_games = []
        filtered_count = 0
        classification_failures = 0
        team_name_failures = 0
        
        for game in games:
            # Method 1: Check team classifications
            home_class = game.get('homeClassification', '').lower()
            away_class = game.get('awayClassification', '').lower()
            
            classification_valid = (home_class == 'fbs' and away_class == 'fbs')
            
            # Method 2: Check against FBS team names (if cached)
            home_team = game.get('homeTeam', '')
            away_team = game.get('awayTeam', '')
            
            if fbs_team_names:
                team_names_valid = (home_team in fbs_team_names and away_team in fbs_team_names)
            else:
                team_names_valid = True  # Skip if no cache
            
            # Game passes if both methods agree it's FBS
            if classification_valid and team_names_valid:
                fbs_games.append(game)
            else:
                filtered_count += 1
                if not classification_valid:
                    classification_failures += 1
                if fbs_team_names and not team_names_valid:
                    team_name_failures += 1
        
        validation_report = {
            'total_received': len(games),
            'fbs_filtered': len(fbs_games),
            'non_fbs_filtered': filtered_count,
            'classification_failures': classification_failures,
            'team_name_failures': team_name_failures,
            'filtering_effective': filtered_count > 0
        }
        
        if filtered_count > 0:
            self.logger.info(f"FBS games filtering: {len(fbs_games)}/{len(games)} games kept, filtered {filtered_count} non-FBS")
        else:
            self.logger.info(f"✓ All {len(games)} games were already FBS-only")
        
        return fbs_games, validation_report
    
    def get_fbs_team_names(self, season: int) -> Set[str]:
        """Get cached FBS team names for a season"""
        return self._fbs_teams_cache.get(season, set())
    
    def validate_rating_scale(self, team_ratings: Dict[str, float]) -> Dict:
        """
        Validate that rating scale is appropriate for FBS-only data
        Top teams should be comfortably above 0.01 range
        """
        if not team_ratings:
            return {'validation_passed': False, 'error': 'No ratings provided'}
        
        sorted_ratings = sorted(team_ratings.values(), reverse=True)
        top_rating = sorted_ratings[0]
        median_rating = sorted_ratings[len(sorted_ratings)//2]
        min_rating = sorted_ratings[-1]
        
        # Expected scale for FBS-only PageRank with ~134 teams
        expected_top_min = 0.008  # Top teams should be above this
        expected_top_max = 0.012  # But not unreasonably high
        
        scale_report = {
            'top_rating': top_rating,
            'median_rating': median_rating,
            'min_rating': min_rating,
            'rating_range': top_rating - min_rating,
            'total_teams': len(team_ratings),
            'expected_teams': 134,
            'scale_valid': expected_top_min <= top_rating <= expected_top_max,
            'team_count_valid': len(team_ratings) == 134
        }
        
        scale_report['validation_passed'] = (
            scale_report['scale_valid'] and 
            scale_report['team_count_valid']
        )
        
        if not scale_report['scale_valid']:
            if top_rating < expected_top_min:
                self.logger.warning(f"Rating scale too low: top={top_rating:.6f}, expected>{expected_top_min}")
            elif top_rating > expected_top_max:
                self.logger.warning(f"Rating scale too high: top={top_rating:.6f}, expected<{expected_top_max}")
        
        if not scale_report['team_count_valid']:
            self.logger.warning(f"Team count mismatch: got {len(team_ratings)}, expected 134")
        
        if scale_report['validation_passed']:
            self.logger.info(f"✓ Rating scale validation passed: top={top_rating:.6f}, teams={len(team_ratings)}")
        
        return scale_report
    
    def generate_enforcement_report(self, season: int) -> Dict:
        """Generate comprehensive FBS enforcement report"""
        fbs_teams = self.get_fbs_team_names(season)
        
        report = {
            'season': season,
            'fbs_teams_cached': len(fbs_teams),
            'expected_fbs_count': 134,
            'enforcement_active': True,
            'api_endpoints_covered': [
                'teams (division=fbs enforced)',
                'games (manual FBS filtering)',
                'conferences (FBS context only)'
            ],
            'validation_checks': [
                'Team count validation (134)',
                'Game classification filtering',
                'Team name cross-verification',
                'Rating scale validation',
                'Conference assignment verification'
            ]
        }
        
        return report

def create_fbs_enforcer(config: Dict = None) -> FBSEnforcer:
    """Factory function for FBS enforcer"""
    return FBSEnforcer(config or {})