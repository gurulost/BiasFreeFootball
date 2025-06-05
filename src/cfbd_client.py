"""
CFBD Python Client Integration
Modern wrapper for College Football Data API using official client library
"""

import os
import logging
from typing import Dict, List, Optional
import cfbd
from cfbd.rest import ApiException

class CFBDClient:
    """Official CFBD Python client wrapper for robust API access"""
    
    def __init__(self, api_key: str = None):
        self.logger = logging.getLogger(__name__)
        
        # Configure API client
        configuration = cfbd.Configuration()
        configuration.api_key['Authorization'] = api_key or os.getenv('CFB_API_KEY')
        configuration.api_key_prefix['Authorization'] = 'Bearer'
        
        # Create API client and service APIs
        self.api_client = cfbd.ApiClient(configuration)
        self.teams_api = cfbd.TeamsApi(self.api_client)
        self.games_api = cfbd.GamesApi(self.api_client)
        self.conferences_api = cfbd.ConferencesApi(self.api_client)
        
        # Cache for teams and conferences
        self._teams_cache = {}
        self._conferences_cache = {}
        
    def get_fbs_teams(self, year: int) -> Dict[str, cfbd.models.Team]:
        """Get FBS teams for a specific year with caching"""
        if year in self._teams_cache:
            return self._teams_cache[year]
        
        try:
            self.logger.info(f"Fetching FBS teams for {year} using official CFBD client")
            teams_list = self.teams_api.get_fbs_teams(year=year)
            
            # Build team lookup by school name
            teams_dict = {team.school: team for team in teams_list}
            
            self._teams_cache[year] = teams_dict
            self.logger.info(f"Cached {len(teams_dict)} FBS teams for {year}")
            
            return teams_dict
            
        except ApiException as e:
            self.logger.error(f"CFBD API error fetching teams: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Error fetching teams: {e}")
            raise
    
    def get_conferences(self) -> Dict[int, str]:
        """Get conference ID to name mapping"""
        if self._conferences_cache:
            return self._conferences_cache
        
        try:
            self.logger.info("Fetching conferences using official CFBD client")
            conferences_list = self.conferences_api.get_conferences()
            
            # Build conference ID to name mapping
            conf_dict = {conf.id: conf.name for conf in conferences_list}
            
            self._conferences_cache = conf_dict
            self.logger.info(f"Cached {len(conf_dict)} conferences")
            
            return conf_dict
            
        except ApiException as e:
            self.logger.error(f"CFBD API error fetching conferences: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Error fetching conferences: {e}")
            raise
    
    def get_games(self, year: int, season_type: str = 'both', week: Optional[int] = None) -> List[cfbd.models.Game]:
        """Get games for a specific year and criteria"""
        try:
            params = {
                'year': year,
                'season_type': season_type
            }
            
            if week:
                params['week'] = week
            
            self.logger.info(f"Fetching games for {year} (season_type={season_type}, week={week})")
            games = self.games_api.get_games(**params)
            
            # Filter for completed games only
            completed_games = [game for game in games if game.completed]
            
            self.logger.info(f"Retrieved {len(completed_games)} completed games")
            return completed_games
            
        except ApiException as e:
            self.logger.error(f"CFBD API error fetching games: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Error fetching games: {e}")
            raise
    
    def get_team_conference(self, team_name: str, year: int) -> str:
        """Get conference for a specific team and year"""
        teams = self.get_fbs_teams(year)
        
        if team_name not in teams:
            self.logger.warning(f"Team {team_name} not found in {year} FBS teams")
            return "Unknown"
        
        team = teams[team_name]
        
        # Handle both conference_id (numeric) and conference (string) fields
        if hasattr(team, 'conference') and team.conference:
            return team.conference
        elif hasattr(team, 'conference_id') and team.conference_id:
            conferences = self.get_conferences()
            return conferences.get(team.conference_id, "Unknown")
        else:
            return "Independent"
    
    def canonicalize_team_name(self, team_name: str, year: int) -> Optional[str]:
        """Get canonical team name using official API data"""
        teams = self.get_fbs_teams(year)
        
        # Direct lookup
        if team_name in teams:
            return teams[team_name].school
        
        # Case-insensitive lookup
        team_name_lower = team_name.lower()
        for school, team in teams.items():
            if school.lower() == team_name_lower:
                return team.school
        
        # Check alternate names
        for school, team in teams.items():
            if hasattr(team, 'alternate_names') and team.alternate_names:
                for alt_name in team.alternate_names:
                    if alt_name.lower() == team_name_lower:
                        return team.school
        
        return None
    
    def validate_season_data(self, year: int) -> Dict[str, any]:
        """Validate that season data is complete and accurate"""
        try:
            teams = self.get_fbs_teams(year)
            conferences = self.get_conferences()
            
            # Basic validation metrics
            validation_results = {
                'teams_count': len(teams),
                'conferences_count': len(conferences),
                'expected_fbs_teams': 134,  # Standard FBS count
                'teams_with_conferences': 0,
                'conference_distribution': {},
                'validation_passed': True,
                'warnings': []
            }
            
            # Analyze conference distribution
            for team_name, team in teams.items():
                conf_name = self.get_team_conference(team_name, year)
                
                if conf_name and conf_name != "Unknown":
                    validation_results['teams_with_conferences'] += 1
                    
                if conf_name in validation_results['conference_distribution']:
                    validation_results['conference_distribution'][conf_name] += 1
                else:
                    validation_results['conference_distribution'][conf_name] = 1
            
            # Validation checks
            if validation_results['teams_count'] < 130:
                validation_results['warnings'].append(f"Low team count: {validation_results['teams_count']}")
                validation_results['validation_passed'] = False
            
            if validation_results['teams_with_conferences'] < 120:
                validation_results['warnings'].append("Many teams missing conference assignments")
                validation_results['validation_passed'] = False
            
            self.logger.info(f"Season {year} validation: {validation_results['validation_passed']}")
            return validation_results
            
        except Exception as e:
            self.logger.error(f"Error validating season data: {e}")
            return {
                'validation_passed': False,
                'error': str(e)
            }
    
    def close(self):
        """Close API client"""
        if self.api_client:
            self.api_client.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def create_cfbd_client(api_key: str = None) -> CFBDClient:
    """Factory function to create CFBD client"""
    return CFBDClient(api_key)