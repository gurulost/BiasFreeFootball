"""
Modern CFBD client using official cfbd-python library with Game object model
Replaces brittle dictionary lookups with clean attribute access
"""

import os
import logging
import json
import pandas as pd
from typing import Dict, List, Optional
from datetime import datetime
import cfbd
from cfbd.configuration import Configuration
from cfbd.api_client import ApiClient
from cfbd.api.teams_api import TeamsApi
from cfbd.api.games_api import GamesApi
from cfbd.api.conferences_api import ConferencesApi
# RecordsApi import commented out - not available in current cfbd library version
# from cfbd.api.records_api import RecordsApi
from cfbd.exceptions import ApiException

class ModernCFBDClient:
    """Modern CFBD client using official library with Game object model"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Configure the official CFBD API client
        configuration = Configuration()
        api_key = os.getenv('CFB_API_KEY', config.get('api', {}).get('key', ''))
        
        if not api_key:
            self.logger.error("CFB_API_KEY required for authentic data access")
            raise ValueError("CFB_API_KEY environment variable must be set")
        
        # Set the access token directly as per cfbd library documentation
        configuration.access_token = api_key
        
        # Create API client and specific API instances
        api_client = ApiClient(configuration)
        self.teams_api = TeamsApi(api_client)
        self.games_api = GamesApi(api_client)
        self.conferences_api = ConferencesApi(api_client)
        # RecordsApi not available in current cfbd library version
        # self.records_api = RecordsApi(api_client)
        
        self.logger.info("Modern CFBD client initialized with official library")
    
    def fetch_fbs_teams(self, season: int) -> List[Dict]:
        """Fetch FBS teams using official Team model with authoritative data"""
        try:
            self.logger.info(f"Fetching FBS teams for {season} using authoritative Team model")
            
            # Use official API to get all teams, then filter for FBS classification
            teams = self.teams_api.get_teams(year=season)
            
            # Convert Team objects to dictionary format using clean attribute access
            fbs_teams_data = []
            for team in teams:
                # Filter for FBS teams only using Team model classification
                if hasattr(team, 'classification') and team.classification == 'fbs':
                    team_dict = {
                        'id': team.id,
                        'school': team.school,
                        'mascot': team.mascot if team.mascot else None,
                        'abbreviation': team.abbreviation if team.abbreviation else None,
                        'conference': team.conference if team.conference else 'Unknown',
                        'division': team.division if hasattr(team, 'division') and team.division else None,
                        'classification': team.classification,
                        'color': team.color if team.color else None,
                        'alternate_color': team.alternate_color if hasattr(team, 'alternate_color') and team.alternate_color else None,
                        'alternate_names': team.alternate_names if hasattr(team, 'alternate_names') and team.alternate_names else []
                    }
                    fbs_teams_data.append(team_dict)
            
            self.logger.info(f"Fetched {len(fbs_teams_data)} FBS teams using authoritative Team model")
            
            # Validate FBS count (should be exactly 134 for recent seasons)
            if len(fbs_teams_data) != 134:
                self.logger.warning(f"Expected 134 FBS teams, got {len(fbs_teams_data)}")
            
            # Save raw data for caching
            os.makedirs('data/raw', exist_ok=True)
            with open(f'data/raw/fbs_teams_{season}.json', 'w') as f:
                json.dump(fbs_teams_data, f, indent=2)
            
            return fbs_teams_data
            
        except ApiException as e:
            self.logger.error(f"CFBD API request failed: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Failed to fetch FBS teams: {e}")
            raise
    
    def fetch_games(self, season: int, week: Optional[int] = None, 
                   season_type: str = 'regular') -> List[Dict]:
        """Fetch games using official library with Game object model"""
        try:
            self.logger.info(f"Fetching {season_type} games for season {season}" + 
                           (f", week {week}" if week else ""))
            
            # Use official CFBD library for reliable data access
            if week:
                # Week-specific games
                games = self.games_api.get_games(year=season, week=week, season_type=season_type)
            else:
                # All games for season type
                games = self.games_api.get_games(year=season, season_type=season_type)
            
            # Convert Game objects to dictionary format using clean attribute access
            games_data = []
            for game in games:
                # Use direct attribute access from Game object model
                game_dict = {
                    'id': game.id,
                    'season': game.season,
                    'week': game.week,
                    'seasonType': game.season_type,
                    'completed': game.completed,
                    'neutralSite': game.neutral_site if hasattr(game, 'neutral_site') else False,
                    'conferenceGame': game.conference_game if hasattr(game, 'conference_game') else False,
                    'homeTeam': game.home_team,
                    'homePoints': game.home_points if game.home_points is not None else 0,
                    'homeConference': game.home_conference,
                    'homeClassification': game.home_classification if hasattr(game, 'home_classification') else 'fbs',
                    'awayTeam': game.away_team,
                    'awayPoints': game.away_points if game.away_points is not None else 0,
                    'awayConference': game.away_conference,
                    'awayClassification': game.away_classification if hasattr(game, 'away_classification') else 'fbs',
                    'startDate': str(game.start_date) if hasattr(game, 'start_date') and game.start_date else None,
                    'venue': game.venue if hasattr(game, 'venue') else None,
                    'attendance': game.attendance if hasattr(game, 'attendance') else None
                }
                games_data.append(game_dict)
            
            self.logger.info(f"Fetched {len(games_data)} games using Game object attributes")
            
            # Filter for completed games only
            completed_games = [g for g in games_data if g['completed']]
            self.logger.info(f"Filtered to {len(completed_games)} completed games")
            
            # Save raw data for caching
            os.makedirs('data/raw', exist_ok=True)
            week_str = f"_week{week}" if week else ""
            raw_path = f"data/raw/games_{season}_{season_type}{week_str}.json"
            with open(raw_path, 'w') as f:
                json.dump(completed_games, f, indent=2)
            
            return completed_games
            
        except ApiException as e:
            self.logger.error(f"CFBD API request failed: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Failed to fetch games: {e}")
            raise
    
    def fetch_fbs_games_only(self, season: int, season_type: str = 'regular') -> List[Dict]:
        """Fetch all completed FBS-only games for a season using validation-first approach"""
        # First get all FBS teams for strict filtering
        fbs_teams = self.fetch_fbs_teams(season)
        fbs_team_names = {team['school'] for team in fbs_teams}
        
        self.logger.info(f"Filtering games using {len(fbs_team_names)} authentic FBS teams")
        
        # Fetch all games
        all_games = self.fetch_games(season, season_type=season_type)
        
        # Filter for FBS-only games (both teams must be FBS)
        fbs_games = []
        for game in all_games:
            home_team = game['homeTeam']
            away_team = game['awayTeam']
            
            if home_team in fbs_team_names and away_team in fbs_team_names:
                fbs_games.append(game)
        
        self.logger.info(f"Filtered to {len(fbs_games)} FBS-only games from {len(all_games)} total")
        
        # Validate expected game count for data integrity
        if season == 2024 and len(fbs_games) < 700:
            self.logger.warning(f"Expected ~800+ FBS games for 2024, got {len(fbs_games)}")
        
        return fbs_games
    
    def fetch_conferences(self, season: int) -> List[Dict]:
        """Fetch conferences for season using official library"""
        try:
            self.logger.info(f"Fetching conferences for {season} season")
            
            conferences = self.conferences_api.get_conferences()
            
            # Convert to dictionary format
            conferences_data = []
            for conf in conferences:
                conf_dict = {
                    'id': conf.id,
                    'name': conf.name,
                    'short_name': conf.short_name if hasattr(conf, 'short_name') else None,
                    'abbreviation': conf.abbreviation if hasattr(conf, 'abbreviation') else None
                }
                conferences_data.append(conf_dict)
            
            self.logger.info(f"Fetched {len(conferences_data)} conferences")
            
            # Save raw data
            os.makedirs('data/raw', exist_ok=True)
            with open(f'data/raw/conferences_{season}.json', 'w') as f:
                json.dump(conferences_data, f, indent=2)
            
            return conferences_data
            
        except ApiException as e:
            self.logger.error(f"CFBD API request failed: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Failed to fetch conferences: {e}")
            raise
    
    def fetch_results_upto_bowls(self, season: int) -> List[Dict]:
        """Fetch all regular season and postseason game results"""
        try:
            self.logger.info(f"Fetching all completed games for {season} season")
            
            # Fetch regular season games
            regular_games = self.games_api.get_games(year=season, season_type='regular')
            postseason_games = self.games_api.get_games(year=season, season_type='postseason')
            
            all_games = []
            
            # Process regular season games
            for game in regular_games:
                if game.completed:
                    game_dict = {
                        'id': game.id,
                        'season': game.season,
                        'week': game.week,
                        'season_type': game.season_type,
                        'start_date': game.start_date.isoformat() if game.start_date else None,
                        'neutral_site': game.neutral_site,
                        'conference_game': game.conference_game,
                        'attendance': game.attendance,
                        'venue_id': game.venue_id,
                        'venue': game.venue,
                        'home_id': game.home_id,
                        'home_team': game.home_team,
                        'home_conference': game.home_conference,
                        'home_points': game.home_points,
                        'home_line_scores': game.home_line_scores,
                        'home_pregame_elo': getattr(game, 'home_pregame_elo', None),
                        'home_postgame_elo': getattr(game, 'home_postgame_elo', None),
                        'away_id': game.away_id,
                        'away_team': game.away_team,
                        'away_conference': game.away_conference,
                        'away_points': game.away_points,
                        'away_line_scores': game.away_line_scores,
                        'away_pregame_elo': getattr(game, 'away_pregame_elo', None),
                        'away_postgame_elo': getattr(game, 'away_postgame_elo', None),
                        'excitement_index': getattr(game, 'excitement_index', None),
                        'highlights': getattr(game, 'highlights', None),
                        'notes': getattr(game, 'notes', None)
                    }
                    all_games.append(game_dict)
            
            # Process postseason games
            for game in postseason_games:
                if game.completed:
                    game_dict = {
                        'id': game.id,
                        'season': game.season,
                        'week': game.week,
                        'season_type': game.season_type,
                        'start_date': game.start_date.isoformat() if game.start_date else None,
                        'neutral_site': game.neutral_site,
                        'conference_game': game.conference_game,
                        'attendance': game.attendance,
                        'venue_id': game.venue_id,
                        'venue': game.venue,
                        'home_id': game.home_id,
                        'home_team': game.home_team,
                        'home_conference': game.home_conference,
                        'home_points': game.home_points,
                        'home_line_scores': game.home_line_scores,
                        'home_pregame_elo': getattr(game, 'home_pregame_elo', None),
                        'home_postgame_elo': getattr(game, 'home_postgame_elo', None),
                        'away_id': game.away_id,
                        'away_team': game.away_team,
                        'away_conference': game.away_conference,
                        'away_points': game.away_points,
                        'away_line_scores': game.away_line_scores,
                        'away_pregame_elo': getattr(game, 'away_pregame_elo', None),
                        'away_postgame_elo': getattr(game, 'away_postgame_elo', None),
                        'excitement_index': getattr(game, 'excitement_index', None),
                        'highlights': getattr(game, 'highlights', None),
                        'notes': getattr(game, 'notes', None)
                    }
                    all_games.append(game_dict)
            
            self.logger.info(f"Fetched {len(all_games)} completed games for {season}")
            
            # Save raw data
            os.makedirs('data/raw', exist_ok=True)
            with open(f'data/raw/games_{season}.json', 'w') as f:
                json.dump(all_games, f, indent=2)
            
            return all_games
            
        except ApiException as e:
            self.logger.error(f"CFBD API request failed: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Failed to fetch games: {e}")
            raise

    def process_game_data(self, games: List[Dict], teams: List[Dict]) -> 'pd.DataFrame':
        """Process raw game data into a clean DataFrame with team validation"""
        import pandas as pd
        
        # Build team lookup for conference validation
        team_lookup = {team['school']: team for team in teams}
        
        game_records = []
        for game in games:
            home_team = game.get('home_team')
            away_team = game.get('away_team')
            
            # Skip games with missing team data
            if not home_team or not away_team:
                continue
            
            # Strip whitespace from team names
            home_team = home_team.strip()
            away_team = away_team.strip()
            
            # Skip if teams not in FBS lookup
            if home_team not in team_lookup or away_team not in team_lookup:
                continue
            
            home_points = game.get('home_points', 0) or 0
            away_points = game.get('away_points', 0) or 0
            
            winner = home_team if home_points > away_points else away_team
            loser = away_team if home_points > away_points else home_team
            
            # Get conferences from authoritative team data
            winner_conference = team_lookup[winner]['conference']
            loser_conference = team_lookup[loser]['conference']
            
            game_records.append({
                'winner': winner,
                'loser': loser,
                'winner_conference': winner_conference,
                'loser_conference': loser_conference,
                'margin': abs(home_points - away_points),
                'venue': 'neutral' if game.get('neutral_site') else 'home',
                'week': game.get('week'),
                'season_type': game.get('season_type'),
                'bowl_intra_conf': (
                    winner_conference == loser_conference and 
                    game.get('season_type') == 'postseason'
                )
            })
        
        if not game_records:
            return pd.DataFrame(columns=[
                'winner', 'loser', 'winner_conference', 'loser_conference', 
                'margin', 'venue', 'week', 'season_type', 'bowl_intra_conf'
            ])
        
        return pd.DataFrame(game_records)
    
    def validate_data_integrity(self, games: List[Dict], teams: List[Dict]) -> Dict[str, bool]:
        """Validate data integrity using available foundational models"""
        try:
            self.logger.info("Validating data integrity using Team and Game models")
            
            # Build team lookup from authoritative Team model data
            team_lookup = {team['school']: team for team in teams}
            fbs_teams = {team['school'] for team in teams if team['classification'] == 'fbs'}
            
            # Validate game data against authoritative team data
            validation_results = {
                'fbs_teams_valid': True,
                'conference_assignments_valid': True,
                'game_teams_valid': True,
                'missing_teams': [],
                'invalid_conferences': []
            }
            
            # Check each game against authoritative team data
            for game in games:
                home_team = game.get('home_team')
                away_team = game.get('away_team')
                home_conf = game.get('home_conference', '')
                away_conf = game.get('away_conference', '')
                
                if not home_team or not away_team:
                    continue
                
                # Validate teams exist in FBS
                if home_team not in fbs_teams:
                    validation_results['missing_teams'].append(home_team)
                    validation_results['fbs_teams_valid'] = False
                
                if away_team not in fbs_teams:
                    validation_results['missing_teams'].append(away_team)
                    validation_results['fbs_teams_valid'] = False
                
                # Validate conference assignments
                if home_team in team_lookup:
                    official_conf = team_lookup[home_team]['conference']
                    if home_conf and home_conf != official_conf:
                        validation_results['invalid_conferences'].append({
                            'team': home_team,
                            'game_conference': home_conf,
                            'official_conference': official_conf
                        })
                        validation_results['conference_assignments_valid'] = False
                
                if away_team in team_lookup:
                    official_conf = team_lookup[away_team]['conference']
                    if away_conf and away_conf != official_conf:
                        validation_results['invalid_conferences'].append({
                            'team': away_team,
                            'game_conference': away_conf,
                            'official_conference': official_conf
                        })
                        validation_results['conference_assignments_valid'] = False
            
            # Log validation results
            if all(validation_results.values()):
                self.logger.info("Data integrity validation PASSED using foundational models")
            else:
                self.logger.warning("Data integrity validation found issues")
                if validation_results['missing_teams']:
                    self.logger.warning(f"  Missing FBS teams: {len(set(validation_results['missing_teams']))}")
                if validation_results['invalid_conferences']:
                    self.logger.warning(f"  Conference assignment errors: {len(validation_results['invalid_conferences'])}")
            
            return validation_results
            
        except Exception as e:
            self.logger.error(f"Data integrity validation failed: {e}")
            return {
                'fbs_teams_valid': False,
                'conference_assignments_valid': False,
                'game_teams_valid': False,
                'error': str(e)
            }

def create_cfbd_client(config: Dict = None) -> ModernCFBDClient:
    """Factory function for creating CFBD client"""
    if config is None:
        # Load default config
        try:
            import yaml
            with open('config.yaml', 'r') as f:
                config = yaml.safe_load(f)
        except FileNotFoundError:
            config = {'api': {}, 'paths': {'data_raw': 'data/raw'}}
    
    return ModernCFBDClient(config)