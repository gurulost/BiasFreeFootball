"""
Data ingestion module for CFB API
Handles fetching schedule, results, and team information using the official cfbd-python library
"""

import os
import logging
import yaml
from datetime import datetime
from typing import Dict, List, Optional
import pandas as pd
import cfbd
from cfbd.exceptions import ApiException

class CFBDataIngester:
    def __init__(self, config: Dict):
        self.config = config
        self.logger = logging.getLogger(__name__)

        # Configure the official CFBD API client
        from cfbd import Configuration, ApiClient, TeamsApi, GamesApi, ConferencesApi

        configuration = Configuration()
        api_key = os.getenv('CFB_API_KEY', config.get('api', {}).get('key', ''))
        if not api_key:
            self.logger.error("API key not found in config.yaml or CFB_API_KEY environment variable.")
            raise ValueError("API Key is missing.")

        # Set the access token directly as per cfbd library documentation
        configuration.access_token = api_key

        # Create API client and specific API instances
        api_client = ApiClient(configuration)
        self.teams_api = TeamsApi(api_client)
        self.games_api = GamesApi(api_client)
        self.conferences_api = ConferencesApi(api_client)

        # Initialize FBS enforcer for data validation
        from src.fbs_enforcer import create_fbs_enforcer
        self.fbs_enforcer = create_fbs_enforcer(config)

        # Load canonical team mapping for data validation
        self.canonical_teams = self._load_canonical_teams()
        self.conference_cache = {}  # Cache for conference ID to name mapping

    def _load_canonical_teams(self) -> Dict:
        """Load canonical team name mapping"""
        try:
            with open('data/canonical_teams.yaml', 'r') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            self.logger.warning("Canonical teams file not found - data validation disabled")
            return {}

    def canonicalize_team(self, team_name: str) -> Optional[Dict]:
        """Convert team name to canonical form with conference info"""
        if not team_name or not self.canonical_teams:
            return None

        # Direct lookup
        if team_name in self.canonical_teams:
            return self.canonical_teams[team_name]

        # Case-insensitive lookup
        for key, value in self.canonical_teams.items():
            if key.lower() == team_name.lower():
                return value

        return None

    def get_conference_name(self, conference_data, season: int) -> str:
        """Get conference name from API data"""
        # Handle both direct string and ID-based conference data
        if isinstance(conference_data, str):
            return conference_data
        elif isinstance(conference_data, int):
            # Fallback to ID mapping if needed
            if not self.conference_cache:
                self.fetch_conferences()
            return self.conference_cache.get(conference_data, 'Unknown')
        return 'Unknown'

    def fetch_teams(self, season: int, classification: str = 'fbs') -> List[Dict]:
        """Fetch all teams for a given season, enforcing FBS classification."""
        try:
            # Always fetch FBS teams
            api_response = self.teams_api.get_fbs_teams(year=season)
            return [team.to_dict() for team in api_response]
        except ApiException as e:
            self.logger.error(f"Error fetching FBS teams: {e}")
            return []

    def fetch_games(self, season: int, week: int, season_type: str = 'regular', classification: str = 'fbs') -> List[Dict]:
        """Fetch all games for a given week and season, enforcing FBS classification."""
        try:
            # Always fetch FBS games
            api_response = self.games_api.get_games(year=season, week=week, season_type=season_type, classification=classification)
            return [game.to_dict() for game in api_response]
        except ApiException as e:
            self.logger.error(f"Error fetching FBS games: {e}")
            return []

    def fetch_conferences(self) -> List[Dict]:
        """Fetch all conference information."""
        try:
            api_response = self.conferences_api.get_conferences()
            conferences = [conf.to_dict() for conf in api_response]

            # Update cache
            for conf in conferences:
                self.conference_cache[conf['id']] = conf['name']

            return conferences
        except ApiException as e:
            self.logger.error(f"Error fetching conferences: {e}")
            return []

    def fetch_results_upto_bowls(self, season: int) -> List[Dict]:
        """Fetch all regular season and postseason game results."""
        regular_games = self.games_api.get_games(year=season, season_type='regular', classification='fbs')
        postseason_games = self.games_api.get_games(year=season, season_type='postseason', classification='fbs')
        return [game.to_dict() for game in regular_games + postseason_games]

    def process_game_data(self, games: List[Dict]) -> pd.DataFrame:
        """Process raw game data into a clean DataFrame."""
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

            winner = home_team if game.get('home_points', 0) > game.get('away_points', 0) else away_team
            loser = away_team if game.get('home_points', 0) > game.get('away_points', 0) else home_team

            winner_data = self.canonicalize_team(winner)
            loser_data = self.canonicalize_team(loser)

            game_records.append({
                'winner': winner,
                'loser': loser,
                'winner_conference': winner_data.get('conf') if winner_data else 'Unknown',
                'loser_conference': loser_data.get('conf') if loser_data else 'Unknown',
                'margin': abs(game.get('home_points', 0) - game.get('away_points', 0)),
                'venue': 'neutral' if game.get('neutral_site') else 'home',
                'week': game.get('week'),
                'season_type': game.get('season_type'),
                'bowl_intra_conf': False  # Default value
            })

        if not game_records:
            return pd.DataFrame(columns=['winner', 'loser', 'winner_conference', 'loser_conference', 'margin', 'venue', 'week', 'season_type', 'bowl_intra_conf'])

        return pd.DataFrame(game_records)

def fetch_results_upto_week(week: int, season: int = 2024) -> pd.DataFrame:
    """Fetch game results up to specified week for compatibility"""
    import yaml
    
    # Load configuration
    try:
        with open('config.yaml', 'r') as f:
            config = yaml.safe_load(f)
    except FileNotFoundError:
        config = {'api': {}, 'paths': {'data_raw': 'data/raw'}}
    
    # Create ingester instance
    ingester = CFBDataIngester(config)
    
    # Fetch games using modernized approach
    games = ingester.fetch_results_upto_bowls(season)
    
    # Convert to DataFrame format expected by live pipeline
    return ingester.process_game_data(games)

def fetch_results_upto_bowls(season: int) -> list:
    """Fetch all game results including bowls for compatibility"""
    import yaml
    
    # Load configuration
    try:
        with open('config.yaml', 'r') as f:
            config = yaml.safe_load(f)
    except FileNotFoundError:
        config = {'api': {}, 'paths': {'data_raw': 'data/raw'}}
    
    # Create ingester instance
    ingester = CFBDataIngester(config)
    
    # Return raw game data
    return ingester.fetch_results_upto_bowls(season)