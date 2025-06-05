"""
Data ingestion module for CFB API
Handles fetching schedule, results, and team information
"""

import requests
import json
import os
import logging
from datetime import datetime
from typing import Dict, List, Optional
import pandas as pd

class CFBDataIngester:
    def __init__(self, config: Dict):
        self.config = config
        self.api_key = os.getenv('CFB_API_KEY', config.get('api', {}).get('key', ''))
        self.base_url = config.get('api', {}).get('base_url', 'https://api.collegefootballdata.com')
        self.headers = {}
        if self.api_key:
            self.headers['Authorization'] = f'Bearer {self.api_key}'
        
        self.logger = logging.getLogger(__name__)
        
    def _make_request(self, endpoint: str, params: Dict = None) -> Dict:
        """Make API request with error handling"""
        url = f"{self.base_url}/{endpoint}"
        
        try:
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            self.logger.error(f"API request failed: {e}")
            raise
            
    def fetch_teams(self, season: int, division: str = 'fbs') -> List[Dict]:
        """Fetch team information for a given season - FBS only"""
        params = {
            'year': season,
            'division': division
        }
        
        teams = self._make_request('teams', params)
        
        # Filter for authentic FBS teams only based on classification
        fbs_teams = [team for team in teams if team.get('classification') == 'fbs']
        
        self.logger.info(f"Filtered {len(fbs_teams)} FBS teams from {len(teams)} total teams")
        
        # Save raw data
        raw_path = f"{self.config['paths']['data_raw']}/teams_{season}_fbs.json"
        os.makedirs(os.path.dirname(raw_path), exist_ok=True)
        with open(raw_path, 'w') as f:
            json.dump(fbs_teams, f, indent=2)
            
        return fbs_teams
    
    def fetch_conferences(self, season: int) -> List[Dict]:
        """Fetch conference information"""
        params = {'year': season}
        
        conferences = self._make_request('conferences', params)
        
        # Save raw data
        raw_path = f"{self.config['paths']['data_raw']}/conferences_{season}.json"
        os.makedirs(os.path.dirname(raw_path), exist_ok=True)
        with open(raw_path, 'w') as f:
            json.dump(conferences, f, indent=2)
            
        return conferences
    
    def fetch_games(self, season: int, week: Optional[int] = None, 
                   season_type: str = 'regular') -> List[Dict]:
        """Fetch game results up to specified week"""
        params = {
            'year': season,
            'seasonType': season_type,
            'division': 'fbs'
        }
        
        if week:
            params['week'] = week
            
        games = self._make_request('games', params)
        
        # Filter only completed games
        completed_games = [g for g in games if g.get('completed', False)]
        
        # Save raw data
        week_str = f"_week{week}" if week else ""
        raw_path = f"{self.config['paths']['data_raw']}/games_{season}{week_str}.json"
        os.makedirs(os.path.dirname(raw_path), exist_ok=True)
        with open(raw_path, 'w') as f:
            json.dump(completed_games, f, indent=2)
            
        return completed_games
    
    def fetch_results_upto_week(self, week: int, season: int) -> List[Dict]:
        """Fetch all completed games up to and including specified week - FBS only"""
        all_games = []
        
        # Get authentic FBS teams list for strict filtering
        fbs_teams = self.fetch_teams(season, division='fbs')
        fbs_team_names = {team['school'] for team in fbs_teams}
        
        self.logger.info(f"Filtering games using {len(fbs_team_names)} authentic FBS teams")
        
        # Regular season games
        for w in range(1, week + 1):
            try:
                week_games = self.fetch_games(season, w, 'regular')
                # Filter for FBS-only games (both teams must be FBS)
                fbs_week_games = [g for g in week_games 
                                 if (g.get('homeClassification') == 'fbs' and 
                                     g.get('awayClassification') == 'fbs')]
                all_games.extend(fbs_week_games)
                self.logger.info(f"Week {w}: {len(fbs_week_games)} FBS-only games from {len(week_games)} total")
            except Exception as e:
                self.logger.warning(f"Failed to fetch week {w}: {e}")
                
        self.logger.info(f"Total FBS-only games collected: {len(all_games)}")
        return all_games
    
    def fetch_results_upto_bowls(self, season: int) -> List[Dict]:
        """Fetch all completed games including bowls - FBS teams only"""
        all_games = []
        
        # Get authentic FBS teams list for strict filtering
        fbs_teams = self.fetch_teams(season, division='fbs')
        fbs_team_names = {team['school'] for team in fbs_teams}
        
        self.logger.info(f"Filtering complete season using {len(fbs_team_names)} authentic FBS teams")
        
        # Regular season games with FBS-only filtering
        regular_games = self.fetch_games(season, season_type='regular')
        fbs_regular_games = [g for g in regular_games 
                            if (g.get('homeClassification') == 'fbs' and 
                                g.get('awayClassification') == 'fbs')]
        all_games.extend(fbs_regular_games)
        
        # Postseason games with FBS-only filtering
        bowl_games = self.fetch_games(season, season_type='postseason')
        fbs_bowl_games = [g for g in bowl_games 
                         if (g.get('homeClassification') == 'fbs' and 
                             g.get('awayClassification') == 'fbs')]
        all_games.extend(fbs_bowl_games)
        
        self.logger.info(f"FBS-only games: {len(all_games)} (regular: {len(fbs_regular_games)}, bowls: {len(fbs_bowl_games)})")
        self.logger.info(f"Filtered out {len(regular_games) + len(bowl_games) - len(all_games)} non-FBS games")
        
        # Save raw data
        raw_path = f"{self.config['paths']['data_raw']}/games_{season}_fbs_complete.json"
        os.makedirs(os.path.dirname(raw_path), exist_ok=True)
        with open(raw_path, 'w') as f:
            json.dump(all_games, f, indent=2)
            
        return all_games
    
    def process_game_data(self, games: List[Dict]) -> pd.DataFrame:
        """Process raw game data into standardized format"""
        processed_games = []
        
        for game in games:
            if not game.get('completed', False):
                continue
                
            home_team = game.get('homeTeam')
            away_team = game.get('awayTeam')
            home_score = game.get('homePoints', 0)
            away_score = game.get('awayPoints', 0)
            home_conf = game.get('homeConference')
            away_conf = game.get('awayConference')
            
            if not all([home_team, away_team, home_score is not None, away_score is not None]):
                continue
                
            # Determine winner/loser and venue
            if game.get('neutralSite', False):
                venue = 'neutral'
            elif home_score > away_score:
                venue = 'home'
            else:
                venue = 'away'
                
            if home_score > away_score:
                winner, loser = home_team, away_team
                winner_score, loser_score = home_score, away_score
                winner_conf, loser_conf = home_conf, away_conf
                winner_home = True
            else:
                winner, loser = away_team, home_team
                winner_score, loser_score = away_score, home_score
                winner_conf, loser_conf = away_conf, home_conf
                winner_home = False
                
            processed_game = {
                'game_id': game.get('id'),
                'season': game.get('season'),
                'week': game.get('week'),
                'season_type': game.get('season_type'),
                'winner': winner,
                'loser': loser,
                'winner_score': winner_score,
                'loser_score': loser_score,
                'margin': abs(winner_score - loser_score),
                'venue': venue,
                'neutral_site': game.get('neutral_site', False),
                'winner_conference': winner_conf,
                'loser_conference': loser_conf,
                'points_winner': winner_score,
                'points_loser': loser_score,
                'winner_home': winner_home,
                'date': game.get('start_date'),
                'is_bowl': game.get('season_type') == 'postseason'
            }
            
            # Determine if cross-conference
            processed_game['cross_conf'] = (
                processed_game['winner_conference'] != processed_game['loser_conference']
                and processed_game['winner_conference'] is not None
                and processed_game['loser_conference'] is not None
            )
            
            processed_games.append(processed_game)
            
        return pd.DataFrame(processed_games)

def fetch_results_upto_week(week: int, season: int, config: Dict = None) -> pd.DataFrame:
    """Convenience function for pipeline use"""
    if config is None:
        import yaml
        with open('config.yaml', 'r') as f:
            config = yaml.safe_load(f)
            
    ingester = CFBDataIngester(config)
    games = ingester.fetch_results_upto_week(week, season)
    return ingester.process_game_data(games)

def fetch_results_upto_bowls(season: int, config: Dict = None) -> pd.DataFrame:
    """Convenience function for retro pipeline use"""
    if config is None:
        import yaml
        with open('config.yaml', 'r') as f:
            config = yaml.safe_load(f)
            
    ingester = CFBDataIngester(config)
    games = ingester.fetch_results_upto_bowls(season)
    return ingester.process_game_data(games)
