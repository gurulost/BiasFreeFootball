"""
Data ingestion module for CFB API
Handles fetching schedule, results, and team information
"""

import requests
import json
import os
import logging
import yaml
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
        
        # Load canonical team mapping for data validation
        self.canonical_teams = self._load_canonical_teams()
        
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
        """Process raw game data with production-grade validation"""
        from src.validation import DataValidator
        
        # Run comprehensive validation suite
        validator = DataValidator(self.config)
        
        # First pass: strict schema and outlier validation
        if self.config.get('validation', {}).get('enable_hardening', True):
            season = games[0].get('season', 2024) if games else 2024
            games_df = validator.validate_complete_dataset(
                games, self.canonical_teams, season
            )
            
            # Canonical mapping already applied in validation
            return games_df
        
        # Fallback to legacy validation for development
        return self._legacy_process_game_data(games)
    
    def _legacy_process_game_data(self, games: List[Dict]) -> pd.DataFrame:
        """Legacy processing with basic validation (for development)"""
        processed_games = []
        bad_rows = []
        validation_warnings = []
        missing_aliases = set()
        
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
            
            # Validate and canonicalize team names
            home_canonical = self.canonicalize_team(home_team) if home_team else None
            away_canonical = self.canonicalize_team(away_team) if away_team else None
            
            # Check for missing team canonicalization and collect aliases
            if home_team and not home_canonical:
                validation_warnings.append(f"Unknown home team: {home_team}")
                missing_aliases.add(home_team)
            if away_team and not away_canonical:
                validation_warnings.append(f"Unknown away team: {away_team}")
                missing_aliases.add(away_team)
            
            # Apply canonical names and conferences if available
            if home_canonical:
                home_team = home_canonical['name']
                if not home_conf or pd.isna(home_conf):
                    home_conf = home_canonical['conf']
            
            if away_canonical:
                away_team = away_canonical['name']
                if not away_conf or pd.isna(away_conf):
                    away_conf = away_canonical['conf']
                
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
        
        # Save missing aliases report for automated processing
        if missing_aliases:
            self._save_missing_aliases_report(missing_aliases, games[0].get('season', 2024))
        
        # Log validation results
        if validation_warnings:
            self.logger.warning(f"Data validation warnings: {len(validation_warnings)} issues found")
            for warning in validation_warnings[:10]:  # Log first 10 warnings
                self.logger.warning(f"  {warning}")
        
        if bad_rows:
            self.logger.error(f"Data validation failed: {len(bad_rows)} games dropped due to data issues")
            for bad_row in bad_rows[:5]:  # Log first 5 bad rows
                self.logger.error(f"  Dropped: {bad_row}")
            
            # Fail if too many bad rows (indicates systematic data issue)
            if len(bad_rows) > len(games) * 0.05:  # More than 5% bad data
                raise ValueError(f"Too many data validation failures: {len(bad_rows)}/{len(games)} games")
        
        # CI blocking mechanism: Assert zero missing aliases in strict mode
        if self.config.get('validation', {}).get('strict_mode', False):
            assert not missing_aliases, \
                f"Unhandled aliases in strict mode: {sorted(missing_aliases)}"
        
        df = pd.DataFrame(processed_games)
        
        # Validate schedule completeness for FBS teams
        if not df.empty:
            self._validate_schedule_completeness(df)
        
        return df
    
    def _validate_schedule_completeness(self, games_df: pd.DataFrame) -> None:
        """Validate that all FBS teams have complete schedules"""
        # Get all teams that appear in games
        all_teams = set()
        for _, game in games_df.iterrows():
            all_teams.add(game['winner'])
            all_teams.add(game['loser'])
        
        # Count games per team
        team_game_counts = {}
        for team in all_teams:
            team_games = games_df[
                (games_df['winner'] == team) | (games_df['loser'] == team)
            ]
            team_game_counts[team] = len(team_games)
        
        # Check for teams with suspiciously few games (< 8 games suggests missing data)
        missing_games = []
        for team, count in team_game_counts.items():
            if count < 8:  # Most FBS teams play 12+ games
                missing_games.append((team, count))
        
        if missing_games:
            self.logger.warning(f"Teams with potentially incomplete schedules:")
            for team, count in sorted(missing_games):
                self.logger.warning(f"  {team}: {count} games (expected 10-15)")
                
            # Special check for known major teams
            major_teams = ['BYU', 'Ohio State', 'Georgia', 'Alabama', 'Texas', 'Notre Dame']
            for team in major_teams:
                if team in team_game_counts and team_game_counts[team] < 10:
                    self.logger.error(f"CRITICAL: {team} has only {team_game_counts[team]} games - data integrity issue!")
        
        self.logger.info(f"Schedule validation complete: {len(all_teams)} teams, {len(games_df)} games")
    
    def _save_missing_aliases_report(self, missing_aliases: set, season: int) -> None:
        """Save missing aliases report for automated processing"""
        import json
        import os
        
        # Create reports directory if it doesn't exist
        reports_dir = 'reports'
        os.makedirs(reports_dir, exist_ok=True)
        
        # Save missing aliases as JSON for tool processing
        report_path = f"{reports_dir}/missing_aliases_{season}.json"
        with open(report_path, 'w') as f:
            json.dump(sorted(list(missing_aliases)), f, indent=2)
        
        self.logger.info(f"Missing aliases report saved to {report_path}")
        self.logger.info(f"To auto-generate placeholders, run: python tools/add_placeholders.py {report_path}")

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
