"""
API reliability and fallback mechanisms
Implements exponential backoff, timeout handling, and data integrity verification
"""

import time
import requests
import logging
from typing import Dict, List, Optional
from pathlib import Path
import json

class APIReliabilityManager:
    """Manages API calls with reliability safeguards"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.max_retries = config.get('api', {}).get('max_retries', 3)
        self.base_delay = config.get('api', {}).get('base_delay', 1.0)
        self.timeout = config.get('api', {}).get('timeout', 30)
        
    def make_reliable_request(self, url: str, headers: Dict = None, params: Dict = None) -> Dict:
        """Make API request with exponential backoff and error handling"""
        
        for attempt in range(self.max_retries):
            try:
                self.logger.debug(f"API request attempt {attempt + 1}/{self.max_retries}: {url}")
                
                response = requests.get(
                    url, 
                    headers=headers or {}, 
                    params=params or {},
                    timeout=self.timeout
                )
                
                if response.status_code == 200:
                    self.logger.debug(f"API request successful: {url}")
                    return response.json()
                
                elif response.status_code == 429:  # Rate limited
                    retry_after = int(response.headers.get('Retry-After', self.base_delay * (2 ** attempt)))
                    self.logger.warning(f"Rate limited, waiting {retry_after}s before retry")
                    time.sleep(retry_after)
                    continue
                
                elif response.status_code >= 500:  # Server error
                    delay = self.base_delay * (2 ** attempt)
                    self.logger.warning(f"Server error {response.status_code}, retrying in {delay}s")
                    time.sleep(delay)
                    continue
                
                else:
                    # Client error - don't retry
                    self.logger.error(f"API error {response.status_code}: {response.text}")
                    raise requests.RequestException(f"API returned {response.status_code}")
                    
            except requests.Timeout:
                delay = self.base_delay * (2 ** attempt)
                self.logger.warning(f"Request timeout, retrying in {delay}s")
                if attempt < self.max_retries - 1:
                    time.sleep(delay)
                    continue
                else:
                    raise
                    
            except requests.ConnectionError:
                delay = self.base_delay * (2 ** attempt)
                self.logger.warning(f"Connection error, retrying in {delay}s")
                if attempt < self.max_retries - 1:
                    time.sleep(delay)
                    continue
                else:
                    raise
        
        # All retries failed
        raise requests.RequestException(f"API request failed after {self.max_retries} attempts")
    
    def verify_data_freshness(self, data: List[Dict], season: int, expected_min_games: int = 700) -> bool:
        """Verify API data meets freshness and completeness requirements"""
        
        if not data:
            self.logger.error("Empty dataset received from API")
            return False
        
        if len(data) < expected_min_games:
            self.logger.error(f"Incomplete dataset: {len(data)} games < {expected_min_games} expected")
            return False
        
        # Check season consistency
        seasons_found = set(game.get('season') for game in data if game.get('season'))
        if season not in seasons_found:
            self.logger.error(f"Expected season {season} not found in data")
            return False
        
        # Check for reasonable game distribution
        season_games = [game for game in data if game.get('season') == season]
        if len(season_games) < expected_min_games * 0.8:
            self.logger.warning(f"Low game count for season {season}: {len(season_games)}")
        
        self.logger.info(f"Data freshness verified: {len(season_games)} games for season {season}")
        return True
    
    def fallback_to_cached_data(self, season: int, week: Optional[int] = None) -> Optional[List[Dict]]:
        """Attempt to load cached data when API is unavailable"""
        
        cache_patterns = [
            f"data/raw/games_{season}_fbs_complete.json",
            f"data/raw/games_{season}_week{week:02d}.json" if week else None,
            f"data/backup/games_{season}_backup.json"
        ]
        
        for cache_path in cache_patterns:
            if cache_path and Path(cache_path).exists():
                try:
                    with open(cache_path, 'r') as f:
                        cached_data = json.load(f)
                    
                    self.logger.info(f"Using cached data from {cache_path}")
                    return cached_data
                    
                except (json.JSONDecodeError, IOError) as e:
                    self.logger.warning(f"Failed to load cached data from {cache_path}: {e}")
                    continue
        
        self.logger.error("No valid cached data found for fallback")
        return None
    
    def save_backup_data(self, data: List[Dict], season: int, week: Optional[int] = None) -> None:
        """Save backup copy of data for future fallback"""
        
        backup_dir = Path('data/backup')
        backup_dir.mkdir(parents=True, exist_ok=True)
        
        if week:
            backup_path = backup_dir / f'games_{season}_week{week:02d}_backup.json'
        else:
            backup_path = backup_dir / f'games_{season}_backup.json'
        
        try:
            with open(backup_path, 'w') as f:
                json.dump(data, f, indent=2)
            
            self.logger.debug(f"Backup data saved to {backup_path}")
            
        except IOError as e:
            self.logger.warning(f"Failed to save backup data: {e}")

class EndToEndSmokeTest:
    """Comprehensive smoke tests for data integrity after changes"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.logger = logging.getLogger(__name__)
    
    def run_byu_style_smoke_test(self, games_df) -> Dict[str, bool]:
        """Run BYU-style metrics validation"""
        results = {}
        
        # 1. No teams missing games
        all_teams = set()
        for _, game in games_df.iterrows():
            all_teams.add(game['winner'])
            all_teams.add(game['loser'])
        
        team_game_counts = {}
        for team in all_teams:
            count = len(games_df[(games_df['winner'] == team) | (games_df['loser'] == team)])
            team_game_counts[team] = count
        
        # Check for teams with suspiciously few games
        missing_games = [team for team, count in team_game_counts.items() if count < 8]
        results['no_missing_games'] = len(missing_games) == 0
        
        if missing_games:
            self.logger.error(f"Teams with missing games: {missing_games[:5]}")
        
        # 2. Conference strength vector reasonable
        conferences = set()
        for _, game in games_df.iterrows():
            if game.get('winner_conference'):
                conferences.add(game['winner_conference'])
            if game.get('loser_conference'):
                conferences.add(game['loser_conference'])
        
        expected_conferences = 11  # Major FBS conferences
        results['conference_count_reasonable'] = len(conferences) >= expected_conferences * 0.8
        
        if len(conferences) < expected_conferences * 0.8:
            self.logger.error(f"Too few conferences: {len(conferences)} < {expected_conferences * 0.8}")
        
        # 3. Game count reasonable for complete season
        total_games = len(games_df)
        expected_min_games = 700  # ~134 teams * 12 games / 2
        results['game_count_reasonable'] = total_games >= expected_min_games
        
        if total_games < expected_min_games:
            self.logger.error(f"Too few games: {total_games} < {expected_min_games}")
        
        # 4. Score distribution reasonable
        margins = games_df['margin'].tolist()
        avg_margin = sum(margins) / len(margins) if margins else 0
        results['margin_distribution_reasonable'] = 5 <= avg_margin <= 25
        
        if not (5 <= avg_margin <= 25):
            self.logger.error(f"Unreasonable average margin: {avg_margin}")
        
        self.logger.info(f"Smoke test results: {sum(results.values())}/{len(results)} passed")
        return results
    
    def run_full_pipeline_smoke_test(self, season: int, week: Optional[int] = None) -> bool:
        """Run end-to-end pipeline smoke test"""
        
        try:
            from src.ingest import CFBDataIngester
            
            # Test data ingestion
            ingester = CFBDataIngester(self.config)
            
            if week:
                games = ingester.fetch_results_upto_week(week, season)
            else:
                games = ingester.fetch_results_upto_bowls(season)
            
            games_df = ingester.process_game_data(games)
            
            # Run BYU-style validation
            smoke_results = self.run_byu_style_smoke_test(games_df)
            
            # Check critical metrics pass
            critical_checks = ['no_missing_games', 'game_count_reasonable']
            critical_passed = all(smoke_results.get(check, False) for check in critical_checks)
            
            if not critical_passed:
                self.logger.error("Critical smoke test checks failed")
                return False
            
            self.logger.info("End-to-end smoke test passed")
            return True
            
        except Exception as e:
            self.logger.error(f"Smoke test failed with exception: {e}")
            return False

def create_api_manager(config: Dict = None) -> APIReliabilityManager:
    """Factory function for API reliability manager"""
    if config is None:
        import yaml
        with open('config.yaml', 'r') as f:
            config = yaml.safe_load(f)
    
    return APIReliabilityManager(config)