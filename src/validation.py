"""
Comprehensive data validation and hardening module
Implements production-grade data quality checks and schema validation
"""

from pydantic import BaseModel, constr, conint, validator
from typing import List, Dict, Set, Literal, Optional
import numpy as np
import pandas as pd
import hashlib
import json
import logging
from pathlib import Path

class GameRecord(BaseModel):
    """Pydantic schema for strict game data validation"""
    season: conint(ge=1900, le=2030)
    week: conint(ge=1, le=20)
    home_team: constr(strip_whitespace=True, min_length=2)
    away_team: constr(strip_whitespace=True, min_length=2)
    home_points: conint(ge=0, le=200)
    away_points: conint(ge=0, le=200)
    neutral_site: bool
    season_type: Literal["regular", "postseason"]
    completed: bool = True
    
    @validator('home_team', 'away_team')
    def teams_must_be_different(cls, v, values):
        """Ensure teams don't play themselves"""
        if 'home_team' in values and v == values['home_team']:
            raise ValueError("Team cannot play itself")
        return v
    
    @validator('home_points', 'away_points')
    def reasonable_scores(cls, v):
        """Check for unreasonable scores"""
        if v > 150:
            raise ValueError(f"Score {v} exceeds reasonable limit")
        return v

class DataValidator:
    """Production-grade data validation and quality assurance"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.logger = logging.getLogger(__name__)
        
    def validate_schema(self, games: List[Dict]) -> List[GameRecord]:
        """Strict pydantic schema validation"""
        validated_games = []
        validation_errors = []
        
        for i, game in enumerate(games):
            try:
                # Map API fields to schema fields
                normalized_game = {
                    'season': game.get('season'),
                    'week': game.get('week'),
                    'home_team': game.get('homeTeam', '').strip(),
                    'away_team': game.get('awayTeam', '').strip(),
                    'home_points': game.get('homePoints', 0),
                    'away_points': game.get('awayPoints', 0),
                    'neutral_site': game.get('neutralSite', False),
                    'season_type': game.get('season_type', 'regular'),
                    'completed': game.get('completed', True)
                }
                
                validated_game = GameRecord(**normalized_game)
                validated_games.append(validated_game)
                
            except Exception as e:
                validation_errors.append(f"Game {i}: {e}")
        
        if validation_errors:
            self.logger.error(f"Schema validation failed: {len(validation_errors)} errors")
            for error in validation_errors[:10]:  # Log first 10 errors
                self.logger.error(f"  {error}")
            
            # Fail if too many validation errors
            if len(validation_errors) > len(games) * 0.01:  # More than 1% bad data
                raise ValueError(f"Too many schema validation failures: {len(validation_errors)}")
        
        self.logger.info(f"Schema validation passed: {len(validated_games)}/{len(games)} games valid")
        return validated_games
    
    def check_duplicates(self, games: List[GameRecord]) -> None:
        """Detect duplicate games"""
        game_ids = set()
        duplicates = []
        
        for game in games:
            # Create unique game identifier
            game_id = (game.season, game.week, game.home_team, game.away_team)
            
            if game_id in game_ids:
                duplicates.append(game_id)
            else:
                game_ids.add(game_id)
        
        if duplicates:
            self.logger.error(f"Duplicate games detected: {len(duplicates)}")
            for dup in duplicates[:5]:
                self.logger.error(f"  {dup}")
            raise ValueError(f"Duplicate games found: {duplicates}")
        
        self.logger.info(f"Duplicate check passed: {len(games)} unique games")
    
    def check_outliers(self, games: List[GameRecord]) -> None:
        """Detect statistical outliers and impossible values"""
        margins = [abs(game.home_points - game.away_points) for game in games]
        total_points = [game.home_points + game.away_points for game in games]
        
        # Check for suspiciously large margins
        margin_99th = np.percentile(margins, 99)
        if margin_99th > 70:
            self.logger.warning(f"Large margins detected: 99th percentile = {margin_99th}")
        
        # Check for impossibly high scores
        max_points = max(total_points) if total_points else 0
        if max_points > 150:
            high_scoring = [g for g in games if (g.home_points + g.away_points) > 150]
            self.logger.warning(f"High-scoring games: {len(high_scoring)} games > 150 points")
            for game in high_scoring[:3]:
                self.logger.warning(f"  {game.home_team} {game.home_points} - {game.away_points} {game.away_team}")
        
        # Check for ties in modern era (post-2005 overtime)
        modern_ties = [g for g in games if g.season > 2005 and g.home_points == g.away_points]
        if modern_ties:
            self.logger.warning(f"Tie games in overtime era: {len(modern_ties)} games")
        
        self.logger.info(f"Outlier check completed: max margin={max(margins)}, max total={max_points}")
    
    def canonical_roundtrip_test(self, canonical_teams: Dict) -> None:
        """Ensure canonical mapping is self-consistent"""
        roundtrip_failures = []
        
        for team_alias, team_data in canonical_teams.items():
            canonical_name = team_data['name']
            
            # Re-lookup the canonical name
            if canonical_name in canonical_teams:
                roundtrip_data = canonical_teams[canonical_name]
                if roundtrip_data['name'] != canonical_name:
                    roundtrip_failures.append((team_alias, canonical_name, roundtrip_data['name']))
        
        if roundtrip_failures:
            self.logger.error(f"Canonical roundtrip failures: {len(roundtrip_failures)}")
            for alias, expected, actual in roundtrip_failures:
                self.logger.error(f"  {alias} -> {expected} -> {actual}")
            raise ValueError("Canonical mapping is not self-consistent")
        
        self.logger.info(f"Canonical roundtrip test passed: {len(canonical_teams)} teams")
    
    def compute_data_checksum(self, data: List[Dict]) -> str:
        """Compute SHA256 checksum of raw data"""
        data_json = json.dumps(data, sort_keys=True, separators=(',', ':'))
        return hashlib.sha256(data_json.encode()).hexdigest()
    
    def save_checksum(self, checksum: str, season: int, week: Optional[int] = None) -> None:
        """Save data checksum for integrity verification"""
        checksums_dir = Path('data/raw')
        checksums_dir.mkdir(parents=True, exist_ok=True)
        
        checksum_file = checksums_dir / '_checksums.txt'
        
        # Create identifier
        if week is not None:
            identifier = f"{season}_week{week:02d}"
        else:
            identifier = f"{season}_complete"
        
        # Append checksum with timestamp
        import datetime
        timestamp = datetime.datetime.now().isoformat()
        
        with open(checksum_file, 'a') as f:
            f.write(f"{identifier},{checksum},{timestamp}\n")
        
        self.logger.info(f"Data checksum saved: {identifier} = {checksum[:12]}...")
    
    def generate_season_summary(self, games_df: pd.DataFrame, season: int) -> None:
        """Generate human-readable season summary for manual review"""
        reports_dir = Path('reports')
        reports_dir.mkdir(exist_ok=True)
        
        # Get all teams
        all_teams = set()
        for _, game in games_df.iterrows():
            all_teams.add(game['winner'])
            all_teams.add(game['loser'])
        
        # Compute team statistics
        team_stats = []
        for team in sorted(all_teams):
            team_games = games_df[
                (games_df['winner'] == team) | (games_df['loser'] == team)
            ]
            
            wins = len(games_df[games_df['winner'] == team])
            losses = len(games_df[games_df['loser'] == team])
            total_games = len(team_games)
            
            # Compute average margin
            margins = []
            for _, game in team_games.iterrows():
                if game['winner'] == team:
                    margins.append(game['margin'])  # Positive for wins
                else:
                    margins.append(-game['margin'])  # Negative for losses
            
            avg_margin = np.mean(margins) if margins else 0
            
            team_stats.append({
                'team': team,
                'games': total_games,
                'wins': wins,
                'losses': losses,
                'win_pct': wins / total_games if total_games > 0 else 0,
                'avg_margin': avg_margin
            })
        
        # Save summary
        summary_df = pd.DataFrame(team_stats)
        summary_path = reports_dir / f'season_{season}_team_summary.csv'
        summary_df.to_csv(summary_path, index=False)
        
        self.logger.info(f"Season summary saved: {summary_path}")
        
        # Log potential issues
        issues = []
        for _, row in summary_df.iterrows():
            if row['games'] < 8:
                issues.append(f"{row['team']}: only {row['games']} games")
            elif row['games'] > 16:
                issues.append(f"{row['team']}: {row['games']} games (many)")
        
        if issues:
            self.logger.warning(f"Potential scheduling issues:")
            for issue in issues[:10]:
                self.logger.warning(f"  {issue}")
    
    def validate_complete_dataset(self, games: List[Dict], canonical_teams: Dict, 
                                 season: int, week: Optional[int] = None) -> pd.DataFrame:
        """Run complete validation suite"""
        self.logger.info("Starting comprehensive data validation...")
        
        # 1. Schema validation
        validated_games = self.validate_schema(games)
        
        # 2. Duplicate detection
        self.check_duplicates(validated_games)
        
        # 3. Outlier detection
        self.check_outliers(validated_games)
        
        # 4. Canonical mapping consistency
        self.canonical_roundtrip_test(canonical_teams)
        
        # 5. Data integrity checksum
        checksum = self.compute_data_checksum(games)
        self.save_checksum(checksum, season, week)
        
        # Convert to DataFrame for further processing
        games_data = []
        for game in validated_games:
            # Determine winner/loser
            if game.home_points > game.away_points:
                winner, loser = game.home_team, game.away_team
                winner_score, loser_score = game.home_points, game.away_points
                venue = 'home'
            else:
                winner, loser = game.away_team, game.home_team
                winner_score, loser_score = game.away_points, game.home_points
                venue = 'away'
            
            if game.neutral_site:
                venue = 'neutral'
            
            # Apply canonical team mapping
            winner_canonical = canonical_teams.get(winner, {'name': winner, 'conf': None})
            loser_canonical = canonical_teams.get(loser, {'name': loser, 'conf': None})
            
            games_data.append({
                'season': game.season,
                'week': game.week,
                'winner': winner_canonical['name'],
                'loser': loser_canonical['name'],
                'winner_score': winner_score,
                'loser_score': loser_score,
                'margin': abs(winner_score - loser_score),
                'venue': venue,
                'neutral_site': game.neutral_site,
                'season_type': game.season_type,
                'winner_conference': winner_canonical['conf'],
                'loser_conference': loser_canonical['conf'],
                'points_winner': winner_score,
                'points_loser': loser_score,
                'winner_home': venue == 'home',
                'cross_conf': (winner_canonical['conf'] != loser_canonical['conf'] 
                              and winner_canonical['conf'] is not None 
                              and loser_canonical['conf'] is not None),
                'date': None,
                'is_bowl': game.season_type == 'postseason'
            })
        
        games_df = pd.DataFrame(games_data)
        
        # 6. Generate season summary
        if week is None:  # Complete season
            self.generate_season_summary(games_df, season)
        
        self.logger.info("Data validation suite completed successfully")
        return games_df

def validate_dataset(games: List[Dict], canonical_teams: Dict, season: int, 
                    week: Optional[int] = None, config: Dict = None) -> pd.DataFrame:
    """Convenience function for complete data validation"""
    if config is None:
        config = {}
    
    validator = DataValidator(config)
    return validator.validate_complete_dataset(games, canonical_teams, season, week)