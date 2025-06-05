"""
Season-Specific Team Validation Module
Ensures team-conference mappings are accurate for each season and validates game data consistency
"""

import logging
from typing import Dict, List, Set, Optional, Tuple
import pandas as pd

class SeasonValidator:
    """Validates season-specific team data and cross-verifies with games"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.logger = logging.getLogger(__name__)
        
    def validate_season_teams(self, teams: List[Dict], season: int) -> Tuple[Dict[str, str], Set[str]]:
        """
        Validate season-specific team data and build authoritative mappings
        
        Returns:
            (team_to_conference_map, fbs_team_names)
        """
        if len(teams) != 134:
            self.logger.warning(f"Expected 134 FBS teams for {season}, got {len(teams)}")
        
        team_to_conference = {}
        fbs_team_names = set()
        missing_conferences = []
        
        for team in teams:
            team_name = team.get('school')
            conference = team.get('conference')
            
            if not team_name:
                self.logger.error(f"Team missing school name: {team}")
                continue
                
            fbs_team_names.add(team_name)
            
            if conference:
                team_to_conference[team_name] = conference
            else:
                missing_conferences.append(team_name)
                team_to_conference[team_name] = 'Unknown'
        
        if missing_conferences:
            self.logger.warning(f"Teams with missing conferences: {missing_conferences}")
        
        self.logger.info(f"Season {season} validation: {len(fbs_team_names)} FBS teams, {len(team_to_conference)} with conferences")
        return team_to_conference, fbs_team_names
    
    def cross_verify_games_with_teams(self, games_df: pd.DataFrame, 
                                    team_to_conference: Dict[str, str],
                                    fbs_team_names: Set[str],
                                    season: int) -> pd.DataFrame:
        """
        Cross-verify game data against authoritative team mappings
        Fix or flag mismatches
        """
        self.logger.info(f"Cross-verifying {len(games_df)} games against {len(fbs_team_names)} FBS teams")
        
        # Track validation issues
        invalid_teams = set()
        conference_mismatches = []
        corrected_games = 0
        
        # Validate each game
        for idx, game in games_df.iterrows():
            home_team = game.get('home_team')
            away_team = game.get('away_team')
            
            # Check if teams are in FBS list
            if home_team not in fbs_team_names:
                invalid_teams.add(home_team)
                continue
                
            if away_team not in fbs_team_names:
                invalid_teams.add(away_team)
                continue
            
            # Get authoritative conference assignments
            home_conf_auth = team_to_conference.get(home_team, 'Unknown')
            away_conf_auth = team_to_conference.get(away_team, 'Unknown')
            
            # Check and correct conference assignments in game data
            home_conf_game = game.get('home_conference', 'Unknown')
            away_conf_game = game.get('away_conference', 'Unknown')
            
            # Fix mismatches using authoritative data
            if home_conf_game != home_conf_auth:
                if home_conf_game != 'Unknown':
                    conference_mismatches.append({
                        'team': home_team,
                        'game_conf': home_conf_game,
                        'auth_conf': home_conf_auth,
                        'game_id': f"{home_team} vs {away_team}"
                    })
                games_df.at[idx, 'home_conference'] = home_conf_auth
                corrected_games += 1
            
            if away_conf_game != away_conf_auth:
                if away_conf_game != 'Unknown':
                    conference_mismatches.append({
                        'team': away_team,
                        'game_conf': away_conf_game,
                        'auth_conf': away_conf_auth,
                        'game_id': f"{home_team} vs {away_team}"
                    })
                games_df.at[idx, 'away_conference'] = away_conf_auth
                corrected_games += 1
        
        # Filter out games with invalid teams (non-FBS)
        initial_count = len(games_df)
        games_df = games_df[
            games_df['winner'].isin(fbs_team_names) & 
            games_df['loser'].isin(fbs_team_names)
        ].copy()
        
        filtered_count = initial_count - len(games_df)
        
        # Log validation results
        if invalid_teams:
            self.logger.warning(f"Filtered {len(invalid_teams)} non-FBS teams: {sorted(list(invalid_teams))[:10]}...")
        
        if conference_mismatches:
            self.logger.info(f"Fixed {len(conference_mismatches)} conference mismatches")
            for mismatch in conference_mismatches[:5]:  # Show first 5
                self.logger.debug(f"  {mismatch['team']}: {mismatch['game_conf']} → {mismatch['auth_conf']}")
        
        self.logger.info(f"Validation complete: {corrected_games} games corrected, {filtered_count} games removed")
        
        return games_df
    
    def validate_conference_consistency(self, games_df: pd.DataFrame, season: int) -> Dict:
        """
        Validate that teams don't have mixed conference assignments within a season
        """
        team_conferences = {}
        mixed_assignments = {}
        
        for _, game in games_df.iterrows():
            home_team = game.get('home_team')
            away_team = game.get('away_team')
            home_conf = game.get('home_conference')
            away_conf = game.get('away_conference')
            
            # Track conference assignments for each team
            for team, conf in [(home_team, home_conf), (away_team, away_conf)]:
                if team and conf and conf != 'Unknown':
                    if team in team_conferences:
                        if team_conferences[team] != conf:
                            if team not in mixed_assignments:
                                mixed_assignments[team] = set()
                            mixed_assignments[team].add(conf)
                            mixed_assignments[team].add(team_conferences[team])
                    else:
                        team_conferences[team] = conf
        
        # Log mixed assignments (should be rare/zero for single season)
        if mixed_assignments:
            self.logger.error(f"Teams with mixed conference assignments in {season}:")
            for team, confs in mixed_assignments.items():
                self.logger.error(f"  {team}: {sorted(list(confs))}")
        else:
            self.logger.info(f"Conference consistency check passed: no mixed assignments in {season}")
        
        return {
            'team_conferences': team_conferences,
            'mixed_assignments': mixed_assignments,
            'total_teams': len(team_conferences)
        }
    
    def validate_2024_realignment(self, team_to_conference: Dict[str, str]) -> bool:
        """
        Validate specific 2024 conference realignment moves
        """
        expected_2024_moves = {
            'Texas': 'SEC',
            'Oklahoma': 'SEC', 
            'Oregon': 'Big Ten',
            'Washington': 'Big Ten',
            'USC': 'Big Ten',
            'UCLA': 'Big Ten',
            'SMU': 'ACC',
            'Stanford': 'ACC',
            'California': 'ACC',
            'BYU': 'Big 12'  # Joined Big 12 in 2023, should be consistent in 2024
        }
        
        correct_assignments = 0
        total_checks = len(expected_2024_moves)
        
        self.logger.info("Validating 2024 conference realignment:")
        
        for team, expected_conf in expected_2024_moves.items():
            actual_conf = team_to_conference.get(team, 'Not Found')
            if actual_conf == expected_conf:
                correct_assignments += 1
                self.logger.info(f"  ✓ {team}: {actual_conf}")
            else:
                self.logger.error(f"  ✗ {team}: {actual_conf} (expected: {expected_conf})")
        
        success_rate = correct_assignments / total_checks
        self.logger.info(f"2024 realignment validation: {correct_assignments}/{total_checks} correct ({success_rate:.1%})")
        
        return success_rate >= 0.9  # Allow for minor data issues
    
    def generate_validation_report(self, season: int, team_to_conference: Dict[str, str],
                                 games_df: pd.DataFrame) -> Dict:
        """
        Generate comprehensive validation report
        """
        # Conference distribution
        conf_counts = {}
        for conf in team_to_conference.values():
            conf_counts[conf] = conf_counts.get(conf, 0) + 1
        
        # Game statistics
        total_games = len(games_df)
        regular_games = len(games_df[games_df['season_type'] == 'regular'])
        bowl_games = len(games_df[games_df['season_type'] == 'postseason'])
        
        report = {
            'season': season,
            'teams': {
                'total_fbs': len(team_to_conference),
                'expected_fbs': 134,
                'conference_distribution': conf_counts
            },
            'games': {
                'total': total_games,
                'regular_season': regular_games,
                'postseason': bowl_games,
                'expected_total': 798  # 2024 target
            },
            'validation_passed': (
                len(team_to_conference) == 134 and
                total_games >= 790 and
                'Unknown' not in conf_counts or conf_counts['Unknown'] < 5
            )
        }
        
        return report

def validate_season_data(teams: List[Dict], games_df: pd.DataFrame, 
                        season: int, config: Dict = None) -> Tuple[pd.DataFrame, Dict]:
    """
    Convenience function for complete season validation
    
    Returns:
        (validated_games_df, validation_report)
    """
    validator = SeasonValidator(config or {})
    
    # Step 1: Validate teams and build mappings
    team_to_conference, fbs_team_names = validator.validate_season_teams(teams, season)
    
    # Step 2: Cross-verify games against teams
    validated_games = validator.cross_verify_games_with_teams(
        games_df, team_to_conference, fbs_team_names, season
    )
    
    # Step 3: Check conference consistency
    consistency_report = validator.validate_conference_consistency(validated_games, season)
    
    # Step 4: Validate 2024 realignment (if applicable)
    realignment_valid = True
    if season == 2024:
        realignment_valid = validator.validate_2024_realignment(team_to_conference)
    
    # Step 5: Generate report
    validation_report = validator.generate_validation_report(season, team_to_conference, validated_games)
    validation_report['consistency'] = consistency_report
    validation_report['realignment_valid'] = realignment_valid
    
    return validated_games, validation_report