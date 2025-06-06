"""
Comprehensive Data Quality Validator
Implements fail-fast checks for FBS team counts, conference assignments, 
rating distributions, and game completeness to ensure bias-free rankings
"""

import logging
from typing import Dict, List, Set, Tuple, Optional
import pandas as pd
from collections import Counter

class DataQualityValidator:
    """Comprehensive data quality validation for FBS-only rankings"""

    def __init__(self, config: Dict = None):
        self.config = config or {}
        self.logger = logging.getLogger(__name__)

        # Expected values for validation
        self.expected_fbs_count = 134  # 2024 standard
        self.expected_min_games_per_team = 8  # Minimum games per FBS team
        self.expected_total_games = 798  # 2024 FBS-only total (752 regular + 46 bowls)
        self.expected_rating_range = (0.008, 0.015)  # Top ratings in FBS-only system

    def validate_fbs_team_count(self, teams: List[Dict], season: int) -> Dict:
        """Assert FBS team count matches expectation"""
        team_count = len(teams)

        validation_result = {
            'check_name': 'fbs_team_count',
            'season': season,
            'actual_count': team_count,
            'expected_count': self.expected_fbs_count,
            'validation_passed': team_count == self.expected_fbs_count,
            'severity': 'CRITICAL' if team_count != self.expected_fbs_count else 'PASS'
        }

        if not validation_result['validation_passed']:
            self.logger.error(f"FBS team count mismatch: {team_count} != {self.expected_fbs_count}")
            validation_result['error_details'] = f"Expected {self.expected_fbs_count} FBS teams, got {team_count}"
        else:
            self.logger.info(f"✓ FBS team count validation passed: {team_count} teams")

        return validation_result

    def validate_conference_assignments(self, teams: List[Dict], season: int) -> Dict:
        """Assert no team has conference == None or "Unknown" """
        teams_without_conference = []
        unknown_conference_teams = []
        conference_counts = Counter()

        for team in teams:
            team_name = team.get('school', 'Unknown Team')
            conference = team.get('conference')

            if conference is None or pd.isna(conference):
                teams_without_conference.append(team_name)
            elif conference == 'Unknown':
                unknown_conference_teams.append(team_name)
            else:
                conference_counts[conference] += 1

        validation_result = {
            'check_name': 'conference_assignments',
            'season': season,
            'teams_without_conference': teams_without_conference,
            'unknown_conference_teams': unknown_conference_teams,
            'conference_distribution': dict(conference_counts),
            'validation_passed': len(teams_without_conference) == 0 and len(unknown_conference_teams) == 0,
            'severity': 'CRITICAL' if teams_without_conference or unknown_conference_teams else 'PASS'
        }

        if teams_without_conference:
            self.logger.error(f"Teams without conference assignment: {teams_without_conference}")
        if unknown_conference_teams:
            self.logger.error(f"Teams with 'Unknown' conference: {unknown_conference_teams}")

        if validation_result['validation_passed']:
            self.logger.info(f"✓ Conference assignment validation passed: {len(conference_counts)} conferences")

        return validation_result

    def validate_rating_distribution(self, team_ratings: Dict[str, float], season: int) -> Dict:
        """Check rating range and distribution for FBS-only system"""
        if not team_ratings:
            return {
                'check_name': 'rating_distribution',
                'validation_passed': False,
                'severity': 'CRITICAL',
                'error_details': 'No team ratings provided'
            }

        ratings_list = list(team_ratings.values())
        top_rating = max(ratings_list)
        bottom_rating = min(ratings_list)
        median_rating = sorted(ratings_list)[len(ratings_list) // 2]

        # Count teams above different thresholds
        teams_above_008 = sum(1 for r in ratings_list if r > 0.008)
        teams_above_010 = sum(1 for r in ratings_list if r > 0.010)

        # Validation checks
        top_rating_valid = self.expected_rating_range[0] <= top_rating <= self.expected_rating_range[1]
        distribution_healthy = teams_above_008 >= 20  # Expect multiple strong teams
        range_appropriate = (top_rating - bottom_rating) > 0.003  # Sufficient spread

        validation_result = {
            'check_name': 'rating_distribution',
            'season': season,
            'top_rating': top_rating,
            'bottom_rating': bottom_rating,
            'median_rating': median_rating,
            'rating_range': top_rating - bottom_rating,
            'teams_above_008': teams_above_008,
            'teams_above_010': teams_above_010,
            'expected_top_range': self.expected_rating_range,
            'top_rating_valid': top_rating_valid,
            'distribution_healthy': distribution_healthy,
            'range_appropriate': range_appropriate,
            'validation_passed': top_rating_valid and distribution_healthy and range_appropriate,
            'severity': 'WARNING' if not (top_rating_valid and distribution_healthy and range_appropriate) else 'PASS'
        }

        if not top_rating_valid:
            self.logger.warning(f"Top rating {top_rating:.6f} outside expected range {self.expected_rating_range}")
        if not distribution_healthy:
            self.logger.warning(f"Only {teams_above_008} teams above 0.008 threshold (expected >= 20)")
        if not range_appropriate:
            self.logger.warning(f"Rating range {top_rating - bottom_rating:.6f} may be too compressed")

        if validation_result['validation_passed']:
            self.logger.info(f"✓ Rating distribution validation passed: top={top_rating:.6f}, spread={top_rating - bottom_rating:.6f}")

        return validation_result

    def validate_game_completeness(self, games_df: pd.DataFrame, teams: List[Dict], season: int) -> Dict:
        """Ensure each FBS team has appropriate number of games"""
        team_names = {team['school'] for team in teams}

        # Count games per team
        game_counts = {}

        for team in team_names:
            winner_count = len(games_df[games_df['winner'] == team])
            loser_count = len(games_df[games_df['loser'] == team])
            game_counts[team] = winner_count + loser_count

        # Determine expected minimum based on dataset size
        total_games = len(games_df)
        if total_games < 100:  # Partial season data (week data)
            expected_min = 0  # Allow teams with 0 games in partial data
            critical_threshold = 0  # Only fail if systematic issues
        else:  # Full season data
            expected_min = self.expected_min_games_per_team
            critical_threshold = 5  # More than 5 teams with insufficient games

        # Find teams with insufficient games
        teams_with_few_games = {team: count for team, count in game_counts.items() 
                               if count < expected_min}

        # Find teams not in any games (only critical for full season)
        teams_without_games = team_names - set(game_counts.keys())
        if total_games < 100:
            teams_without_games = set()  # Don't flag for partial data

        validation_result = {
            'check_name': 'game_completeness',
            'season': season,
            'total_games': len(games_df),
            'expected_total_games': self.expected_total_games,
            'teams_with_few_games': teams_with_few_games,
            'teams_without_games': list(teams_without_games),
            'min_games_per_team': min(game_counts.values()) if game_counts else 0,
            'max_games_per_team': max(game_counts.values()) if game_counts else 0,
            'avg_games_per_team': sum(game_counts.values()) / len(game_counts) if game_counts else 0,
            'dataset_type': 'partial' if total_games < 100 else 'full_season',
            'validation_passed': len(teams_without_games) == 0 and len(teams_with_few_games) <= critical_threshold,
            'severity': 'CRITICAL' if teams_without_games or len(teams_with_few_games) > critical_threshold else 'WARNING' if teams_with_few_games else 'PASS'
        }

        if teams_without_games:
            self.logger.error(f"Teams without any games: {list(teams_without_games)}")
        elif teams_with_few_games and total_games >= 100:
            self.logger.warning(f"Teams with < {expected_min} games: {len(teams_with_few_games)} teams")
        elif total_games < 100:
            self.logger.info(f"Partial dataset validation: {total_games} games, max per team: {max(game_counts.values()) if game_counts else 0}")

        if validation_result['validation_passed']:
            self.logger.info(f"✓ Game completeness validation passed: dataset type = {validation_result['dataset_type']}")

        return validation_result

    def validate_conference_strength_anomalies(self, team_ratings: Dict[str, float], 
                                             teams: List[Dict], season: int) -> Dict:
        """Check for conference strength anomalies that could indicate data issues"""
        # Group teams by conference
        conf_teams = {}
        for team in teams:
            conf = team.get('conference', 'Unknown')
            if conf not in conf_teams:
                conf_teams[conf] = []
            conf_teams[conf].append(team['school'])

        # Calculate conference averages
        conf_averages = {}
        conf_top_teams = {}

        for conf, team_list in conf_teams.items():
            ratings = [team_ratings.get(team, 0) for team in team_list if team in team_ratings]
            if ratings:
                conf_averages[conf] = sum(ratings) / len(ratings)
                conf_top_teams[conf] = max(ratings)

        # Identify potential anomalies
        power5_conferences = {'SEC', 'Big Ten', 'Big 12', 'ACC', 'Pac-12'}
        power5_in_data = {conf for conf in conf_averages.keys() if conf in power5_conferences}

        # Check for reasonable Power 5 representation
        power5_avg = sum(conf_averages[conf] for conf in power5_in_data) / len(power5_in_data) if power5_in_data else 0
        g5_conferences = {conf for conf in conf_averages.keys() if conf not in power5_conferences and conf != 'Unknown'}
        g5_avg = sum(conf_averages[conf] for conf in g5_conferences) / len(g5_conferences) if g5_conferences else 0

        strength_gap_appropriate = (power5_avg - g5_avg) > 0.0005 if power5_avg > 0 and g5_avg > 0 else True

        validation_result = {
            'check_name': 'conference_strength_anomalies',
            'season': season,
            'conference_averages': conf_averages,
            'conference_top_ratings': conf_top_teams,
            'power5_conferences_found': list(power5_in_data),
            'power5_average': power5_avg,
            'g5_average': g5_avg,
            'strength_gap': power5_avg - g5_avg if power5_avg > 0 and g5_avg > 0 else 0,
            'strength_gap_appropriate': strength_gap_appropriate,
            'validation_passed': len(power5_in_data) >= 4 and strength_gap_appropriate,
            'severity': 'WARNING' if not (len(power5_in_data) >= 4 and strength_gap_appropriate) else 'PASS'
        }

        if len(power5_in_data) < 4:
            self.logger.warning(f"Only {len(power_in_data)} Power 5 conferences found: {list(power5_in_data)}")
        if not strength_gap_appropriate:
            self.logger.warning(f"Conference strength gap may be too small: P5={power5_avg:.6f}, G5={g5_avg:.6f}")

        if validation_result['validation_passed']:
            self.logger.info(f"✓ Conference strength validation passed: P5 gap = {power5_avg - g5_avg:.6f}")

        return validation_result

    def run_comprehensive_validation(self, teams: List[Dict], games_df: pd.DataFrame, 
                                   team_ratings: Dict[str, float], season: int) -> Dict:
        """Run all validation checks and return comprehensive report"""
        validation_results = []

        # Run all validation checks
        validation_results.append(self.validate_fbs_team_count(teams, season))
        validation_results.append(self.validate_conference_assignments(teams, season))
        validation_results.append(self.validate_rating_distribution(team_ratings, season))
        validation_results.append(self.validate_game_completeness(games_df, teams, season))
        validation_results.append(self.validate_conference_strength_anomalies(team_ratings, teams, season))

        # Aggregate results
        critical_failures = [r for r in validation_results if r['severity'] == 'CRITICAL']
        warnings = [r for r in validation_results if r['severity'] == 'WARNING']
        passes = [r for r in validation_results if r['severity'] == 'PASS']

        overall_passed = len(critical_failures) == 0

        comprehensive_report = {
            'season': season,
            'validation_timestamp': pd.Timestamp.now().isoformat(),
            'overall_validation_passed': overall_passed,
            'validation_summary': {
                'total_checks': len(validation_results),
                'critical_failures': len(critical_failures),
                'warnings': len(warnings),
                'passes': len(passes)
            },
            'individual_results': validation_results,
            'critical_issues': [r['check_name'] for r in critical_failures],
            'warning_issues': [r['check_name'] for r in warnings]
        }

        # Log summary
        if overall_passed:
            self.logger.info(f"✓ Comprehensive data quality validation PASSED ({len(passes)}/{len(validation_results)} checks)")
        else:
            self.logger.error(f"✗ Comprehensive data quality validation FAILED ({len(critical_failures)} critical issues)")

        return comprehensive_report

def create_data_quality_validator(config: Dict = None) -> DataQualityValidator:
    """Factory function for data quality validator"""
    return DataQualityValidator(config)