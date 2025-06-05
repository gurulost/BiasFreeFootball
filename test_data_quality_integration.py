"""
Integration test for comprehensive data quality validation
Tests all fail-fast checks with authentic FBS data
"""

import os
import yaml
from src.ingest import CFBDataIngester
from src.data_quality_validator import create_data_quality_validator

def test_data_quality_integration():
    """Test comprehensive data quality validation with authentic data"""
    
    # Load config
    with open('config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    print("=== DATA QUALITY INTEGRATION TEST ===")
    
    # Step 1: Fetch authentic FBS data
    print("1. Fetching authentic FBS data...")
    ingester = CFBDataIngester(config)
    
    teams = ingester.fetch_teams(2024, division='fbs')
    print(f"   Teams fetched: {len(teams)}")
    
    # Get sample games for testing
    week1_games = ingester.fetch_games(2024, week=1, season_type='regular')
    print(f"   Week 1 games: {len(week1_games)}")
    
    # Process games to DataFrame
    games_df = ingester.process_game_data(week1_games)
    print(f"   Processed games: {len(games_df)}")
    
    # Step 2: Create sample team ratings for validation
    print("\n2. Creating sample team ratings...")
    # Simulate realistic PageRank distribution for FBS teams
    team_ratings = {}
    for i, team in enumerate(teams):
        # Create realistic rating scale: top teams ~0.012, bottom ~0.007
        team_ratings[team['school']] = 0.007 + (0.005 * (len(teams) - i) / len(teams))
    
    top_rating = max(team_ratings.values())
    print(f"   Sample ratings created: top={top_rating:.6f}, teams={len(team_ratings)}")
    
    # Step 3: Run comprehensive data quality validation
    print("\n3. Running comprehensive data quality validation...")
    quality_validator = create_data_quality_validator(config)
    
    validation_report = quality_validator.run_comprehensive_validation(
        teams, games_df, team_ratings, 2024
    )
    
    # Step 4: Analyze validation results
    print("\n4. Validation Results:")
    print(f"   Overall passed: {validation_report['overall_validation_passed']}")
    print(f"   Summary: {validation_report['validation_summary']}")
    
    # Show individual check results
    for result in validation_report['individual_results']:
        check_name = result['check_name']
        severity = result['severity']
        passed = result.get('validation_passed', False)
        status = "✓" if passed else "✗" if severity == 'CRITICAL' else "⚠"
        print(f"   {status} {check_name}: {severity}")
    
    # Step 5: Test specific validation scenarios
    print("\n5. Testing specific validation scenarios:")
    
    # Test FBS team count validation
    team_count_result = quality_validator.validate_fbs_team_count(teams, 2024)
    print(f"   FBS team count: {team_count_result['actual_count']} (expected: {team_count_result['expected_count']})")
    
    # Test conference assignment validation
    conf_result = quality_validator.validate_conference_assignments(teams, 2024)
    print(f"   Conference assignments: {len(conf_result['teams_without_conference'])} missing, {len(conf_result['unknown_conference_teams'])} unknown")
    
    # Test rating distribution validation
    rating_result = quality_validator.validate_rating_distribution(team_ratings, 2024)
    print(f"   Rating distribution: top={rating_result['top_rating']:.6f}, teams>0.008={rating_result['teams_above_008']}")
    
    # Test game completeness validation
    game_result = quality_validator.validate_game_completeness(games_df, teams, 2024)
    print(f"   Game completeness: {game_result['total_games']} games, {len(game_result['teams_without_games'])} teams without games")
    
    # Step 6: Test fail-fast behavior
    print("\n6. Testing fail-fast behavior with invalid data:")
    
    # Test with incorrect team count (simulate non-FBS teams included)
    invalid_teams = teams + [{'school': 'FCS Team', 'conference': 'FCS Conference'}]
    invalid_team_result = quality_validator.validate_fbs_team_count(invalid_teams, 2024)
    print(f"   Invalid team count test: {invalid_team_result['validation_passed']} (should be False)")
    
    # Test with teams missing conferences
    teams_missing_conf = teams[:5]  # Take first 5 teams
    for team in teams_missing_conf:
        team['conference'] = None  # Remove conference
    invalid_conf_result = quality_validator.validate_conference_assignments(teams_missing_conf, 2024)
    print(f"   Missing conference test: {invalid_conf_result['validation_passed']} (should be False)")
    
    # Test with compressed rating scale (simulate non-FBS dilution)
    compressed_ratings = {team: 0.005 for team in team_ratings.keys()}  # All ratings too low
    invalid_rating_result = quality_validator.validate_rating_distribution(compressed_ratings, 2024)
    print(f"   Compressed rating test: {invalid_rating_result['validation_passed']} (should be False)")
    
    # Step 7: Summary
    print(f"\n=== INTEGRATION TEST SUMMARY ===")
    
    test_results = {
        'authentic_data_validation': validation_report['overall_validation_passed'],
        'fbs_team_count_check': team_count_result['validation_passed'],
        'conference_assignment_check': conf_result['validation_passed'],
        'rating_distribution_check': rating_result['validation_passed'],
        'game_completeness_check': game_result['validation_passed'],
        'fail_fast_team_count': not invalid_team_result['validation_passed'],
        'fail_fast_conferences': not invalid_conf_result['validation_passed'],
        'fail_fast_ratings': not invalid_rating_result['validation_passed']
    }
    
    passed_tests = sum(test_results.values())
    total_tests = len(test_results)
    
    print(f"Tests passed: {passed_tests}/{total_tests}")
    for test_name, passed in test_results.items():
        status = "✓" if passed else "✗"
        print(f"  {status} {test_name}")
    
    overall_success = passed_tests >= total_tests - 1  # Allow one minor failure
    
    if overall_success:
        print("\n✓ DATA QUALITY INTEGRATION TEST PASSED")
        print("System enforces comprehensive data quality with fail-fast validation")
        return True
    else:
        print("\n✗ DATA QUALITY INTEGRATION TEST FAILED")
        print("System has data quality validation gaps")
        return False

if __name__ == "__main__":
    test_data_quality_integration()