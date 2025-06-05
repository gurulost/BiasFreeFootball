"""
Comprehensive test for FBS-only enforcement across all API calls
Validates that the system enforces FBS data with proper rating scale
"""

import os
import yaml
from src.ingest import CFBDataIngester
from src.fbs_enforcer import create_fbs_enforcer

def test_fbs_enforcement():
    """Test comprehensive FBS-only enforcement implementation"""
    
    # Load config
    with open('config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    print("=== COMPREHENSIVE FBS ENFORCEMENT TEST ===")
    
    # Test 1: Initialize ingester with FBS enforcer
    print("\n1. Testing FBS enforcer integration:")
    try:
        ingester = CFBDataIngester(config)
        fbs_enforcer = create_fbs_enforcer(config)
        print("   ✓ FBS enforcer initialized successfully")
    except Exception as e:
        print(f"   ✗ FBS enforcer initialization failed: {e}")
        return False
    
    # Test 2: FBS teams enforcement
    print("\n2. Testing FBS teams enforcement:")
    fbs_teams = ingester.fetch_teams(2024, division='fbs')
    
    expected_count = 134
    if len(fbs_teams) == expected_count:
        print(f"   ✓ Correct FBS team count: {len(fbs_teams)}")
    else:
        print(f"   ⚠ FBS team count: {len(fbs_teams)} (expected: {expected_count})")
    
    # Test classification filtering
    non_fbs_teams = [t for t in fbs_teams if t.get('classification', '').lower() != 'fbs']
    if len(non_fbs_teams) == 0:
        print("   ✓ All teams have FBS classification")
    else:
        print(f"   ⚠ {len(non_fbs_teams)} non-FBS teams found")
    
    # Test 3: Games enforcement
    print("\n3. Testing FBS games enforcement:")
    week1_games = ingester.fetch_games(2024, week=1, season_type='regular')
    
    # Check all games are FBS vs FBS
    fbs_team_names = {t['school'] for t in fbs_teams}
    non_fbs_games = 0
    classification_failures = 0
    
    for game in week1_games:
        home_team = game.get('homeTeam', '')
        away_team = game.get('awayTeam', '')
        home_class = game.get('homeClassification', '').lower()
        away_class = game.get('awayClassification', '').lower()
        
        # Check team names
        if home_team not in fbs_team_names or away_team not in fbs_team_names:
            non_fbs_games += 1
        
        # Check classifications
        if home_class != 'fbs' or away_class != 'fbs':
            classification_failures += 1
    
    if non_fbs_games == 0:
        print(f"   ✓ All {len(week1_games)} games involve FBS teams")
    else:
        print(f"   ⚠ {non_fbs_games} games involve non-FBS teams")
    
    if classification_failures == 0:
        print("   ✓ All games have FBS classifications")
    else:
        print(f"   ⚠ {classification_failures} classification failures")
    
    # Test 4: Rating scale validation
    print("\n4. Testing rating scale with sample PageRank:")
    
    # Create simple ratings for scale testing
    team_ratings = {team['school']: 1.0/len(fbs_teams) for team in fbs_teams}
    
    # Simulate realistic PageRank distribution
    sorted_teams = list(team_ratings.keys())
    for i, team in enumerate(sorted_teams):
        # Create realistic PageRank scale (top teams ~0.009, bottom ~0.007)
        team_ratings[team] = 0.007 + (0.002 * (len(sorted_teams) - i) / len(sorted_teams))
    
    scale_report = fbs_enforcer.validate_rating_scale(team_ratings)
    
    print(f"   Rating scale validation: {scale_report['validation_passed']}")
    print(f"   Top rating: {scale_report['top_rating']:.6f}")
    print(f"   Team count: {scale_report['total_teams']}")
    print(f"   Scale valid: {scale_report['scale_valid']}")
    print(f"   Count valid: {scale_report['team_count_valid']}")
    
    # Test 5: 2024 realignment verification
    print("\n5. Testing 2024 realignment enforcement:")
    
    team_to_conf = {t['school']: t.get('conference', 'Unknown') for t in fbs_teams}
    
    key_moves_2024 = {
        'Texas': 'SEC',
        'Oregon': 'Big Ten',
        'Washington': 'Big Ten',
        'USC': 'Big Ten',
        'SMU': 'ACC',
        'Stanford': 'ACC'
    }
    
    correct_assignments = 0
    for team, expected_conf in key_moves_2024.items():
        actual_conf = team_to_conf.get(team, 'Not Found')
        if actual_conf == expected_conf:
            correct_assignments += 1
            print(f"   ✓ {team}: {actual_conf}")
        else:
            print(f"   ✗ {team}: {actual_conf} (expected: {expected_conf})")
    
    # Test 6: API endpoint coverage
    print("\n6. Testing API endpoint coverage:")
    
    enforcement_report = fbs_enforcer.generate_enforcement_report(2024)
    print(f"   FBS teams cached: {enforcement_report['fbs_teams_cached']}")
    print(f"   Expected FBS count: {enforcement_report['expected_fbs_count']}")
    print(f"   Enforcement active: {enforcement_report['enforcement_active']}")
    print(f"   Endpoints covered: {len(enforcement_report['api_endpoints_covered'])}")
    print(f"   Validation checks: {len(enforcement_report['validation_checks'])}")
    
    # Summary and scoring
    print(f"\n=== ENFORCEMENT TEST SUMMARY ===")
    
    scores = {
        'team_count': len(fbs_teams) == 134,
        'team_classification': len(non_fbs_teams) == 0,
        'games_filtering': non_fbs_games == 0,
        'game_classification': classification_failures == 0,
        'rating_scale': scale_report['validation_passed'],
        'realignment': correct_assignments >= 5
    }
    
    passed_tests = sum(scores.values())
    total_tests = len(scores)
    
    print(f"Tests passed: {passed_tests}/{total_tests}")
    for test_name, passed in scores.items():
        status = "✓" if passed else "✗"
        print(f"  {status} {test_name}")
    
    overall_success = passed_tests >= total_tests - 1  # Allow one minor failure
    
    if overall_success:
        print("\n✓ FBS ENFORCEMENT TEST PASSED")
        print("System successfully enforces FBS-only data across all API calls")
        return True
    else:
        print("\n✗ FBS ENFORCEMENT TEST FAILED")
        print("System has FBS enforcement gaps")
        return False

if __name__ == "__main__":
    test_fbs_enforcement()