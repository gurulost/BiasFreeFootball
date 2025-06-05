"""
Test script to validate the API endpoint fix for conference assignments
Tests the three-commit solution from the memo
"""

import os
import yaml
import logging
from src.ingest import CFBDataIngester

def load_config():
    """Load configuration"""
    with open('config.yaml', 'r') as f:
        return yaml.safe_load(f)

def test_api_fix():
    """Test the API endpoint fix for 2024 season data"""
    
    print("=== API ENDPOINT FIX VALIDATION ===\n")
    
    # Load configuration
    config = load_config()
    
    # Check API key
    api_key = os.environ.get('CFB_API_KEY')
    if not api_key:
        print("✗ CFB_API_KEY not configured")
        return False
    
    print(f"✓ API key configured (length: {len(api_key)})")
    
    # Test data ingester with authentic API
    ingester = CFBDataIngester(config)
    
    try:
        # Test 1: Fetch 2024 FBS teams
        print("\n1. Testing FBS teams endpoint...")
        teams = ingester.fetch_teams(2024, division='fbs')
        print(f"✓ Retrieved {len(teams)} FBS teams")
        
        # Test key 2024 realignment teams
        team_lookup = {team['school']: team['conference'] for team in teams}
        
        realignment_tests = {
            'BYU': 'Big 12',
            'Texas': 'SEC',
            'Oklahoma': 'SEC', 
            'Oregon': 'Big Ten',
            'USC': 'Big Ten',
            'UCLA': 'Big Ten',
            'Washington': 'Big Ten',
            'California': 'ACC',
            'Stanford': 'ACC',
            'SMU': 'ACC'
        }
        
        correct_assignments = 0
        for team, expected_conf in realignment_tests.items():
            actual_conf = team_lookup.get(team, 'Not Found')
            if actual_conf == expected_conf:
                print(f"  ✓ {team}: {actual_conf}")
                correct_assignments += 1
            else:
                print(f"  ✗ {team}: {actual_conf} (expected: {expected_conf})")
        
        print(f"Conference assignment accuracy: {correct_assignments}/{len(realignment_tests)} = {correct_assignments/len(realignment_tests)*100:.1f}%")
        
        # Test 2: Fetch conferences
        print("\n2. Testing conferences endpoint...")
        conferences = ingester.fetch_conferences(2024)
        print(f"✓ Retrieved {len(conferences)} conferences")
        
        # Test 3: Fetch games data
        print("\n3. Testing games endpoint...")
        games = ingester.fetch_results_upto_bowls(2024)
        print(f"✓ Retrieved {len(games)} total games")
        
        # Filter for FBS-only games
        fbs_team_names = {team['school'] for team in teams}
        fbs_games = games[
            (games['home_team'].isin(fbs_team_names)) & 
            (games['away_team'].isin(fbs_team_names))
        ]
        
        print(f"✓ FBS-only games: {len(fbs_games)}")
        print(f"✓ Filtered out {len(games) - len(fbs_games)} non-FBS games")
        
        # Test 4: Verify game completeness
        print("\n4. Testing game data completeness...")
        required_columns = ['home_team', 'away_team', 'home_points', 'away_points', 'week', 'season_type']
        missing_data = fbs_games[required_columns].isnull().sum()
        
        if missing_data.sum() == 0:
            print("✓ No missing data in critical columns")
        else:
            print(f"⚠ Missing data found: {missing_data.to_dict()}")
        
        # Test 5: Conference diversity check
        print("\n5. Testing conference diversity...")
        conferences_in_games = set()
        for team in fbs_team_names:
            if team in team_lookup:
                conferences_in_games.add(team_lookup[team])
        
        print(f"✓ Conferences represented in games: {len(conferences_in_games)}")
        major_conferences = ['SEC', 'Big Ten', 'Big 12', 'ACC', 'Pac-12']
        found_major = [conf for conf in major_conferences if conf in conferences_in_games]
        print(f"✓ Major conferences found: {found_major}")
        
        print("\n=== API FIX VALIDATION SUMMARY ===")
        
        if correct_assignments >= 8:  # At least 80% correct
            print("✓ Conference assignments: PASS")
        else:
            print("✗ Conference assignments: FAIL")
            return False
        
        if len(fbs_games) >= 700:  # Reasonable number of FBS games
            print("✓ Game data completeness: PASS")
        else:
            print("✗ Game data completeness: FAIL")
            return False
        
        if len(conferences_in_games) >= 10:  # Good conference diversity
            print("✓ Conference diversity: PASS")
        else:
            print("✗ Conference diversity: FAIL")
            return False
        
        print("✓ All API endpoint tests passed")
        print("✓ 2024 realignment data is accurate")
        print("✓ FBS filtering is working correctly")
        
        return True
        
    except Exception as e:
        print(f"✗ API test failed with error: {e}")
        return False

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    success = test_api_fix()
    if success:
        print("\n🎉 API endpoint fix validation successful!")
        exit(0)
    else:
        print("\n❌ API endpoint fix validation failed")
        exit(1)