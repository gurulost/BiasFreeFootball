"""
Test the new CFBD client integration with comprehensive validation
"""

import os
import sys
import yaml
from src.cfbd_client import CFBDClient

def test_cfbd_client():
    """Test the official CFBD client for accurate 2024 data"""
    print("Testing official CFBD Python client integration...")
    
    try:
        with CFBDClient() as client:
            # Test 1: Validate 2024 season data
            print("\n1. Validating 2024 season data...")
            validation = client.validate_season_data(2024)
            
            print(f"   Teams count: {validation['teams_count']}")
            print(f"   Conferences count: {validation['conferences_count']}")
            print(f"   Teams with conferences: {validation['teams_with_conferences']}")
            print(f"   Validation passed: {validation['validation_passed']}")
            
            if validation['warnings']:
                for warning in validation['warnings']:
                    print(f"   Warning: {warning}")
            
            # Test 2: Key team conference assignments
            print("\n2. Testing key team conference assignments...")
            key_teams_2024 = {
                'BYU': 'Big 12',
                'Texas': 'SEC',
                'Oregon': 'Big Ten',
                'Washington': 'Big Ten',
                'USC': 'Big Ten',
                'UCLA': 'Big Ten',
                'SMU': 'ACC',
                'California': 'ACC',
                'Stanford': 'ACC'
            }
            
            correct_assignments = 0
            for team, expected_conf in key_teams_2024.items():
                actual_conf = client.get_team_conference(team, 2024)
                correct = actual_conf == expected_conf
                if correct:
                    correct_assignments += 1
                
                status = "✓" if correct else "✗"
                print(f"   {status} {team}: {actual_conf} (expected: {expected_conf})")
            
            # Test 3: Conference distribution
            print("\n3. Conference distribution:")
            for conf, count in sorted(validation['conference_distribution'].items()):
                print(f"   {conf}: {count} teams")
            
            # Test 4: Sample games retrieval
            print("\n4. Testing games retrieval...")
            games = client.get_games(2024, season_type='regular', week=1)
            print(f"   Retrieved {len(games)} completed games for Week 1")
            
            if games:
                sample_game = games[0]
                print(f"   Sample game: {sample_game.away_team} at {sample_game.home_team}")
                print(f"   Score: {sample_game.away_points}-{sample_game.home_points}")
                print(f"   Completed: {sample_game.completed}")
            
            # Test 5: Team canonicalization
            print("\n5. Testing team name canonicalization...")
            test_names = ['BYU', 'Brigham Young', 'Texas', 'USC', 'Southern California']
            
            for name in test_names:
                canonical = client.canonicalize_team_name(name, 2024)
                print(f"   '{name}' -> '{canonical}'")
            
            # Summary
            print("\n=== SUMMARY ===")
            assignment_rate = correct_assignments / len(key_teams_2024)
            print(f"Conference assignment accuracy: {correct_assignments}/{len(key_teams_2024)} ({assignment_rate:.1%})")
            
            if assignment_rate == 1.0 and validation['validation_passed']:
                print("✓ CFBD client integration successful!")
                print("✓ Ready for production pipeline with authentic data")
                return True
            else:
                print("✗ Some issues detected with CFBD client")
                return False
                
    except Exception as e:
        print(f"Error testing CFBD client: {e}")
        print("Please verify CFB_API_KEY is valid and has proper permissions")
        return False

if __name__ == "__main__":
    success = test_cfbd_client()
    sys.exit(0 if success else 1)