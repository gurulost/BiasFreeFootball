"""
Test script to validate the API endpoint fix for conference assignments
Tests the three-commit solution from the memo
"""

import os
import sys
import yaml
from src.ingest import CFBDataIngester

def load_config():
    """Load configuration"""
    with open('config.yaml', 'r') as f:
        return yaml.safe_load(f)

def test_api_fix():
    """Test the API endpoint fix for 2024 season data"""
    print("Testing API endpoint fix for conference assignments...")
    
    config = load_config()
    ingester = CFBDataIngester(config)
    
    season = 2024
    
    try:
        # Test 1: Fetch conferences and build mapping
        print(f"\n1. Fetching conferences for {season}...")
        conferences = ingester.fetch_conferences(season)
        print(f"   Retrieved {len(conferences)} conferences")
        
        # Test 2: Fetch teams with season-specific data
        print(f"\n2. Fetching teams for {season}...")
        teams = ingester.fetch_teams(season)
        print(f"   Retrieved {len(teams)} FBS teams")
        
        # Test 3: Validate key teams have correct conference assignments
        print(f"\n3. Validating key team conference assignments...")
        
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
        
        validation_results = {}
        for team_data in teams:
            team_name = team_data.get('school')
            conference_data = team_data.get('conference')
            
            if team_name in key_teams_2024:
                expected_conf = key_teams_2024[team_name]
                actual_conf = ingester.get_conference_name(conference_data, season)
                
                validation_results[team_name] = {
                    'expected': expected_conf,
                    'actual': actual_conf,
                    'correct': actual_conf == expected_conf
                }
                
                status = "✓" if actual_conf == expected_conf else "✗"
                print(f"   {status} {team_name}: {actual_conf} (expected: {expected_conf})")
        
        # Test 4: Conference distribution check
        print(f"\n4. Conference distribution analysis...")
        conf_counts = {}
        for team_data in teams:
            conf_data = team_data.get('conference')
            conf_name = ingester.get_conference_name(conf_data, season)
            conf_counts[conf_name] = conf_counts.get(conf_name, 0) + 1
        
        for conf, count in sorted(conf_counts.items()):
            print(f"   {conf}: {count} teams")
        
        # Test 5: Specific assertions from memo
        print(f"\n5. Running memo assertions...")
        
        byu_conf = None
        texas_conf = None
        big12_teams = []
        
        for team_data in teams:
            team_name = team_data.get('school')
            conf_data = team_data.get('conference')
            conf_name = ingester.get_conference_name(conf_data, season)
            
            if team_name == 'BYU':
                byu_conf = conf_name
            elif team_name == 'Texas':
                texas_conf = conf_name
            
            if conf_name == 'Big 12':
                big12_teams.append(team_name)
        
        print(f"   BYU conference: {byu_conf}")
        print(f"   Texas conference: {texas_conf}")
        print(f"   Big 12 teams count: {len(big12_teams)}")
        print(f"   Big 12 teams: {big12_teams}")
        
        # Summary
        correct_count = sum(1 for r in validation_results.values() if r['correct'])
        total_count = len(validation_results)
        
        print(f"\n=== SUMMARY ===")
        print(f"Validation: {correct_count}/{total_count} teams correctly assigned")
        
        if correct_count == total_count:
            print("✓ All key teams have correct 2024 conference assignments!")
            print("✓ API endpoint fix successful - ready for production pipeline")
        else:
            print("✗ Some teams still have incorrect assignments")
            print("→ API endpoint may need additional configuration")
            
            # Show failures
            print("\nIncorrect assignments:")
            for team, result in validation_results.items():
                if not result['correct']:
                    print(f"  {team}: got {result['actual']}, expected {result['expected']}")
        
        return correct_count == total_count
        
    except Exception as e:
        print(f"Error testing API fix: {e}")
        print(f"This likely indicates API authentication issues")
        print(f"Please verify CFB_API_KEY is valid and has proper permissions")
        return False

if __name__ == "__main__":
    success = test_api_fix()
    sys.exit(0 if success else 1)