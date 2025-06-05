"""
Test script to validate season-specific team validation and FBS filtering
Ensures complete data integrity across the pipeline
"""

import os
import yaml
import json
from src.ingest import CFBDataIngester
from src.season_validator import validate_season_data

def test_season_validation():
    """Test the complete season validation pipeline"""
    
    # Load config
    with open('config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    # Initialize ingester
    ingester = CFBDataIngester(config)
    
    print("=== SEASON-SPECIFIC VALIDATION TEST ===")
    
    # Test 1: Fetch and validate 2024 FBS teams
    print("\n1. Testing 2024 FBS teams fetch and validation:")
    fbs_teams = ingester.fetch_teams(2024, division='fbs')
    print(f"   FBS teams retrieved: {len(fbs_teams)}")
    
    # Check team structure
    sample_team = fbs_teams[0] if fbs_teams else {}
    print(f"   Sample team structure: {list(sample_team.keys())}")
    
    # Count teams with conferences
    teams_with_conf = sum(1 for team in fbs_teams if team.get('conference'))
    print(f"   Teams with conference data: {teams_with_conf}/{len(fbs_teams)}")
    
    # Test 2: Fetch limited game data for validation
    print("\n2. Testing game data fetch (week 1 only):")
    week1_games = ingester.fetch_games(2024, week=1, season_type='regular')
    print(f"   Week 1 games: {len(week1_games)}")
    
    # Convert to DataFrame
    games_df = ingester.process_game_data(week1_games)
    print(f"   Processed games DataFrame: {len(games_df)} rows")
    
    # Test 3: Run season validation
    print("\n3. Running season-specific validation:")
    try:
        validated_games, validation_report = validate_season_data(fbs_teams, games_df, 2024, config)
        
        print(f"   Validation passed: {validation_report['validation_passed']}")
        print(f"   Teams validated: {validation_report['teams']['total_fbs']}")
        print(f"   Games validated: {validation_report['games']['total']}")
        
        if 'realignment_valid' in validation_report:
            print(f"   2024 realignment valid: {validation_report['realignment_valid']}")
        
        # Check for specific 2024 moves
        conf_dist = validation_report['teams']['conference_distribution']
        print(f"   Conference distribution sample: {dict(list(conf_dist.items())[:5])}")
        
    except Exception as e:
        print(f"   Error in validation: {e}")
        return False
    
    # Test 4: Verify key 2024 realignment teams
    print("\n4. Testing specific 2024 realignment validation:")
    
    team_to_conf = {team['school']: team.get('conference', 'Unknown') for team in fbs_teams}
    
    key_moves_2024 = {
        'Texas': 'SEC',
        'Oregon': 'Big Ten',
        'Washington': 'Big Ten', 
        'USC': 'Big Ten',
        'SMU': 'ACC',
        'Stanford': 'ACC'
    }
    
    correct_moves = 0
    for team, expected_conf in key_moves_2024.items():
        actual_conf = team_to_conf.get(team, 'Not Found')
        if actual_conf == expected_conf:
            correct_moves += 1
            print(f"   ✓ {team}: {actual_conf}")
        else:
            print(f"   ✗ {team}: {actual_conf} (expected: {expected_conf})")
    
    # Test 5: Verify FBS-only filtering effectiveness
    print("\n5. Testing FBS-only filtering effectiveness:")
    
    fbs_team_names = {team['school'] for team in fbs_teams}
    non_fbs_games = 0
    
    for _, game in validated_games.iterrows():
        home_team = game.get('home_team')
        away_team = game.get('away_team')
        
        if home_team not in fbs_team_names or away_team not in fbs_team_names:
            non_fbs_games += 1
    
    print(f"   Non-FBS games in validated set: {non_fbs_games}")
    print(f"   FBS filtering effectiveness: {100 * (1 - non_fbs_games / len(validated_games)):.1f}%")
    
    # Summary
    print(f"\n=== VALIDATION SUMMARY ===")
    print(f"FBS teams: {len(fbs_teams)}/134")
    print(f"Games processed: {len(validated_games)}")
    print(f"2024 realignment accuracy: {correct_moves}/{len(key_moves_2024)}")
    print(f"Non-FBS games: {non_fbs_games}")
    print(f"Overall validation: {validation_report['validation_passed']}")
    
    # Success criteria
    success = (
        len(fbs_teams) == 134 and
        non_fbs_games == 0 and
        correct_moves >= 5 and
        validation_report['validation_passed']
    )
    
    if success:
        print("✓ SEASON VALIDATION TEST PASSED")
        return True
    else:
        print("✗ SEASON VALIDATION TEST FAILED")
        return False

if __name__ == "__main__":
    test_season_validation()