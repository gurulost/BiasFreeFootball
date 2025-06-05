"""
Test script to validate the CFBD Games endpoint fix
Verifies that manual FBS filtering works correctly after discovering the division parameter bug
"""

import os
import yaml
import json
from src.ingest import CFBDataIngester

def test_games_endpoint_fix():
    """Test the fixed FBS filtering in games endpoint"""
    
    # Load config
    with open('config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    # Initialize ingester
    ingester = CFBDataIngester(config)
    
    print("=== CFBD GAMES ENDPOINT FIX VALIDATION ===")
    
    # Test 1: Verify FBS teams count
    print("\n1. Testing FBS teams endpoint (should work correctly):")
    fbs_teams = ingester.fetch_teams(2024, division='fbs')
    print(f"   FBS teams retrieved: {len(fbs_teams)}")
    print(f"   Expected: 134 (2024 FBS count)")
    
    if len(fbs_teams) == 134:
        print("   ✓ FBS teams count correct")
    else:
        print(f"   ⚠ FBS teams count unexpected: {len(fbs_teams)}")
    
    # Test 2: Test fixed games endpoint with manual filtering
    print("\n2. Testing fixed games endpoint (with manual FBS filtering):")
    week1_games = ingester.fetch_games(2024, week=1, season_type='regular')
    print(f"   Week 1 FBS games: {len(week1_games)}")
    
    # Verify all games are FBS vs FBS
    non_fbs_count = 0
    for game in week1_games:
        home_class = game.get('homeClassification')
        away_class = game.get('awayClassification')
        if home_class != 'fbs' or away_class != 'fbs':
            non_fbs_count += 1
            print(f"   Non-FBS game found: {game.get('homeTeam')} vs {game.get('awayTeam')} ({home_class} vs {away_class})")
    
    if non_fbs_count == 0:
        print("   ✓ All games are FBS vs FBS")
    else:
        print(f"   ⚠ Found {non_fbs_count} non-FBS games")
    
    # Test 3: Test complete season fetch
    print("\n3. Testing complete season fetch with FBS filtering:")
    complete_games = ingester.fetch_results_upto_bowls(2024)
    print(f"   Complete 2024 FBS season games: {len(complete_games)}")
    print(f"   Expected: ~798 games (752 regular + 46 bowls)")
    
    if len(complete_games) >= 790 and len(complete_games) <= 810:
        print("   ✓ Game count in expected range")
    else:
        print(f"   ⚠ Game count outside expected range")
    
    # Test 4: Verify conference assignments are present
    print("\n4. Testing conference assignments in game data:")
    games_with_conf = 0
    sample_conferences = set()
    
    for game in complete_games[:50]:  # Sample first 50 games
        if 'homeConference' in game and 'awayConference' in game:
            games_with_conf += 1
            sample_conferences.add(game['homeConference'])
            sample_conferences.add(game['awayConference'])
    
    print(f"   Games with conference data: {games_with_conf}/50")
    print(f"   Sample conferences: {sorted(list(sample_conferences))[:5]}...")
    
    if games_with_conf >= 45:  # Allow for some missing data
        print("   ✓ Conference assignments working")
    else:
        print("   ⚠ Conference assignments missing")
    
    # Test 5: Validate 2024 realignment
    print("\n5. Testing 2024 conference realignment:")
    test_teams = {
        'Texas': 'SEC',
        'Oregon': 'Big Ten', 
        'Washington': 'Big Ten',
        'USC': 'Big Ten',
        'UCLA': 'Big Ten',
        'SMU': 'ACC',
        'Stanford': 'ACC',
        'California': 'ACC'
    }
    
    team_to_conf = {}
    for game in complete_games:
        home_team = game.get('homeTeam')
        away_team = game.get('awayTeam')
        home_conf = game.get('homeConference')
        away_conf = game.get('awayConference')
        
        if home_team and home_conf:
            team_to_conf[home_team] = home_conf
        if away_team and away_conf:
            team_to_conf[away_team] = away_conf
    
    realignment_correct = 0
    for team, expected_conf in test_teams.items():
        actual_conf = team_to_conf.get(team, 'Not Found')
        if actual_conf == expected_conf:
            realignment_correct += 1
            print(f"   ✓ {team}: {actual_conf}")
        else:
            print(f"   ⚠ {team}: {actual_conf} (expected: {expected_conf})")
    
    print(f"\n=== SUMMARY ===")
    print(f"FBS teams count: {len(fbs_teams)}/134")
    print(f"Week 1 non-FBS games: {non_fbs_count}")
    print(f"Complete season games: {len(complete_games)}")
    print(f"Conference assignments: {games_with_conf}/50")
    print(f"2024 realignment accuracy: {realignment_correct}/{len(test_teams)}")
    
    overall_success = (
        len(fbs_teams) == 134 and
        non_fbs_count == 0 and
        len(complete_games) >= 790 and
        games_with_conf >= 45 and
        realignment_correct >= 7
    )
    
    if overall_success:
        print("✓ GAMES ENDPOINT FIX VALIDATION PASSED")
        return True
    else:
        print("⚠ GAMES ENDPOINT FIX VALIDATION FAILED")
        return False

if __name__ == "__main__":
    test_games_endpoint_fix()