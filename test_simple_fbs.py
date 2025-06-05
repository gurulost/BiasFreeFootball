"""
Simple test for FBS-only enforcement validation
"""

import os
import yaml
from src.ingest import CFBDataIngester
from src.fbs_enforcer import create_fbs_enforcer

def test_simple_fbs():
    """Test basic FBS enforcement functionality"""
    
    # Load config
    with open('config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    print("=== SIMPLE FBS ENFORCEMENT TEST ===")
    
    # Test FBS teams
    print("1. Testing FBS teams fetch:")
    ingester = CFBDataIngester(config)
    fbs_teams = ingester.fetch_teams(2024, division='fbs')
    print(f"   FBS teams: {len(fbs_teams)}")
    
    # Test sample games
    print("2. Testing sample games fetch:")
    week1_games = ingester.fetch_games(2024, week=1, season_type='regular')
    print(f"   Week 1 games: {len(week1_games)}")
    
    # Test FBS enforcer rating scale
    print("3. Testing rating scale validation:")
    fbs_enforcer = create_fbs_enforcer(config)
    sample_ratings = {team['school']: 0.007 + 0.001 * i/len(fbs_teams) for i, team in enumerate(fbs_teams)}
    scale_report = fbs_enforcer.validate_rating_scale(sample_ratings)
    print(f"   Rating scale valid: {scale_report['validation_passed']}")
    print(f"   Team count: {scale_report['total_teams']}")
    
    # Test conference assignments
    print("4. Testing 2024 realignment:")
    team_to_conf = {t['school']: t.get('conference', 'Unknown') for t in fbs_teams}
    key_teams = ['Texas', 'Oregon', 'USC', 'SMU']
    for team in key_teams:
        conf = team_to_conf.get(team, 'Not Found')
        print(f"   {team}: {conf}")
    
    print("\n✓ SIMPLE FBS TEST COMPLETED")
    return True

if __name__ == "__main__":
    test_simple_fbs()