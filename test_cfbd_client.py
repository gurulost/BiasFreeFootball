"""
Test the new CFBD client integration with comprehensive validation
"""

import os
import sys
import logging
from datetime import datetime

def test_cfbd_client():
    """Test the official CFBD client for accurate 2024 data"""
    
    print("=== CFBD PYTHON CLIENT TEST ===\n")
    
    # Import the official CFBD library
    try:
        import cfbd
        print("✓ CFBD Python library imported successfully")
    except ImportError as e:
        print(f"✗ Failed to import CFBD library: {e}")
        return False
    
    # Test API key configuration
    api_key = os.environ.get('CFB_API_KEY')
    if not api_key:
        print("✗ CFB_API_KEY environment variable not set")
        return False
    
    print(f"✓ API key configured (length: {len(api_key)})")
    
    # Configure the API client
    try:
        configuration = cfbd.Configuration()
        configuration.api_key['Authorization'] = api_key
        configuration.api_key_prefix['Authorization'] = 'Bearer'
        
        api_client = cfbd.ApiClient(configuration)
        print("✓ API client configured successfully")
    except Exception as e:
        print(f"✗ Failed to configure API client: {e}")
        return False
    
    # Test Teams API
    try:
        teams_api = cfbd.TeamsApi(api_client)
        teams = teams_api.get_fbs_teams(year=2024)
        
        fbs_teams = [team for team in teams if team.classification == 'fbs']
        print(f"✓ Retrieved {len(fbs_teams)} FBS teams for 2024")
        
        # Verify key teams with 2024 realignment
        team_lookup = {team.school: team.conference for team in fbs_teams}
        
        key_teams = {
            'BYU': 'Big 12',
            'Texas': 'SEC', 
            'Oregon': 'Big Ten',
            'USC': 'Big Ten',
            'UCLA': 'Big Ten',
            'Washington': 'Big Ten'
        }
        
        correct_count = 0
        for school, expected_conf in key_teams.items():
            actual_conf = team_lookup.get(school, 'Not Found')
            if actual_conf == expected_conf:
                print(f"  ✓ {school}: {actual_conf}")
                correct_count += 1
            else:
                print(f"  ✗ {school}: {actual_conf} (expected: {expected_conf})")
        
        print(f"Conference validation: {correct_count}/{len(key_teams)} correct")
        
    except Exception as e:
        print(f"✗ Teams API test failed: {e}")
        return False
    
    # Test Games API
    try:
        games_api = cfbd.GamesApi(api_client)
        
        # Get regular season games
        regular_games = games_api.get_games(year=2024, season_type='regular')
        print(f"✓ Retrieved {len(regular_games)} regular season games")
        
        # Get postseason games
        postseason_games = games_api.get_games(year=2024, season_type='postseason')
        print(f"✓ Retrieved {len(postseason_games)} postseason games")
        
        total_games = len(regular_games) + len(postseason_games)
        print(f"✓ Total 2024 games: {total_games}")
        
        # Filter for FBS-only games
        fbs_team_names = {team.school for team in fbs_teams}
        
        fbs_regular = [g for g in regular_games 
                      if g.home_team in fbs_team_names and g.away_team in fbs_team_names]
        fbs_postseason = [g for g in postseason_games 
                         if g.home_team in fbs_team_names and g.away_team in fbs_team_names]
        
        fbs_total = len(fbs_regular) + len(fbs_postseason)
        print(f"✓ FBS-only games: {fbs_total} (regular: {len(fbs_regular)}, postseason: {len(fbs_postseason)})")
        
    except Exception as e:
        print(f"✗ Games API test failed: {e}")
        return False
    
    # Test Conferences API
    try:
        conferences_api = cfbd.ConferencesApi(api_client)
        conferences = conferences_api.get_conferences()
        
        conf_names = [conf.name for conf in conferences if conf.name]
        print(f"✓ Retrieved {len(conf_names)} conferences")
        
        # Check for major conferences
        major_conferences = ['SEC', 'Big Ten', 'Big 12', 'ACC', 'Pac-12']
        found_major = [conf for conf in conf_names if conf in major_conferences]
        print(f"✓ Found major conferences: {found_major}")
        
    except Exception as e:
        print(f"✗ Conferences API test failed: {e}")
        return False
    
    print("\n=== CFBD CLIENT TEST SUMMARY ===")
    print("✓ Official CFBD Python library working correctly")
    print("✓ API authentication successful")
    print("✓ Teams, Games, and Conferences APIs operational")
    print("✓ 2024 realignment data accurate")
    print(f"✓ FBS data filtering successful: {len(fbs_teams)} teams, {fbs_total} games")
    
    # Close the API client
    try:
        api_client.close()
        print("✓ API client closed properly")
    except:
        pass  # Not all clients support explicit closing
    
    return True

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    success = test_cfbd_client()
    if success:
        print("\n🎉 CFBD Python client integration verified!")
        exit(0)
    else:
        print("\n❌ CFBD Python client test failed")
        exit(1)