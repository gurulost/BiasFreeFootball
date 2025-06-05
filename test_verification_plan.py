"""
Comprehensive verification test implementing the complete verification plan
Tests all fixes with authentic 2024 FBS data
"""

import json
import os
from pathlib import Path
from src.cfbd_client import create_cfbd_client
from run_authentic_pipeline import run_authentic_pipeline

def test_verification_plan():
    """Run comprehensive verification of all implemented fixes"""
    
    print("=== COMPREHENSIVE VERIFICATION PLAN TEST ===")
    print("Testing all fixes with authentic 2024 FBS data")
    
    # Configuration
    config = {
        'api': {'cfbd_key': os.environ.get('CFB_API_KEY')},
        'season': 2024,
        'week': 15,
        'paths': {
            'data_raw': './data/raw',
            'data_processed': './data/processed',
            'exports': './exports'
        },
        'validation': {
            'enable_hardening': True,
            'enforce_fbs_only': True
        }
    }
    
    if not config['api']['cfbd_key']:
        print("❌ CFB_API_KEY not found - cannot run verification with authentic data")
        return False
    
    # Test 1: No FCS Teams in Data
    print("\n1. Testing FBS-only enforcement...")
    
    try:
        client = create_cfbd_client(config['api']['cfbd_key'])
        fbs_teams = client.get_fbs_teams(2024)
        team_count = len(fbs_teams)
        
        print(f"   FBS teams fetched: {team_count}")
        
        # Expected: exactly 134 FBS teams for 2024
        fbs_count_correct = team_count == 134
        print(f"   Expected 134 FBS teams: {'✓' if fbs_count_correct else '✗'} ({team_count})")
        
        # Fetch games and verify FBS-only
        games = client.get_games(2024, season_type='both')
        fbs_only_games = [g for g in games if 
                         g.home_team in fbs_teams and g.away_team in fbs_teams]
        
        total_games = len(games)
        fbs_games = len(fbs_only_games)
        
        print(f"   Total games: {total_games}")
        print(f"   FBS-only games: {fbs_games}")
        
        # Expected: 800+ games for full season including bowls
        games_count_correct = fbs_games >= 800
        print(f"   Expected 800+ FBS games: {'✓' if games_count_correct else '✗'} ({fbs_games})")
        
        client.close()
        
    except Exception as e:
        print(f"   ❌ Error testing FBS enforcement: {e}")
        fbs_count_correct = False
        games_count_correct = False
    
    # Test 2: Run full pipeline and check outputs
    print("\n2. Running full authentic pipeline...")
    
    try:
        # Run the complete pipeline
        success = run_authentic_pipeline(season=2024)
        
        if not success:
            print("   ❌ Pipeline failed to complete")
            return False
        
        print("   ✓ Pipeline completed successfully")
        
    except Exception as e:
        print(f"   ❌ Pipeline error: {e}")
        return False
    
    # Test 3: Rating Scale and Spread
    print("\n3. Testing rating scale and spread...")
    
    try:
        # Load rankings output
        rankings_file = './exports/rankings_2024_week_15.json'
        if os.path.exists(rankings_file):
            with open(rankings_file, 'r') as f:
                rankings_data = json.load(f)
            
            rankings = rankings_data.get('rankings', [])
            if rankings:
                top_rating = max(team.get('rating', 0) for team in rankings)
                teams_above_008 = len([t for t in rankings if t.get('rating', 0) > 0.008])
                
                print(f"   Top team rating: {top_rating:.6f}")
                print(f"   Teams above 0.008: {teams_above_008}")
                
                # Expected: top rating > 0.013, 4+ teams above 0.008
                rating_scale_correct = top_rating > 0.013
                spread_correct = teams_above_008 >= 4
                
                print(f"   Top rating > 0.013: {'✓' if rating_scale_correct else '✗'}")
                print(f"   4+ teams above 0.008: {'✓' if spread_correct else '✗'}")
            else:
                print("   ❌ No rankings found in output")
                rating_scale_correct = False
                spread_correct = False
        else:
            print("   ❌ Rankings file not found")
            rating_scale_correct = False
            spread_correct = False
            
    except Exception as e:
        print(f"   ❌ Error checking rating scale: {e}")
        rating_scale_correct = False
        spread_correct = False
    
    # Test 4: Quality Wins Populated
    print("\n4. Testing quality wins population...")
    
    try:
        quality_wins_populated = True
        none_quality_wins = 0
        sample_teams = []
        
        if 'rankings' in locals():
            # Check top 10 teams for quality wins
            top_teams = sorted(rankings, key=lambda x: x.get('rating', 0), reverse=True)[:10]
            
            for team in top_teams:
                quality_wins = team.get('quality_wins', [])
                sample_teams.append({
                    'team': team.get('team', 'Unknown'),
                    'quality_wins': quality_wins,
                    'has_wins': len(quality_wins) > 0 and quality_wins != ['None']
                })
                
                if not quality_wins or quality_wins == ['None']:
                    none_quality_wins += 1
            
            print(f"   Top teams checked: {len(sample_teams)}")
            print(f"   Teams with authentic quality wins: {len(sample_teams) - none_quality_wins}")
            
            # Show examples
            for team_data in sample_teams[:3]:
                team_name = team_data['team']
                wins = team_data['quality_wins']
                print(f"     {team_name}: {wins}")
            
            quality_wins_populated = none_quality_wins <= 2  # Allow 2 teams with no quality wins
            print(f"   Quality wins properly populated: {'✓' if quality_wins_populated else '✗'}")
        else:
            print("   ❌ No rankings data available")
            quality_wins_populated = False
            
    except Exception as e:
        print(f"   ❌ Error checking quality wins: {e}")
        quality_wins_populated = False
    
    # Test 5: Correct Conference Labels
    print("\n5. Testing conference labels...")
    
    try:
        unknown_conferences = 0
        byu_conference_correct = False
        
        if 'rankings' in locals():
            for team in rankings:
                conference = team.get('conference', '')
                team_name = team.get('team', '')
                
                if conference == 'Unknown' or not conference:
                    unknown_conferences += 1
                    print(f"     ❌ {team_name}: Unknown conference")
                
                # Check BYU specifically (should be Big 12 in 2024)
                if team_name == 'BYU':
                    byu_conference_correct = conference == 'Big 12'
                    print(f"     BYU conference: {conference} {'✓' if byu_conference_correct else '✗'}")
            
            print(f"   Teams with unknown conferences: {unknown_conferences}")
            conferences_correct = unknown_conferences == 0
            print(f"   All conferences properly labeled: {'✓' if conferences_correct else '✗'}")
        else:
            print("   ❌ No rankings data available")
            conferences_correct = False
            byu_conference_correct = False
            
    except Exception as e:
        print(f"   ❌ Error checking conferences: {e}")
        conferences_correct = False
        byu_conference_correct = False
    
    # Test 6: BYU/Big 12 Rebound
    print("\n6. Testing BYU/Big 12 ranking rebound...")
    
    try:
        byu_ranking_correct = False
        big12_teams_correct = False
        
        if 'rankings' in locals():
            # Find BYU
            byu_data = next((team for team in rankings if team.get('team') == 'BYU'), None)
            if byu_data:
                byu_rating = byu_data.get('rating', 0)
                byu_rank = next((i+1 for i, team in enumerate(
                    sorted(rankings, key=lambda x: x.get('rating', 0), reverse=True)
                ) if team.get('team') == 'BYU'), None)
                
                print(f"   BYU rating: {byu_rating:.6f}")
                print(f"   BYU rank: {byu_rank}")
                
                # Expected: BYU rating ~0.009, rank top 20
                byu_ranking_correct = byu_rating >= 0.009 and byu_rank <= 20
                print(f"   BYU ranking appropriate: {'✓' if byu_ranking_correct else '✗'}")
            
            # Check Big 12 representation in upper tiers
            big12_top_teams = [team for team in rankings 
                              if team.get('conference') == 'Big 12' and team.get('rating', 0) > 0.008]
            
            print(f"   Big 12 teams above 0.008 rating: {len(big12_top_teams)}")
            big12_teams_correct = len(big12_top_teams) >= 2  # At least 2 strong Big 12 teams
            print(f"   Big 12 upper tier representation: {'✓' if big12_teams_correct else '✗'}")
        else:
            print("   ❌ No rankings data available")
            
    except Exception as e:
        print(f"   ❌ Error checking BYU/Big 12: {e}")
        byu_ranking_correct = False
        big12_teams_correct = False
    
    # Summary
    print(f"\n=== VERIFICATION PLAN RESULTS ===")
    
    test_results = {
        'fbs_team_count': fbs_count_correct,
        'fbs_games_count': games_count_correct,
        'rating_scale': rating_scale_correct,
        'rating_spread': spread_correct,
        'quality_wins_populated': quality_wins_populated,
        'conferences_correct': conferences_correct,
        'byu_conference': byu_conference_correct,
        'byu_ranking': byu_ranking_correct,
        'big12_representation': big12_teams_correct
    }
    
    passed_tests = sum(test_results.values())
    total_tests = len(test_results)
    
    print(f"Tests passed: {passed_tests}/{total_tests}")
    for test_name, passed in test_results.items():
        status = "✓" if passed else "✗"
        print(f"  {status} {test_name}")
    
    if passed_tests == total_tests:
        print("\n✅ ALL VERIFICATION TESTS PASSED")
        print("The ranking system is working correctly with:")
        print("• Authentic FBS-only data (134 teams, 800+ games)")
        print("• Proper rating scale and distribution")
        print("• Real quality wins from actual game results")
        print("• Correct 2024 conference assignments")
        print("• Realistic rankings reflecting on-field performance")
        return True
    else:
        print(f"\n❌ VERIFICATION INCOMPLETE ({passed_tests}/{total_tests} passed)")
        print("Some aspects need attention before deployment")
        return False

if __name__ == "__main__":
    test_verification_plan()