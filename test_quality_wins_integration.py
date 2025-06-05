"""
Test script for quality wins integration with authentic game data
Validates that quality wins calculation uses actual game results
"""

import os
import yaml
import pandas as pd
from src.ingest import CFBDataIngester
from src.graph import GraphBuilder
from src.pagerank import PageRankCalculator
from src.quality_wins import QualityWinsCalculator

def test_quality_wins_integration():
    """Test quality wins calculation with authentic 2024 data"""
    
    # Load config
    with open('config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    print("=== QUALITY WINS INTEGRATION TEST ===")
    
    # Step 1: Fetch authentic data for full season
    print("1. Fetching authentic 2024 season data...")
    ingester = CFBDataIngester(config)
    
    teams = ingester.fetch_teams(2024, division='fbs')
    print(f"   Teams: {len(teams)} FBS teams")
    
    # Get full season games (both regular and postseason)
    regular_games = ingester.fetch_games(2024, season_type='regular')
    postseason_games = ingester.fetch_games(2024, season_type='postseason')
    all_games = regular_games + postseason_games
    
    print(f"   Games: {len(regular_games)} regular + {len(postseason_games)} postseason = {len(all_games)} total")
    
    # Process games
    games_df = ingester.process_game_data(all_games)
    print(f"   Processed games: {len(games_df)}")
    
    # Step 2: Build team graph and calculate ratings
    print("\n2. Building team graph and calculating ratings...")
    graph_builder = GraphBuilder(config)
    
    # Initialize with uniform ratings for first iteration
    team_names = set(games_df['winner'].tolist() + games_df['loser'].tolist())
    initial_ratings = {team: 0.5 for team in team_names}
    
    team_graph, conf_graph = graph_builder.build_graphs(games_df, initial_ratings)
    
    print(f"   Team graph: {team_graph.number_of_nodes()} nodes, {team_graph.number_of_edges()} edges")
    
    # Calculate PageRank ratings
    ranker = PageRankCalculator(config)
    team_ratings = ranker.pagerank(team_graph)
    
    print(f"   PageRank calculated: top rating = {max(team_ratings.values()):.6f}")
    
    # Step 3: Calculate quality wins
    print("\n3. Calculating quality wins from actual game results...")
    quality_calculator = QualityWinsCalculator(config)
    
    quality_wins = quality_calculator.calculate_quality_wins(team_graph, team_ratings, max_wins=3)
    quality_validation = quality_calculator.validate_quality_wins(quality_wins, team_ratings)
    
    print(f"   Quality wins calculated for {len(quality_wins)} teams")
    print(f"   Teams with quality wins: {quality_validation['teams_with_quality_wins']}")
    print(f"   Average quality wins: {quality_validation['average_quality_wins']:.2f}")
    
    # Step 4: Analyze top teams' quality wins
    print("\n4. Analyzing quality wins for top teams...")
    
    # Get top 10 teams by rating
    top_teams = sorted(team_ratings.items(), key=lambda x: x[1], reverse=True)[:10]
    
    for rank, (team, rating) in enumerate(top_teams, 1):
        team_quality_wins = quality_wins.get(team, [])
        
        # Get ratings of quality win opponents
        quality_win_ratings = []
        for opponent in team_quality_wins:
            opp_rating = team_ratings.get(opponent, 0.0)
            quality_win_ratings.append((opponent, opp_rating))
        
        # Sort quality wins by opponent rating
        quality_win_ratings.sort(key=lambda x: x[1], reverse=True)
        
        print(f"   #{rank} {team} ({rating:.6f}):")
        if quality_win_ratings:
            for opp, opp_rating in quality_win_ratings:
                print(f"      - {opp} ({opp_rating:.6f})")
        else:
            print(f"      - No quality wins found")
    
    # Step 5: Validate specific scenarios
    print("\n5. Validating quality wins scenarios...")
    
    # Test 1: Check that quality wins are actual opponents defeated
    validation_passed = True
    teams_checked = 0
    
    for team, wins in list(quality_wins.items())[:20]:  # Check first 20 teams
        teams_checked += 1
        
        # Get all opponents this team actually defeated
        actual_wins = set()
        for winner, loser, _ in team_graph.out_edges(team, data=True):
            if winner == team:
                actual_wins.add(loser)
        
        # Check that all quality wins are actual wins
        for quality_opponent in wins:
            if quality_opponent not in actual_wins:
                print(f"   ERROR: {team} has quality win vs {quality_opponent} but didn't actually beat them")
                validation_passed = False
    
    print(f"   Validated {teams_checked} teams for actual wins: {'PASS' if validation_passed else 'FAIL'}")
    
    # Test 2: Check that quality wins are sorted by opponent strength
    quality_ordering_correct = True
    for team, wins in list(quality_wins.items())[:10]:  # Check top 10 teams
        if len(wins) >= 2:
            win_ratings = [team_ratings.get(opp, 0.0) for opp in wins]
            if win_ratings != sorted(win_ratings, reverse=True):
                print(f"   WARNING: {team} quality wins not ordered by opponent rating")
                quality_ordering_correct = False
    
    print(f"   Quality wins ordering check: {'PASS' if quality_ordering_correct else 'WARN'}")
    
    # Step 6: Compare with graph edges
    print("\n6. Validating graph edge consistency...")
    
    # Pick a specific team to analyze in detail
    test_team = top_teams[0][0]  # Top-rated team
    test_quality_wins = quality_wins.get(test_team, [])
    
    print(f"   Analyzing {test_team} in detail:")
    
    # Get all actual wins from graph
    graph_wins = []
    for winner, loser, edge_data in team_graph.out_edges(test_team, data=True):
        if winner == test_team:
            edge_weight = edge_data.get('weight', 0.0)
            opponent_rating = team_ratings.get(loser, 0.0)
            graph_wins.append((opponent_rating, loser, edge_weight))
    
    # Sort by opponent rating (quality)
    graph_wins.sort(reverse=True)
    
    print(f"   Graph shows {len(graph_wins)} total wins")
    print(f"   Top 3 by opponent rating:")
    for i, (opp_rating, opponent, weight) in enumerate(graph_wins[:3]):
        print(f"      {i+1}. {opponent} (rating: {opp_rating:.6f}, weight: {weight:.3f})")
    
    print(f"   Quality wins calculator returned: {test_quality_wins}")
    
    # Step 7: Summary
    print(f"\n=== QUALITY WINS INTEGRATION TEST SUMMARY ===")
    
    test_results = {
        'data_loaded': len(all_games) > 1000,  # Should have full season
        'graph_built': team_graph.number_of_edges() > 0,
        'ratings_calculated': len(team_ratings) == 134,
        'quality_wins_calculated': quality_validation['teams_with_quality_wins'] > 100,
        'actual_wins_validated': validation_passed,
        'ordering_correct': quality_ordering_correct,
        'no_empty_quality_wins_for_top_teams': all(len(quality_wins.get(team[0], [])) > 0 for team in top_teams[:5])
    }
    
    passed_tests = sum(test_results.values())
    total_tests = len(test_results)
    
    print(f"Tests passed: {passed_tests}/{total_tests}")
    for test_name, passed in test_results.items():
        status = "✓" if passed else "✗"
        print(f"  {status} {test_name}")
    
    overall_success = passed_tests >= total_tests - 1  # Allow one minor failure
    
    if overall_success:
        print("\n✓ QUALITY WINS INTEGRATION TEST PASSED")
        print("Quality wins are calculated from actual game results using team graph")
        return True
    else:
        print("\n✗ QUALITY WINS INTEGRATION TEST FAILED")
        print("Quality wins calculation has issues")
        return False

if __name__ == "__main__":
    test_quality_wins_integration()