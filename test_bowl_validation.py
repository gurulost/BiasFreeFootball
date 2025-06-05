"""
Direct test of bowl handling logic in weight calculation
Tests the core intra-conference bowl implementation
"""

from src.weights import WeightCalculator

def test_bowl_validation():
    """Test bowl handling logic directly"""
    
    print("=== BOWL HANDLING VALIDATION TEST ===")
    
    # Create weight calculator
    config = {
        'margin_cap': 5,
        'lambda_decay': 0.05,
        'venue_factors': {'home': 1.1, 'neutral': 1.0, 'away': 0.9},
        'bowl_bump': 1.10
    }
    
    weight_calc = WeightCalculator(config)
    
    # Test 1: Intra-conference bowl game
    print("1. Testing intra-conference bowl game:")
    intra_bowl_game = {
        'season_type': 'postseason',
        'winner_conference': 'Big 12',
        'loser_conference': 'Big 12',
        'points_winner': 28,
        'points_loser': 21,
        'venue': 'neutral',
        'winner_home': False,
        'week': 15,
        'season': 2024
    }
    
    weights = weight_calc.calculate_edge_weights(
        intra_bowl_game, 0.009, 0.008, 15, 12, 11
    )
    
    print(f"   is_bowl: {weights.get('is_bowl', False)}")
    print(f"   is_cross_conf: {weights.get('is_cross_conf', True)}")
    print(f"   is_intra_conf_bowl: {weights.get('is_intra_conf_bowl', False)}")
    print(f"   credit_weight: {weights.get('credit_weight', 0):.3f}")
    print(f"   conf_weight: {weights.get('conf_weight', 0):.3f}")
    
    # Validate intra-conference bowl logic
    intra_bowl_correct = (
        weights.get('is_bowl', False) and
        not weights.get('is_cross_conf', True) and
        weights.get('is_intra_conf_bowl', False) and
        weights.get('conf_weight', 0) == 0
    )
    
    print(f"   Intra-conference bowl logic: {'✓' if intra_bowl_correct else '✗'}")
    
    # Test 2: Cross-conference bowl game
    print("\n2. Testing cross-conference bowl game:")
    cross_bowl_game = {
        'season_type': 'postseason',
        'winner_conference': 'SEC',
        'loser_conference': 'Big Ten',
        'points_winner': 35,
        'points_loser': 24,
        'venue': 'neutral',
        'winner_home': False,
        'week': 15,
        'season': 2024
    }
    
    weights = weight_calc.calculate_edge_weights(
        cross_bowl_game, 0.009, 0.008, 15, 12, 11
    )
    
    print(f"   is_bowl: {weights.get('is_bowl', False)}")
    print(f"   is_cross_conf: {weights.get('is_cross_conf', False)}")
    print(f"   is_intra_conf_bowl: {weights.get('is_intra_conf_bowl', True)}")
    print(f"   credit_weight: {weights.get('credit_weight', 0):.3f}")
    print(f"   conf_weight: {weights.get('conf_weight', 0):.3f}")
    
    # Validate cross-conference bowl logic
    cross_bowl_correct = (
        weights.get('is_bowl', False) and
        weights.get('is_cross_conf', False) and
        not weights.get('is_intra_conf_bowl', True) and
        weights.get('conf_weight', 0) > 0
    )
    
    print(f"   Cross-conference bowl logic: {'✓' if cross_bowl_correct else '✗'}")
    
    # Test 3: Regular season game
    print("\n3. Testing regular season game:")
    regular_game = {
        'season_type': 'regular',
        'winner_conference': 'SEC',
        'loser_conference': 'Big Ten',
        'points_winner': 31,
        'points_loser': 17,
        'venue': 'home',
        'winner_home': True,
        'week': 8,
        'season': 2024
    }
    
    weights = weight_calc.calculate_edge_weights(
        regular_game, 0.009, 0.008, 8, 8, 7
    )
    
    print(f"   is_bowl: {weights.get('is_bowl', True)}")
    print(f"   is_cross_conf: {weights.get('is_cross_conf', False)}")
    print(f"   is_intra_conf_bowl: {weights.get('is_intra_conf_bowl', True)}")
    print(f"   credit_weight: {weights.get('credit_weight', 0):.3f}")
    print(f"   conf_weight: {weights.get('conf_weight', 0):.3f}")
    
    # Validate regular season logic
    regular_correct = (
        not weights.get('is_bowl', True) and
        weights.get('is_cross_conf', False) and
        not weights.get('is_intra_conf_bowl', True) and
        weights.get('conf_weight', 0) > 0
    )
    
    print(f"   Regular season logic: {'✓' if regular_correct else '✗'}")
    
    # Test 4: Bowl bump application
    print("\n4. Testing bowl bump application:")
    
    # Compare bowl vs regular game with same parameters
    base_game = {
        'winner_conference': 'ACC',
        'loser_conference': 'SEC',
        'points_winner': 28,
        'points_loser': 21,
        'venue': 'neutral',
        'winner_home': False,
        'week': 12,
        'season': 2024
    }
    
    regular_version = {**base_game, 'season_type': 'regular'}
    bowl_version = {**base_game, 'season_type': 'postseason', 'week': 15}
    
    regular_weights = weight_calc.calculate_edge_weights(regular_version, 0.009, 0.008, 12, 11, 10)
    bowl_weights = weight_calc.calculate_edge_weights(bowl_version, 0.009, 0.008, 15, 12, 11)
    
    regular_credit = regular_weights.get('credit_weight', 0)
    bowl_credit = bowl_weights.get('credit_weight', 0)
    
    print(f"   Regular game credit: {regular_credit:.3f}")
    print(f"   Bowl game credit: {bowl_credit:.3f}")
    print(f"   Bowl bump factor: {bowl_credit/regular_credit:.3f} (expected: ~1.10)")
    
    bowl_bump_correct = abs(bowl_credit/regular_credit - 1.10) < 0.05
    print(f"   Bowl bump applied: {'✓' if bowl_bump_correct else '✗'}")
    
    # Summary
    print(f"\n=== BOWL HANDLING VALIDATION SUMMARY ===")
    
    test_results = {
        'intra_bowl_detection': intra_bowl_correct,
        'cross_bowl_detection': cross_bowl_correct,
        'regular_season_handling': regular_correct,
        'bowl_bump_application': bowl_bump_correct
    }
    
    passed_tests = sum(test_results.values())
    total_tests = len(test_results)
    
    print(f"Tests passed: {passed_tests}/{total_tests}")
    for test_name, passed in test_results.items():
        status = "✓" if passed else "✗"
        print(f"  {status} {test_name}")
    
    if passed_tests == total_tests:
        print("\n✓ BOWL HANDLING IMPLEMENTATION CORRECT")
        print("Core logic properly handles:")
        print("• Intra-conference bowls get team credit but no conference credit")
        print("• Cross-conference bowls contribute to both team and conference graphs")
        print("• Bowl bump (1.10x) applied to all postseason games")
        print("• Regular season games processed normally")
        return True
    else:
        print("\n✗ BOWL HANDLING HAS ISSUES")
        return False

if __name__ == "__main__":
    test_bowl_validation()