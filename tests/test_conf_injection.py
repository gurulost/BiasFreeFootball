"""
Unit tests for conference strength injection
Verifies relative scaling implementation
"""

import pytest
import numpy as np
import networkx as nx
from src.graph import GraphBuilder


class TestConferenceInjection:
    """Test conference strength injection with relative scaling"""
    
    def setup_method(self):
        """Setup test configuration"""
        self.config = {
            'weights': {
                'risk_b': 1.0,
                'margin_cap': 5.0,
                'lambda_decay': 0.05,
                'shrinkage_k': 4,
                'win_prob_c': 0.40,
                'gamma': 0.75,
                'surprise_cap': 3.0,
                'bowl_bump': 1.10
            }
        }
        self.builder = GraphBuilder(self.config)
    
    def test_relative_scaler(self):
        """Test relative scaling calculations"""
        S = {"SEC": 0.40, "MAC": 0.70}
        mean_S = (0.40 + 0.70) / 2  # 0.55
        
        # SEC should get moderate dampening
        sec_multiplier = (0.40 / mean_S) ** 0.5
        assert abs(sec_multiplier - 0.85) < 1e-2
        
        # MAC should get moderate boost
        mac_multiplier = (0.70 / mean_S) ** 0.5
        assert abs(mac_multiplier - 1.12) < 1e-2
    
    def test_average_conference_unchanged(self):
        """Test that average strength conferences get multiplier = 1"""
        S = {"CONF_A": 0.30, "CONF_B": 0.60, "CONF_C": 0.60}
        mean_S = sum(S.values()) / len(S)  # 0.50
        
        # Conference with strength = mean should get multiplier = 1
        avg_conf = {"AVG": mean_S}
        multiplier = (avg_conf["AVG"] / mean_S) ** 0.5
        assert abs(multiplier - 1.0) < 1e-10
    
    def test_conference_injection_integration(self):
        """Test full conference strength injection"""
        # Create simple graph
        G = nx.DiGraph()
        G.add_edge("Team_A_SEC", "Team_B_SEC", weight=1.0)
        G.add_edge("Team_C_MAC", "Team_D_MAC", weight=1.0)
        G.add_edge("Team_A_SEC", "Team_C_MAC", weight=1.0)  # cross-conference
        
        # Conference ratings
        conf_ratings = {"SEC": 0.40, "MAC": 0.70}
        
        # Apply injection
        self.builder.inject_conf_strength(G, conf_ratings)
        
        # Check that intra-conference edges were modified
        sec_edge_weight = G["Team_A_SEC"]["Team_B_SEC"]["weight"]
        mac_edge_weight = G["Team_C_MAC"]["Team_D_MAC"]["weight"]
        cross_edge_weight = G["Team_A_SEC"]["Team_C_MAC"]["weight"]
        
        # Cross-conference edge should be unchanged
        assert abs(cross_edge_weight - 1.0) < 1e-10
        
        # Intra-conference edges should be scaled
        mean_S = (0.40 + 0.70) / 2
        expected_sec = (0.40 / mean_S) ** 0.5
        expected_mac = (0.70 / mean_S) ** 0.5
        
        assert abs(sec_edge_weight - expected_sec) < 1e-6
        assert abs(mac_edge_weight - expected_mac) < 1e-6
    
    def test_no_division_by_zero(self):
        """Test edge case with empty conference ratings"""
        G = nx.DiGraph()
        G.add_edge("Team_A", "Team_B", weight=1.0)
        
        # Empty conference ratings should not crash
        self.builder.inject_conf_strength(G, {})
        
        # Weight should be unchanged
        assert G["Team_A"]["Team_B"]["weight"] == 1.0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])