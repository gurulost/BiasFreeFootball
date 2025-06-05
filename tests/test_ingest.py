"""
Test suite for data ingestion module
Validates data integrity and canonical team mapping
"""

import pytest
import json
import yaml
import tempfile
import os
from pathlib import Path
from src.ingest import CFBDataIngester

class TestDataIngestion:
    """Test data ingestion and validation"""
    
    @pytest.fixture
    def config(self):
        """Test configuration"""
        return {
            'api': {
                'base_url': 'https://api.collegefootballdata.com',
                'key': os.getenv('CFB_API_KEY', '')
            },
            'paths': {
                'data_raw': 'data/raw',
                'data_processed': 'data/processed'
            },
            'validation': {
                'strict_mode': True  # Enable CI blocking
            }
        }
    
    @pytest.fixture
    def ingester(self, config):
        """Create ingester instance"""
        return CFBDataIngester(config)
    
    def test_canonical_teams_file_exists(self):
        """Test that canonical teams file exists and is valid"""
        canonical_path = Path('data/canonical_teams.yaml')
        assert canonical_path.exists(), "canonical_teams.yaml file must exist"
        
        with open(canonical_path, 'r') as f:
            teams = yaml.safe_load(f)
        
        assert teams is not None, "canonical_teams.yaml must contain valid YAML"
        assert len(teams) > 0, "canonical_teams.yaml must not be empty"
    
    def test_canonical_team_mapping(self, ingester):
        """Test canonical team name mapping"""
        # Test known teams
        known_teams = ['BYU', 'Ohio State', 'Alabama', 'Georgia']
        
        for team in known_teams:
            canonical = ingester.canonicalize_team(team)
            assert canonical is not None, f"Team '{team}' must have canonical mapping"
            assert 'name' in canonical, "Canonical mapping must have 'name' field"
            assert 'conf' in canonical, "Canonical mapping must have 'conf' field"
            assert canonical['conf'] is not None, f"Team '{team}' must have valid conference"
    
    def test_no_null_conferences(self, ingester):
        """Test that no teams have null conferences (CI blocking)"""
        teams = ingester.canonical_teams
        
        null_conf_teams = []
        for team_name, team_data in teams.items():
            if team_data.get('conf') is None:
                null_conf_teams.append(team_name)
        
        assert len(null_conf_teams) == 0, \
            f"Teams with null conferences found (requires manual assignment): {null_conf_teams}"
    
    def test_data_validation_with_authentic_data(self, ingester):
        """Test data validation with actual API data"""
        if not os.getenv('CFB_API_KEY'):
            pytest.skip("CFB_API_KEY not provided - skipping authentic data test")
        
        try:
            # Fetch real 2024 data
            games = ingester.fetch_results_upto_bowls(2024)
            games_df = ingester.process_game_data(games)
            
            # Validate data integrity
            assert len(games_df) > 700, "Should have 700+ FBS games for complete season"
            
            # Check that major teams are included
            all_teams = set()
            for _, game in games_df.iterrows():
                all_teams.add(game['winner'])
                all_teams.add(game['loser'])
            
            major_teams = ['BYU', 'Ohio State', 'Alabama', 'Georgia', 'Texas']
            for team in major_teams:
                assert team in all_teams, f"Major team '{team}' missing from data"
            
            # Validate BYU specifically (was affected by data integrity issue)
            byu_games = games_df[
                (games_df['winner'] == 'BYU') | (games_df['loser'] == 'BYU')
            ]
            assert len(byu_games) >= 10, f"BYU should have 10+ games, found {len(byu_games)}"
            
        except Exception as e:
            if "Too many data validation failures" in str(e):
                pytest.fail("Data validation failed - missing aliases detected")
            else:
                raise
    
    def test_strict_mode_blocks_missing_aliases(self, config):
        """Test that strict mode blocks execution when aliases are missing"""
        # Create mock game data with unknown team
        mock_games = [{
            'completed': True,
            'homeTeam': 'Unknown Team',
            'awayTeam': 'BYU',
            'homePoints': 14,
            'awayPoints': 21,
            'season': 2024,
            'week': 1
        }]
        
        # Enable strict mode
        config['validation']['strict_mode'] = True
        ingester = CFBDataIngester(config)
        
        # Should raise assertion error in strict mode
        with pytest.raises(AssertionError, match="Unhandled aliases in strict mode"):
            ingester.process_game_data(mock_games)
    
    def test_missing_aliases_report_generation(self, ingester, tmp_path):
        """Test automatic missing aliases report generation"""
        # Create mock game data with unknown teams
        mock_games = [{
            'completed': True,
            'homeTeam': 'Unknown Team A',
            'awayTeam': 'Unknown Team B', 
            'homePoints': 14,
            'awayPoints': 21,
            'season': 2024,
            'week': 1
        }]
        
        # Temporarily change reports directory to temp path
        original_cwd = os.getcwd()
        os.chdir(tmp_path)
        
        try:
            # Process data (should generate report)
            ingester.process_game_data(mock_games)
            
            # Check that report was generated
            report_path = tmp_path / 'reports' / 'missing_aliases_2024.json'
            assert report_path.exists(), "Missing aliases report should be generated"
            
            # Validate report content
            with open(report_path, 'r') as f:
                missing_aliases = json.load(f)
            
            assert 'Unknown Team A' in missing_aliases
            assert 'Unknown Team B' in missing_aliases
            
        finally:
            os.chdir(original_cwd)

if __name__ == '__main__':
    pytest.main([__file__, '-v'])