"""
Quick check of BYU's specific data handling issue
"""
import yaml
import pandas as pd
from src.ingest import CFBDataIngester
from src.validation import DataValidator

# Load BYU's specific games and check data flow
with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f)

ingester = CFBDataIngester(config)
games_data = ingester.fetch_results_upto_bowls(2024)
validator = DataValidator(config)
canonical_teams = ingester._load_canonical_teams()
games_df = validator.validate_complete_dataset(games_data, canonical_teams, 2024)

# Check BYU specifically
byu_games = games_df[
    (games_df['home_team'] == 'BYU') | (games_df['away_team'] == 'BYU')
]

print(f"BYU games found: {len(byu_games)}")
print(f"BYU wins: {len(byu_games[byu_games['winner'] == 'BYU'])}")
print(f"BYU losses: {len(byu_games[byu_games['loser'] == 'BYU'])}")

# Check if BYU name is being handled correctly
print(f"\nBYU canonical mapping: {canonical_teams.get('BYU', 'NOT FOUND')}")

# Sample some wins and losses
wins = byu_games[byu_games['winner'] == 'BYU'].head(3)
losses = byu_games[byu_games['loser'] == 'BYU'].head(2)

print("\nSample BYU wins:")
for _, game in wins.iterrows():
    print(f"  vs {game['loser']:15s} {game['winner_score']}-{game['loser_score']} (margin: {game['margin']})")

print("\nSample BYU losses:")
for _, game in losses.iterrows():
    print(f"  vs {game['winner']:15s} {game['loser_score']}-{game['winner_score']} (margin: {game['margin']})")