"""
FBS-only pipeline for authentic 2024 college football rankings
Filters College Football Data API for FBS teams and games only
"""

import yaml
import logging
import pandas as pd
from src.ingest import CFBDataIngester
from src.graph import GraphBuilder
from src.pagerank import pagerank
from src.bias_audit import BiasAudit
from src.publish import Publisher

def run_fbs_only_pipeline(season=2024, week=15):
    """Run pipeline with FBS teams and games only"""
    
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    # Load config
    with open('config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    logger.info(f"Starting FBS-only pipeline for {season} season, week {week}")
    
    # Step 1: Get authentic FBS teams list from API
    ingester = CFBDataIngester(config)
    fbs_teams = ingester.fetch_teams(season, division='fbs')
    fbs_team_names = {team['school'] for team in fbs_teams}
    
    logger.info(f"Found {len(fbs_team_names)} authentic FBS teams from API")
    
    # Step 2: Fetch all games for the season
    all_games = []
    for w in range(1, week + 1):
        week_games = ingester.fetch_games(season, w, 'regular')
        all_games.extend(week_games)
    
    # Add postseason games
    postseason_games = ingester.fetch_games(season, season_type='postseason')
    all_games.extend(postseason_games)
    
    logger.info(f"Fetched {len(all_games)} total games from API")
    
    # Step 3: Filter for FBS-only games
    fbs_games = []
    for game in all_games:
        home_team = game.get('home_team')
        away_team = game.get('away_team')
        
        # Only include games where both teams are FBS
        if (home_team in fbs_team_names and away_team in fbs_team_names and
            game.get('completed', False)):
            fbs_games.append(game)
    
    logger.info(f"Filtered to {len(fbs_games)} FBS-only completed games")
    
    # Step 4: Process games into DataFrame
    games_df = ingester.process_game_data(fbs_games)
    
    # Step 5: Build graphs with FBS data only
    graph_builder = GraphBuilder(config)
    
    # Initialize ratings for FBS teams only
    R_init = {team: 0.5 for team in fbs_team_names}
    
    G_conf, G_team = graph_builder.build_graphs(games_df, R_init, week)
    
    logger.info(f"Built graphs: {G_team.number_of_nodes()} teams, {G_team.number_of_edges()} team edges")
    logger.info(f"Conference graph: {G_conf.number_of_nodes()} conferences, {G_conf.number_of_edges()} conf edges")
    
    # Step 6: Two-stage PageRank
    # Stage 1: Conference strength
    S = pagerank(G_conf, damping=config['pagerank']['damping'], config=config)
    
    # Stage 2: Team ratings with conference injection
    graph_builder.inject_conf_strength(G_team, S, S)
    R = pagerank(G_team, damping=config['pagerank']['damping'], config=config)
    
    # Step 7: Bias audit
    bias_audit = BiasAudit(config)
    bias_metrics = bias_audit.compute_detailed_metrics(R, S, week)
    
    logger.info(f"Neutrality metric: {bias_metrics.get('neutrality_metric', 'N/A'):.6f}")
    
    # Step 8: Generate rankings
    team_rankings = sorted(R.items(), key=lambda x: x[1], reverse=True)
    conf_rankings = sorted(S.items(), key=lambda x: x[1], reverse=True)
    
    # Step 9: Display results
    print(f"\n=== 2024 AUTHENTIC FBS RANKINGS ===")
    print(f"Source: College Football Data API (FBS-only filter)")
    print(f"Total FBS teams: {len(R)}")
    print(f"Total FBS games: {len(games_df)}")
    print(f"Neutrality metric: {bias_metrics.get('neutrality_metric', 0):.6f}")
    
    print(f"\n=== TOP 25 FBS TEAMS ===")
    for i, (team, rating) in enumerate(team_rankings[:25]):
        print(f"{i+1:2d}. {team:30} {rating:.6f}")
    
    print(f"\n=== COLLEGE FOOTBALL PLAYOFF FIELD ===")
    for i, (team, rating) in enumerate(team_rankings[:12]):
        print(f"{i+1:2d}. {team}")
    
    print(f"\n=== FBS CONFERENCE STRENGTH ===")
    for i, (conf, strength) in enumerate(conf_rankings[:10]):
        print(f"{i+1:2d}. {conf:20} {strength:.6f}")
    
    # Save results
    publisher = Publisher(config)
    results = publisher.weekly_csv_json(S, R, week, season, bias_metrics.get('neutrality_metric', 0))
    
    print(f"\nResults saved to:")
    for file_type, path in results.items():
        print(f"{file_type}: {path}")
    
    return {
        'team_ratings': R,
        'conference_ratings': S,
        'bias_metrics': bias_metrics,
        'games_processed': len(games_df),
        'teams_ranked': len(R)
    }

if __name__ == "__main__":
    run_fbs_only_pipeline()