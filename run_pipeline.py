"""
Run complete pipeline with authentic 2024 CFBD data
Generates accurate rankings based on real game results with a validation-first approach
"""

import os
import json
import yaml
import logging
from datetime import datetime
import pandas as pd

# Import the necessary components from your project
from src.ingest import CFBDataIngester
from src.data_quality_validator import DataQualityValidator
from src.graph import GraphBuilder
from src.pagerank import PageRankCalculator
from src.quality_wins import QualityWinsCalculator

def setup_logging():
    """Configure logging for pipeline run"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

def load_config():
    """Load configuration"""
    with open('config.yaml', 'r') as f:
        return yaml.safe_load(f)

def run_pipeline(season=2024):
    """
    Run a complete, validation-first pipeline with authentic CFBD data.
    This pipeline ensures data integrity BEFORE generating rankings.
    """
    logger = setup_logging()
    logger.info(f"Starting authentic pipeline for {season} season")

    try:
        # Load configuration
        config = load_config()

        # Initialize ingester and validator
        ingester = CFBDataIngester(config)
        quality_validator = DataQualityValidator(config)

        # --- Step 1: Ingest and Clean Raw Data ---
        logger.info("Step 1: Fetching and cleaning authentic team and game data")
        teams = ingester.fetch_teams(season)
        fbs_team_names = {team['school'].strip() for team in teams}

        ingester.fetch_conferences()  # Populates conference cache

        all_games = ingester.fetch_results_upto_bowls(season)
        all_games_df = ingester.process_game_data(all_games)

        if all_games_df.empty:
            logger.warning("No valid games found after processing. Exiting.")
            return {'success': False, 'error': 'No valid games found'}

        # Filter for games between two FBS teams
        fbs_games_df = all_games_df[
            all_games_df['winner'].isin(fbs_team_names) &
            all_games_df['loser'].isin(fbs_team_names)
        ].copy()

        logger.info(f"Initial ingestion complete: {len(teams)} teams, {len(fbs_games_df)} FBS games")

        if fbs_games_df.empty:
            logger.error("No FBS vs. FBS games were found in the dataset. Cannot proceed.")
            return {'success': False, 'error': 'No FBS vs. FBS games found'}

        # --- Step 2: Comprehensive Data Quality Validation ---
        logger.info("Step 2: Running comprehensive data quality validation")
        temp_ratings = {team['school']: 0.0 for team in teams}
        validation_report = quality_validator.run_comprehensive_validation(
            teams, fbs_games_df, temp_ratings, season
        )

        if not validation_report['overall_validation_passed']:
            critical_issues = validation_report['critical_issues']
            logger.error(f"PIPELINE FAILED due to critical data quality issues: {critical_issues}")
            raise ValueError(f"Data quality validation failed with issues: {critical_issues}")

        logger.info("✓ Comprehensive data quality validation PASSED")

        # --- Step 3: Generate Rankings from Validated Data ---
        logger.info("Step 3: Generating rankings with validated data")
        graph_builder = GraphBuilder(config)
        ranker = PageRankCalculator(config)

        # Build the initial graphs
        conf_graph, team_graph = graph_builder.build_graphs(fbs_games_df)

        # --- STAGE 1: Calculate Conference Strength ---
        logger.info("Running STAGE 1: Calculating conference strength ratings")
        conf_ratings = ranker.pagerank(conf_graph)
        logger.info(f"Top 5 conferences: {sorted(conf_ratings.items(), key=lambda x: x[1], reverse=True)[:5]}")

        # --- STAGE 2: Inject Conference Strength and Rank Teams ---
        logger.info("Running STAGE 2: Injecting conference strength into team graph")
        # The inject_conf_strength function modifies the team_graph in place
        graph_builder.inject_conf_strength(team_graph, conf_ratings)

        logger.info("Calculating final team ratings from conference-adjusted graph")
        team_ratings = ranker.pagerank(team_graph)

        # --- Step 4: Calculate Quality Wins & Build Final Rankings ---
        logger.info("Step 4: Calculating quality wins and building final rankings")
        team_conf_mapping = {team['school']: team.get('conference', 'Independent') for team in teams}
        quality_calculator = QualityWinsCalculator(config)
        quality_wins = quality_calculator.calculate_quality_wins(team_graph, team_ratings, max_wins=3)

        rankings_data = {
            'metadata': {
                'season': season,
                'generated_at': datetime.now().isoformat(),
                'data_source': 'authentic_cfbd_api',
                'total_games': len(fbs_games_df),
                'total_teams': len(team_ratings),
                'validation_status': 'PASSED'
            },
            'rankings': []
        }
        sorted_teams = sorted(team_ratings.items(), key=lambda x: x[1], reverse=True)
        for rank, (team, rating) in enumerate(sorted_teams, 1):
            rankings_data['rankings'].append({
                'rank': rank,
                'team': team,
                'conference': team_conf_mapping.get(team, 'Unknown'),
                'rating': rating,
                'quality_wins': quality_wins.get(team, [])
            })

        # --- Step 5: Save and Display Results ---
        logger.info("Step 5: Saving authentic rankings")
        os.makedirs('data/cache', exist_ok=True)
        os.makedirs('exports', exist_ok=True)

        cache_file = f"data/cache/final_rankings_{season}_authentic.json"
        export_file = f"exports/{season}_authentic.json"

        with open(cache_file, 'w') as f:
            json.dump(rankings_data, f, indent=2)
        with open(export_file, 'w') as f:
            json.dump(rankings_data, f, indent=2)

        logger.info(f"Authentic rankings saved to {export_file}")
        logger.info("--- Top 25 Authentic Rankings ---")
        for team_data in rankings_data['rankings'][:25]:
            logger.info(
                f"{team_data['rank']:2d}. {team_data['team']:20s} "
                f"({team_data['conference']:15s}) {team_data['rating']:.6f}"
            )

        logger.info("\nAuthentic pipeline completed successfully!")
        return {
            'success': True,
            'rankings_file': export_file,
            'total_teams': len(team_ratings),
            'total_games': len(fbs_games_df)
        }

    except Exception as e:
        logger.error(f"Pipeline failed with an unexpected error: {e}", exc_info=True)
        return {'success': False, 'error': str(e)}

if __name__ == "__main__":
    result = run_pipeline()
    if not result['success']:
        print(f"\n=== PIPELINE FAILED ===\nError: {result['error']}")
        exit(1)