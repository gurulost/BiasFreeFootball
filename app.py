import os
import logging
from flask import Flask, render_template, jsonify, request
from werkzeug.middleware.proxy_fix import ProxyFix
import yaml
import json
from datetime import datetime
from src.live_pipeline import run_live
from src.retro_pipeline import run_retro
from src.storage import Storage
from src.bias_audit import BiasAudit
from src.publish import Publisher

# Configure logging
logging.basicConfig(level=logging.DEBUG)

# Create the app
app = Flask(__name__)
app.secret_key = os.environ.get("SESSION_SECRET", "dev-key-change-in-production")
app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)

# Add custom Jinja2 filter for JSON conversion
@app.template_filter('tojsonfilter')
def to_json_filter(obj):
    try:
        if obj is None:
            return 'null'
        # Handle Jinja2 Undefined objects
        if hasattr(obj, '_undefined_hint'):
            return 'null'
        return json.dumps(obj, default=str)
    except (TypeError, ValueError):
        return 'null'

# Load configuration
with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f)

# Initialize automated ranking scheduler
from src.scheduler import start_automated_updates, stop_automated_updates, get_scheduler
import atexit

# Start automated updates when app starts
start_automated_updates(config)

# Ensure scheduler stops when app shuts down
atexit.register(stop_automated_updates)

# Initialize components
storage = Storage()
bias_audit = BiasAudit()
publisher = Publisher()

@app.route('/')
def index():
    """Main dashboard page"""
    try:
        # Get latest ratings
        latest_ratings = storage.get_latest_ratings()
        bias_metrics = bias_audit.get_latest_metrics()
        
        return render_template('index.html', 
                             latest_ratings=latest_ratings,
                             bias_metrics=bias_metrics,
                             config=config)
    except Exception as e:
        logging.error(f"Error loading dashboard: {e}")
        return render_template('index.html', 
                             latest_ratings=None,
                             bias_metrics=None,
                             config=config)

@app.route('/rankings')
def rankings():
    """Rankings page with detailed view"""
    week = request.args.get('week', 'latest')
    season = request.args.get('season', datetime.now().year)
    
    try:
        if week == 'latest':
            ratings_data = storage.get_latest_ratings()
        else:
            ratings_data = storage.load_ratings(int(week), int(season))
            
        return render_template('rankings.html', 
                             ratings_data=ratings_data,
                             week=week,
                             season=season)
    except Exception as e:
        logging.error(f"Error loading rankings: {e}")
        return render_template('rankings.html', 
                             ratings_data=None,
                             week=week,
                             season=season)

@app.route('/bias-audit')
def bias_audit_page():
    """Bias audit dashboard"""
    try:
        metrics_history = bias_audit.get_metrics_history()
        conference_trajectories = bias_audit.get_conference_trajectories()
        
        return render_template('bias_audit.html',
                             metrics_history=metrics_history,
                             conference_trajectories=conference_trajectories)
    except Exception as e:
        logging.error(f"Error loading bias audit: {e}")
        return render_template('bias_audit.html',
                             metrics_history=None,
                             conference_trajectories=None)

@app.route('/api/rankings')
def api_rankings():
    """API endpoint for rankings data"""
    week = request.args.get('week', 'latest')
    season = request.args.get('season', datetime.now().year)
    
    try:
        if week == 'latest':
            data = storage.get_latest_ratings()
        else:
            data = storage.load_ratings(int(week), int(season))
        
        return jsonify(data)
    except Exception as e:
        logging.error(f"API error: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/bias-metrics')
def api_bias_metrics():
    """API endpoint for bias metrics"""
    try:
        metrics = bias_audit.get_latest_metrics()
        return jsonify(metrics)
    except Exception as e:
        logging.error(f"API error: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/run_pipeline')
def api_run_pipeline():
    """API endpoint to trigger intelligent FBS-only pipeline runs"""
    try:
        from src.season_utils import get_pipeline_recommendation, should_use_retro_rankings
        
        # Get intelligent pipeline recommendation based on current date
        recommendation = get_pipeline_recommendation()
        
        if recommendation['pipeline'] == 'retro' or should_use_retro_rankings():
            # Between seasons - automatically use RETRO pipeline for definitive FBS rankings
            from src.retro_pipeline import run_retro
            result = run_retro(season=recommendation['season'], max_outer=6)
            
            if result.get('success'):
                return jsonify({
                    'success': True,
                    'message': f"Definitive {recommendation['season']} FBS season rankings",
                    'pipeline_type': 'retro',
                    'reason': recommendation['reason'],
                    'season': recommendation['season'],
                    'games_processed': result.get('metrics', {}).get('games_processed', 0),
                    'teams_ranked': len(result.get('team_ratings', {})),
                    'neutrality_metric': result.get('metrics', {}).get('neutrality_metric', 0)
                })
            else:
                return jsonify({
                    'success': False,
                    'error': result.get('error', 'Unknown error'),
                    'pipeline_type': 'retro'
                }), 500
                
        else:
            # Active season - use live pipeline for current week FBS games
            from src.live_pipeline import run_live
            result = run_live(week=recommendation['week'], season=recommendation['season'])
            
            if result.get('success'):
                return jsonify({
                    'success': True,
                    'message': f"Week {recommendation['week']} FBS live rankings",
                    'pipeline_type': 'live',
                    'reason': recommendation['reason'],
                    'week': recommendation['week'],
                    'season': recommendation['season'],
                    'games_processed': result.get('metrics', {}).get('games_processed', 0),
                    'teams_ranked': len(result.get('team_ratings', {})),
                    'neutrality_metric': result.get('metrics', {}).get('neutrality_metric', 0)
                })
            else:
                return jsonify({
                    'success': False,
                    'error': result.get('error', 'Unknown error'),
                    'pipeline_type': 'live'
                }), 500
        
    except Exception as e:
        logging.error(f"FBS Pipeline error: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/run-pipeline', methods=['POST'])
def api_run_pipeline_post():
    """API endpoint to trigger pipeline runs via POST"""
    return api_run_pipeline()

@app.route('/api/export/<format>')
def api_export(format):
    """Export rankings in CSV or JSON format"""
    week = request.args.get('week', 'latest')
    season = request.args.get('season', datetime.now().year)
    
    try:
        if week == 'latest':
            data = storage.get_latest_ratings()
        else:
            data = storage.load_ratings(int(week), int(season))
            
        if format == 'csv':
            csv_data = publisher.export_csv(data)
            return csv_data, 200, {'Content-Type': 'text/csv'}
        elif format == 'json':
            return jsonify(data)
        else:
            return jsonify({"error": "Invalid format"}), 400
            
    except Exception as e:
        logging.error(f"Export error: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/current')
def current_rankings():
    """Display current auto-updated rankings"""
    try:
        scheduler = get_scheduler(config)
        rankings_data = scheduler.get_current_rankings()
        
        if not rankings_data:
            rankings_data = storage.get_latest_ratings()
        
        if not rankings_data:
            return render_template('rankings.html', 
                                 rankings=[], 
                                 metadata={'title': 'Current Rankings', 'auto_updated': True},
                                 error="No current rankings available")
        
        rankings = rankings_data.get('rankings', [])
        metadata = rankings_data.get('metadata', {})
        metadata['title'] = 'Current Rankings'
        metadata['auto_updated'] = True
        metadata['next_update'] = "Every Monday at 3:30 AM ET"
        
        # Format data for template compatibility
        ratings_data = {
            'team_ratings': rankings,
            'metadata': metadata
        }
        return render_template('rankings.html', 
                             rankings=rankings, 
                             metadata=metadata,
                             ratings_data=ratings_data)
                             
    except Exception as e:
        logging.error(f"Current rankings error: {e}")
        return render_template('rankings.html', 
                             rankings=[], 
                             metadata={'title': 'Current Rankings', 'auto_updated': True},
                             ratings_data=None,
                             error=f"Error loading current rankings: {e}")

@app.route('/final')
@app.route('/final/<int:season>')
def final_rankings(season=None):
    """Display final season rankings"""
    try:
        if season is None:
            current_year = datetime.now().year
            season = current_year - 1 if datetime.now().month <= 7 else current_year
        
        scheduler = get_scheduler(config)
        rankings_data = scheduler.get_final_rankings(season)
        
        if not rankings_data:
            return render_template('rankings.html', 
                                 rankings=[], 
                                 metadata={'title': f'{season} Final Rankings', 'season': season},
                                 error=f"Final rankings for {season} not available yet")
        
        rankings = rankings_data.get('rankings', [])
        metadata = rankings_data.get('metadata', {})
        metadata['title'] = f'{season} Final Rankings'
        metadata['season'] = season
        metadata['is_final'] = True
        
        # Format data for template compatibility
        ratings_data = {
            'team_ratings': rankings,
            'metadata': metadata
        }
        return render_template('rankings.html', 
                             rankings=rankings, 
                             metadata=metadata,
                             ratings_data=ratings_data)
                             
    except Exception as e:
        logging.error(f"Final rankings error: {e}")
        return render_template('rankings.html', 
                             rankings=[], 
                             metadata={'title': f'{season} Final Rankings', 'season': season or 'Unknown'},
                             ratings_data=None,
                             error=f"Error loading final rankings: {e}")

@app.route('/api/current')
def api_current_rankings():
    """API endpoint for current rankings"""
    try:
        scheduler = get_scheduler(config)
        rankings_data = scheduler.get_current_rankings()
        
        if not rankings_data:
            rankings_data = storage.get_latest_ratings()
        
        if not rankings_data:
            return jsonify({'error': 'No current rankings available'}), 404
        
        return jsonify(rankings_data)
        
    except Exception as e:
        logging.error(f"API current rankings error: {e}")
        return jsonify({'error': 'Failed to load current rankings'}), 500

@app.route('/api/final')
@app.route('/api/final/<int:season>')
def api_final_rankings(season=None):
    """API endpoint for final rankings"""
    try:
        if season is None:
            current_year = datetime.now().year
            season = current_year - 1 if datetime.now().month <= 7 else current_year
        
        scheduler = get_scheduler(config)
        rankings_data = scheduler.get_final_rankings(season)
        
        if not rankings_data:
            return jsonify({'error': f'Final rankings for {season} not available'}), 404
        
        return jsonify(rankings_data)
        
    except Exception as e:
        logging.error(f"API final rankings error: {e}")
        return jsonify({'error': 'Failed to load final rankings'}), 500

@app.route('/api/update', methods=['POST'])
def api_manual_update():
    """Manually trigger ranking update"""
    try:
        data = request.get_json() or {}
        season = data.get('season')
        week = data.get('week')
        
        scheduler = get_scheduler(config)
        result = scheduler.manual_update(season=season, week=week)
        
        return jsonify({
            'status': 'success',
            'message': 'Rankings updated successfully',
            'result': result
        })
        
    except Exception as e:
        logging.error(f"Manual update error: {e}")
        return jsonify({
            'status': 'error',
            'message': f'Update failed: {e}'
        }), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
