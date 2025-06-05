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
    return json.dumps(obj) if obj is not None else 'null'

# Load configuration
with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f)

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

@app.route('/api/run-pipeline', methods=['POST'])
def api_run_pipeline():
    """API endpoint to trigger pipeline runs"""
    data = request.json
    pipeline_type = data.get('type', 'live')
    week = data.get('week', 1)
    season = data.get('season', datetime.now().year)
    
    try:
        if pipeline_type == 'live':
            result = run_live(week, season)
        elif pipeline_type == 'retro':
            result = run_retro(season)
        else:
            return jsonify({"error": "Invalid pipeline type"}), 400
            
        return jsonify({"success": True, "result": result})
    except Exception as e:
        logging.error(f"Pipeline error: {e}")
        return jsonify({"error": str(e)}), 500

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

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
