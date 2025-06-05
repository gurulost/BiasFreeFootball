#!/usr/bin/env python3
"""
Generate HTML table from rankings JSON export
Creates a clean, sortable table for public display
"""

import json
import sys
from datetime import datetime
from pathlib import Path

def generate_html_table(json_file: str) -> str:
    """Generate HTML table from rankings JSON"""
    
    try:
        with open(json_file, 'r') as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        return f"<html><body><h1>Error loading rankings: {e}</h1></body></html>"
    
    rankings = data.get('rankings', [])
    metadata = data.get('metadata', {})
    
    # Determine ranking type and title
    is_final = 'retro' in json_file or metadata.get('pipeline_type') == 'retro'
    season = metadata.get('season', 'Unknown')
    week = metadata.get('week', 'Final')
    
    if is_final:
        title = f"{season} Final College Football Rankings"
        subtitle = "Complete season retrospective analysis"
        rating_key = 'rating_retro'
    else:
        title = f"Week {week} College Football Rankings"
        subtitle = f"{season} season - Updated weekly"
        rating_key = 'rating_live'
    
    # Get generation timestamp
    generated_time = metadata.get('generated_at', datetime.now().isoformat())
    try:
        gen_dt = datetime.fromisoformat(generated_time.replace('Z', '+00:00'))
        formatted_time = gen_dt.strftime('%B %d, %Y at %I:%M %p UTC')
    except:
        formatted_time = generated_time
    
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title}</title>
    <link rel="stylesheet" href="style.css">
    <link href="https://cdn.datatables.net/1.13.6/css/jquery.dataTables.min.css" rel="stylesheet">
    <script src="https://code.jquery.com/jquery-3.7.1.min.js"></script>
    <script src="https://cdn.datatables.net/1.13.6/js/jquery.dataTables.min.js"></script>
</head>
<body>
    <div class="container">
        <header>
            <h1>{title}</h1>
            <p class="subtitle">{subtitle}</p>
            <p class="timestamp">Last updated: {formatted_time}</p>
        </header>
        
        <div class="rankings-table-container">
            <table id="rankings-table" class="display">
                <thead>
                    <tr>
                        <th>Rank</th>
                        <th>Team</th>
                        <th>Conference</th>
                        <th>Rating</th>
                        <th>Record</th>
                    </tr>
                </thead>
                <tbody>"""
    
    # Add table rows
    for i, team in enumerate(rankings, 1):
        team_name = team.get('team', 'Unknown')
        conference = team.get('conference', 'Independent')
        rating = team.get(rating_key, team.get('rating', 0))
        wins = team.get('wins', 0)
        losses = team.get('losses', 0)
        
        html += f"""
                    <tr>
                        <td>{i}</td>
                        <td class="team-name">{team_name}</td>
                        <td>{conference}</td>
                        <td>{rating:.6f}</td>
                        <td>{wins}-{losses}</td>
                    </tr>"""
    
    html += """
                </tbody>
            </table>
        </div>
        
        <footer>
            <div class="methodology">
                <h3>Methodology</h3>
                <p>Rankings generated using a two-layer PageRank algorithm with enhanced EM convergence. 
                   The system processes only FBS games to create unbiased, data-driven team rankings 
                   with comprehensive conference strength adjustment.</p>
                <p>Features: Bootstrap confidence intervals, relative conference scaling, 
                   and advanced neutrality metrics ensure mathematical accuracy and fairness.</p>
            </div>
            
            <div class="data-source">
                <p><strong>Data Source:</strong> College Football Data API</p>
                <p><strong>System:</strong> Industry-grade data validation with comprehensive edge condition handling</p>
            </div>
        </footer>
    </div>
    
    <script>
        $(document).ready(function() {
            $('#rankings-table').DataTable({
                "pageLength": 25,
                "order": [[ 0, "asc" ]],
                "columnDefs": [
                    { "orderable": false, "targets": 0 }
                ]
            });
        });
    </script>
</body>
</html>"""
    
    return html

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python make_html_table.py <rankings.json>")
        sys.exit(1)
    
    json_file = sys.argv[1]
    html_output = generate_html_table(json_file)
    print(html_output)