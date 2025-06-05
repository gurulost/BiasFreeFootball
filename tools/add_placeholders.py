#!/usr/bin/env python3
"""
Auto-generate canonical team placeholders for missing aliases
Usage: python tools/add_placeholders.py reports/missing_aliases_2024.json
"""

import json
import sys
import yaml
from pathlib import Path

def load_missing_aliases(filepath):
    """Load missing aliases from JSON report"""
    with open(filepath, 'r') as f:
        return json.load(f)

def load_canonical_teams():
    """Load existing canonical teams mapping"""
    canonical_path = Path('data/canonical_teams.yaml')
    if canonical_path.exists():
        with open(canonical_path, 'r') as f:
            return yaml.safe_load(f) or {}
    return {}

def generate_placeholders(missing_aliases, existing_teams):
    """Generate placeholder entries for missing aliases"""
    placeholders = {}
    
    for alias in missing_aliases:
        if alias not in existing_teams:
            # Generate placeholder with null conference to force manual review
            placeholders[alias] = {
                'name': alias,
                'conf': None  # Forces human to fill in correct conference
            }
    
    return placeholders

def append_to_canonical_file(placeholders):
    """Append placeholders to canonical_teams.yaml"""
    canonical_path = Path('data/canonical_teams.yaml')
    
    if placeholders:
        print(f"Adding {len(placeholders)} placeholder entries to canonical_teams.yaml")
        
        with open(canonical_path, 'a') as f:
            f.write("\n# Auto-generated placeholders - REQUIRES MANUAL CONFERENCE ASSIGNMENT\n")
            for alias, data in placeholders.items():
                f.write(f"{alias}: {{name: \"{data['name']}\", conf: null}}\n")
        
        print("Placeholders added. Please manually assign conferences and commit changes.")
        print("Teams with null conferences:")
        for alias in placeholders:
            print(f"  - {alias}")
    else:
        print("No new placeholders needed - all aliases already mapped")

def main():
    if len(sys.argv) != 2:
        print("Usage: python tools/add_placeholders.py reports/missing_aliases_YYYY.json")
        sys.exit(1)
    
    missing_file = sys.argv[1]
    
    try:
        # Load data
        missing_aliases = load_missing_aliases(missing_file)
        existing_teams = load_canonical_teams()
        
        # Generate placeholders
        placeholders = generate_placeholders(missing_aliases, existing_teams)
        
        # Append to file
        append_to_canonical_file(placeholders)
        
    except FileNotFoundError as e:
        print(f"Error: File not found - {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()