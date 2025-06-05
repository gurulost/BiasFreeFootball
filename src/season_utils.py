"""
Season utilities for intelligent pipeline routing
Handles between-season logic and automatic fallback to definitive rankings
"""

from datetime import datetime, date
from typing import Tuple, Optional

def get_current_season_info() -> Tuple[int, bool, Optional[int]]:
    """
    Determine current season, if it's active, and current week
    
    Returns:
        (season_year, is_active, current_week)
    """
    now = datetime.now()
    current_year = now.year
    current_month = now.month
    current_day = now.day
    
    # College football season runs roughly August - January
    # Season year is based on when the season starts (fall year)
    
    if current_month >= 8:  # August - December
        season_year = current_year
        is_season_active = True
        
        # Estimate current week (rough approximation)
        if current_month == 8:
            current_week = 1 if current_day >= 25 else None
        elif current_month == 9:
            current_week = min(5, (current_day // 7) + 1)
        elif current_month == 10:
            current_week = min(9, ((current_day + 30) // 7) - 3)
        elif current_month == 11:
            current_week = min(13, ((current_day + 61) // 7) - 7)
        elif current_month == 12:
            if current_day <= 15:
                current_week = 15  # Conference championships/bowl season
                is_season_active = True
            else:
                # Bowl games/CFP
                current_week = None
                is_season_active = False
        else:
            current_week = None
            
    elif current_month <= 1:  # January
        season_year = current_year - 1  # Previous year's season
        is_season_active = current_day <= 20  # CFP usually ends by Jan 20
        current_week = None
        
    else:  # February - July (off-season)
        season_year = current_year - 1  # Previous completed season
        is_season_active = False
        current_week = None
    
    return season_year, is_season_active, current_week

def should_use_retro_rankings() -> bool:
    """
    Determine if we should use RETRO (definitive) rankings instead of live
    
    Returns:
        True if between seasons (should use RETRO), False if active season
    """
    _, is_active, _ = get_current_season_info()
    return not is_active

def get_pipeline_recommendation() -> dict:
    """
    Get recommended pipeline based on current date
    
    Returns:
        Dictionary with pipeline recommendation and reasoning
    """
    season_year, is_active, current_week = get_current_season_info()
    
    if is_active and current_week:
        return {
            'pipeline': 'live',
            'season': season_year,
            'week': current_week,
            'reason': f'Active season - Week {current_week} of {season_year} season',
            'description': 'Live weekly rankings with current game data'
        }
    elif is_active and not current_week:
        return {
            'pipeline': 'retro',
            'season': season_year,
            'week': None,
            'reason': f'Bowl/playoff season {season_year} - use definitive rankings',
            'description': 'RETRO pipeline with complete season data'
        }
    else:
        return {
            'pipeline': 'retro',
            'season': season_year,
            'week': None,
            'reason': f'Off-season - show definitive {season_year} season rankings',
            'description': 'Final RETRO rankings from completed season'
        }

def get_season_status_message() -> str:
    """Get user-friendly season status message"""
    season_year, is_active, current_week = get_current_season_info()
    
    if is_active and current_week:
        return f"Active Season: Week {current_week} of {season_year}"
    elif is_active:
        return f"Bowl/Playoff Season: {season_year}"
    else:
        return f"Off-Season: Showing {season_year} Final Rankings"