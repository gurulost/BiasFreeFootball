"""
Automated ranking scheduler for Flask application
Runs weekly updates and manages current/final rankings display
"""

import logging
import schedule
import time
import threading
from datetime import datetime, timezone
from typing import Dict, Optional
from pathlib import Path
import json

from src.live_pipeline import run_current_week
from src.retro_pipeline import RetroPipeline

class RankingScheduler:
    """Manages automated ranking updates and current/final ranking state"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.is_running = False
        self.scheduler_thread = None
        
        # Schedule Monday updates at 3:30 AM ET (8:30 AM UTC)
        schedule.every().monday.at("08:30").do(self._weekly_update)
        
        # Check for season transitions daily at midnight
        schedule.every().day.at("00:00").do(self._check_season_transition)
        
    def start_scheduler(self):
        """Start the background scheduler thread"""
        if self.is_running:
            return
            
        self.is_running = True
        self.scheduler_thread = threading.Thread(target=self._run_scheduler, daemon=True)
        self.scheduler_thread.start()
        self.logger.info("Ranking scheduler started")
        
    def stop_scheduler(self):
        """Stop the background scheduler"""
        self.is_running = False
        if self.scheduler_thread:
            self.scheduler_thread.join(timeout=5)
        self.logger.info("Ranking scheduler stopped")
        
    def _run_scheduler(self):
        """Background thread for running scheduled tasks"""
        while self.is_running:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
            
    def _weekly_update(self):
        """Run weekly ranking update"""
        try:
            self.logger.info("Starting scheduled weekly ranking update")
            
            # Check if we're in football season
            if not self._is_football_season():
                self.logger.info("Off-season detected, skipping weekly update")
                return
                
            # Run current week pipeline
            current_year = datetime.now().year
            if datetime.now().month >= 8:
                season = current_year
            else:
                season = current_year - 1
                
            result = run_current_week(season=season)
            
            # Update current rankings cache
            self._update_current_rankings(result)
            
            self.logger.info("Weekly ranking update completed successfully")
            
        except Exception as e:
            self.logger.error(f"Weekly update failed: {e}")
            
    def _check_season_transition(self):
        """Check if we need to transition to final rankings"""
        try:
            if self._is_postseason_complete():
                self._generate_final_rankings()
        except Exception as e:
            self.logger.error(f"Season transition check failed: {e}")
            
    def _is_football_season(self) -> bool:
        """Check if we're currently in football season"""
        now = datetime.now()
        
        # Football season: August through January
        if now.month >= 8 or now.month <= 1:
            return True
        return False
        
    def _is_postseason_complete(self) -> bool:
        """Check if postseason (including CFP) is complete"""
        now = datetime.now()
        
        # Postseason typically ends by mid-January
        if now.month == 1 and now.day > 15:
            return True
        elif now.month >= 2:
            return True
        return False
        
    def _update_current_rankings(self, pipeline_result: Dict):
        """Update cached current rankings"""
        try:
            # Find the latest export file
            exports_dir = Path('exports')
            live_files = list(exports_dir.glob('*_live.json'))
            
            if live_files:
                latest_file = max(live_files, key=lambda p: p.stat().st_mtime)
                
                # Copy to current rankings cache
                cache_dir = Path('data/cache')
                cache_dir.mkdir(exist_ok=True)
                
                with open(latest_file, 'r') as f:
                    rankings_data = json.load(f)
                
                with open(cache_dir / 'current_rankings.json', 'w') as f:
                    json.dump(rankings_data, f, indent=2)
                    
                self.logger.info(f"Updated current rankings cache from {latest_file}")
                
        except Exception as e:
            self.logger.error(f"Failed to update current rankings cache: {e}")
            
    def _generate_final_rankings(self):
        """Generate and cache final season rankings"""
        try:
            current_year = datetime.now().year
            season = current_year - 1 if datetime.now().month <= 7 else current_year
            
            # Check if final rankings already exist
            cache_dir = Path('data/cache')
            final_cache = cache_dir / f'final_rankings_{season}.json'
            
            if final_cache.exists():
                self.logger.info(f"Final rankings for {season} already exist")
                return
                
            self.logger.info(f"Generating final rankings for {season}")
            
            # Run retro pipeline
            retro = RetroPipeline('config.yaml')
            result = retro.run_retro(season)
            
            # Find the retro export
            exports_dir = Path('exports')
            retro_files = list(exports_dir.glob(f'{season}_retro.json'))
            
            if retro_files:
                latest_retro = max(retro_files, key=lambda p: p.stat().st_mtime)
                
                cache_dir.mkdir(exist_ok=True)
                
                with open(latest_retro, 'r') as f:
                    final_data = json.load(f)
                
                # Cache final rankings
                with open(final_cache, 'w') as f:
                    json.dump(final_data, f, indent=2)
                    
                # Also update current rankings to show final during off-season
                with open(cache_dir / 'current_rankings.json', 'w') as f:
                    json.dump(final_data, f, indent=2)
                    
                self.logger.info(f"Generated and cached final rankings for {season}")
                
        except Exception as e:
            self.logger.error(f"Failed to generate final rankings: {e}")
            
    def get_current_rankings(self) -> Optional[Dict]:
        """Get current rankings from cache"""
        try:
            cache_file = Path('data/cache/current_rankings.json')
            if cache_file.exists():
                with open(cache_file, 'r') as f:
                    return json.load(f)
        except Exception as e:
            self.logger.error(f"Failed to load current rankings: {e}")
        return None
        
    def get_final_rankings(self, season: int) -> Optional[Dict]:
        """Get final rankings for a specific season"""
        try:
            cache_file = Path(f'data/cache/final_rankings_{season}.json')
            if cache_file.exists():
                with open(cache_file, 'r') as f:
                    return json.load(f)
        except Exception as e:
            self.logger.error(f"Failed to load final rankings for {season}: {e}")
        return None
        
    def manual_update(self, season: Optional[int] = None, week: Optional[int] = None) -> Dict:
        """Manually trigger ranking update"""
        try:
            if season is None:
                current_year = datetime.now().year
                season = current_year if datetime.now().month >= 8 else current_year - 1
                
            if week is None:
                result = run_current_week(season=season)
            else:
                from src.live_pipeline import run_live
                result = run_live(week=week, season=season)
                
            self._update_current_rankings(result)
            return result
            
        except Exception as e:
            self.logger.error(f"Manual update failed: {e}")
            raise

# Global scheduler instance
_scheduler = None

def get_scheduler(config: Dict = None) -> RankingScheduler:
    """Get or create the global scheduler instance"""
    global _scheduler
    
    if _scheduler is None:
        if config is None:
            import yaml
            with open('config.yaml', 'r') as f:
                config = yaml.safe_load(f)
        _scheduler = RankingScheduler(config)
        
    return _scheduler

def start_automated_updates(config: Dict = None):
    """Start automated ranking updates"""
    scheduler = get_scheduler(config)
    scheduler.start_scheduler()
    
def stop_automated_updates():
    """Stop automated ranking updates"""
    global _scheduler
    if _scheduler:
        _scheduler.stop_scheduler()