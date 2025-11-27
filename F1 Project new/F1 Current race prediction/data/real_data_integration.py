"""
Real F1 Data Integration using Fast-F1 library.

This module fetches real Formula 1 data from the official F1 API using Fast-F1
and integrates it with the simulation to provide more realistic race predictions.
"""

import fastf1
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Optional, Tuple
import warnings
import sys
import os

# Add utils to path for loading screen
utils_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'utils')
if utils_path not in sys.path:
    sys.path.insert(0, utils_path)

try:
    from loading_screen import suppress_fastf1_logging, loading_with_animation, show_data_loading_screen, setup_global_logging_suppression
    # Apply global suppression immediately
    setup_global_logging_suppression()
except ImportError:
    # Fallback if loading screen is not available
    from contextlib import contextmanager
    
    @contextmanager
    def suppress_fastf1_logging():
        yield
    
    def loading_with_animation(message, func, *args, **kwargs):
        print(f"🔄 {message}...")
        return func(*args, **kwargs)
    
    def show_data_loading_screen(stage="Fetching data"):
        print(f"🏎️ {stage}...")
    
    def setup_global_logging_suppression():
        pass

# Configure logging
logger = logging.getLogger(__name__)

# Enable Fast-F1 caching for better performance
cache_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'cache')
if not os.path.exists(cache_dir):
    os.makedirs(cache_dir)

# Suppress Fast-F1 logging during cache setup
with suppress_fastf1_logging():
    fastf1.Cache.enable_cache(cache_dir)


class RealDataProvider:
    """Provides real F1 data from Fast-F1 API."""
    
    def __init__(self):
        self.current_year = datetime.now().year
        # Always use the previous year for more reliable data availability
        self.last_season_year = 2024  # Use 2024 data which is more reliable
        
    def get_current_season_schedule(self) -> pd.DataFrame:
        """Get the current F1 season schedule."""
        def _fetch_schedule():
            try:
                # Try the last completed season first for more reliable data
                schedule = fastf1.get_event_schedule(self.last_season_year)
                logger.info(f"Using {self.last_season_year} season data")
                return schedule
            except Exception as e:
                logger.warning(f"Could not fetch {self.last_season_year} schedule: {e}")
                # Fallback to current year
                try:
                    schedule = fastf1.get_event_schedule(self.current_year)
                    logger.info(f"Using {self.current_year} season data as fallback")
                    return schedule
                except Exception as e2:
                    logger.error(f"Could not fetch any schedule: {e2}")
                    return pd.DataFrame()
        
        return loading_with_animation("Fetching F1 season schedule", _fetch_schedule)
    
    def get_recent_session(self, session_type: str = 'R') -> Optional[fastf1.core.Session]:
        """
        Get the most recent completed session.
        
        Args:
            session_type: 'R' for Race, 'Q' for Qualifying, 'FP1', 'FP2', 'FP3' for Practice
        """
        try:
            schedule = self.get_current_season_schedule()
            if schedule.empty:
                return None
                
            # Get completed events (past dates)
            now = pd.Timestamp.now(tz='UTC')
            completed_events = schedule[schedule['Session5DateUtc'] < now]
            
            if completed_events.empty:
                # No completed events this year, try last year
                schedule = fastf1.get_event_schedule(self.last_season_year)
                completed_events = schedule
                
            if completed_events.empty:
                return None
                
            # Get the most recent event
            latest_event = completed_events.iloc[-1]
            
            try:
                session = fastf1.get_session(
                    latest_event.name if hasattr(latest_event, 'name') else self.last_season_year,
                    latest_event['EventName'],
                    session_type
                )
                session.load()
                return session
            except Exception as e:
                logger.warning(f"Could not load recent session: {e}")
                return None
                
        except Exception as e:
            logger.error(f"Error getting recent session: {e}")
            return None
    
    def get_driver_performance_data(self, year: Optional[int] = None) -> Dict[str, Dict]:
        """
        Extract real driver performance data from recent sessions.
        
        Returns:
            Dictionary with driver performance metrics
        """
        def _fetch_driver_data():
            driver_stats = {}
            
            try:
                # Try to get data from multiple recent years for better statistics
                years_to_try = [2024, 2023, 2022]  # Try multiple years for more data
                
                for year in years_to_try:
                    try:
                        schedule = fastf1.get_event_schedule(year)
                        
                        # Get last few races for statistics
                        recent_races = schedule.tail(5)  # Last 5 races from each year
                        
                        for _, event in recent_races.iterrows():
                            try:
                                session = fastf1.get_session(year, event['EventName'], 'R')
                                session.load()
                                
                                if session.laps is None or session.laps.empty:
                                    continue
                                    
                                # Extract driver performance metrics
                                for driver_num in session.results['DriverNumber']:
                                    driver_laps = session.laps.pick_driver(driver_num)
                                    
                                    if driver_laps.empty:
                                        continue
                                        
                                    driver_info = session.get_driver(driver_num)
                                    driver_name = driver_info['Abbreviation']
                                    
                                    if driver_name not in driver_stats:
                                        driver_stats[driver_name] = {
                                            'lap_times': [],
                                            'positions': [],
                                            'consistency_scores': [],
                                            'fastest_laps': [],
                                            'overtakes': 0,
                                            'grid_positions': [],
                                            'race_positions': [],
                                            'total_races': 0
                                        }
                                    
                                    # Collect lap times (exclude outliers)
                                    valid_laps = driver_laps.pick_quicklaps()
                                    if not valid_laps.empty:
                                        lap_times = valid_laps['LapTime'].dt.total_seconds()
                                        driver_stats[driver_name]['lap_times'].extend(lap_times.tolist())
                                        
                                        # Calculate consistency (lower std deviation = more consistent)
                                        if len(lap_times) > 1:
                                            consistency = 100 - (np.std(lap_times) * 10)  # Scale to 0-100
                                            driver_stats[driver_name]['consistency_scores'].append(max(0, min(100, consistency)))
                                    
                                    # Track positions throughout race
                                    positions = driver_laps['Position'].dropna()
                                    if not positions.empty:
                                        driver_stats[driver_name]['positions'].extend(positions.tolist())
                                    
                                    # Get qualifying and race positions
                                    try:
                                        grid_pos = session.results.loc[session.results['DriverNumber'] == driver_num, 'GridPosition'].iloc[0]
                                        race_pos = session.results.loc[session.results['DriverNumber'] == driver_num, 'Position'].iloc[0]
                                        
                                        if pd.notna(grid_pos):
                                            driver_stats[driver_name]['grid_positions'].append(float(grid_pos))
                                        if pd.notna(race_pos):
                                            driver_stats[driver_name]['race_positions'].append(float(race_pos))
                                    except:
                                        pass
                                    
                                    driver_stats[driver_name]['total_races'] += 1
                                    
                            except Exception as e:
                                logger.warning(f"Could not process event {event['EventName']} in {year}: {e}")
                                continue
                                
                    except Exception as e:
                        logger.warning(f"Could not get schedule for {year}: {e}")
                        continue
                        
            except Exception as e:
                logger.error(f"Error extracting driver performance data: {e}")
                
            return driver_stats
        
        return loading_with_animation("Analyzing driver performance data", _fetch_driver_data)
    
    def get_team_performance_data(self, year: Optional[int] = None) -> Dict[str, Dict]:
        """Extract real team performance data."""
        if year is None:
            year = self.last_season_year
            
        team_stats = {}
        
        try:
            schedule = fastf1.get_event_schedule(year)
            recent_races = schedule.tail(3)
            
            for _, event in recent_races.iterrows():
                try:
                    session = fastf1.get_session(year, event['EventName'], 'R')
                    session.load()
                    
                    if session.results is None or session.results.empty:
                        continue
                        
                    for _, driver_result in session.results.iterrows():
                        team_name = driver_result['TeamName']
                        
                        if team_name not in team_stats:
                            team_stats[team_name] = {
                                'points': [],
                                'grid_positions': [],
                                'race_positions': [],
                                'reliability_issues': 0,
                                'total_races': 0,
                                'pit_stop_times': []
                            }
                        
                        # Track points and positions
                        if pd.notna(driver_result.get('Points', 0)):
                            team_stats[team_name]['points'].append(float(driver_result['Points']))
                        
                        if pd.notna(driver_result.get('GridPosition')):
                            team_stats[team_name]['grid_positions'].append(float(driver_result['GridPosition']))
                        
                        if pd.notna(driver_result.get('Position')):
                            team_stats[team_name]['race_positions'].append(float(driver_result['Position']))
                        
                        # Check for reliability issues (DNF)
                        status = driver_result.get('Status', '')
                        if 'DNF' in str(status) or 'Retired' in str(status):
                            team_stats[team_name]['reliability_issues'] += 1
                        
                        team_stats[team_name]['total_races'] += 1
                        
                except Exception as e:
                    logger.warning(f"Could not process team data for event {event['EventName']}: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error extracting team performance data: {e}")
            
        return team_stats
    
    def get_track_specific_data(self, track_name: str, year: Optional[int] = None) -> Dict:
        """Get track-specific performance data."""
        if year is None:
            year = self.last_season_year
            
        track_data = {
            'lap_times': {},
            'weather_history': [],
            'tire_strategies': {},
            'overtaking_zones': []
        }
        
        try:
            # Try to find the event for this track
            schedule = fastf1.get_event_schedule(year)
            
            # Find matching event (flexible matching)
            matching_event = None
            for _, event in schedule.iterrows():
                if track_name.lower() in event['EventName'].lower() or event['EventName'].lower() in track_name.lower():
                    matching_event = event
                    break
            
            if matching_event is None:
                logger.warning(f"Could not find event for track: {track_name}")
                return track_data
            
            # Load race session
            session = fastf1.get_session(year, matching_event['EventName'], 'R')
            session.load()
            
            if session.laps is not None and not session.laps.empty:
                # Extract lap times per driver
                for driver_num in session.results['DriverNumber']:
                    driver_laps = session.laps.pick_driver(driver_num)
                    if not driver_laps.empty:
                        driver_info = session.get_driver(driver_num)
                        driver_name = driver_info['Abbreviation']
                        
                        valid_laps = driver_laps.pick_quicklaps()
                        if not valid_laps.empty:
                            lap_times = valid_laps['LapTime'].dt.total_seconds()
                            track_data['lap_times'][driver_name] = lap_times.mean()
                
                # Extract weather data if available
                if hasattr(session, 'weather_data') and session.weather_data is not None:
                    weather = session.weather_data
                    if not weather.empty:
                        track_data['weather_history'] = {
                            'avg_temp': weather['AirTemp'].mean() if 'AirTemp' in weather.columns else 20,
                            'avg_humidity': weather['Humidity'].mean() if 'Humidity' in weather.columns else 50,
                            'rain_probability': (weather['Rainfall'].sum() > 0) if 'Rainfall' in weather.columns else False
                        }
                
        except Exception as e:
            logger.warning(f"Could not extract track data for {track_name}: {e}")
            
        return track_data
    
    def calculate_realistic_driver_ratings(self) -> Dict[str, Dict]:
        """Calculate realistic driver ratings based on real performance data."""
        driver_perf_data = self.get_driver_performance_data()
        ratings = {}
        
        for driver, stats in driver_perf_data.items():
            if stats['total_races'] == 0:
                continue
                
            # Calculate average position performance
            avg_grid_pos = np.mean(stats['grid_positions']) if stats['grid_positions'] else 10
            avg_race_pos = np.mean(stats['race_positions']) if stats['race_positions'] else 10
            
            # Position improvement (negative means better performance in race)
            position_improvement = avg_grid_pos - avg_race_pos
            
            # Consistency score
            consistency = np.mean(stats['consistency_scores']) if stats['consistency_scores'] else 75
            
            # Calculate skill ratings (scale 0-100)
            # Better average position = higher skill
            skill_dry = max(20, min(100, 100 - (avg_race_pos - 1) * 4))  # Scale position to skill
            skill_wet = skill_dry + np.random.normal(0, 5)  # Add some variation for wet
            skill_wet = max(20, min(100, skill_wet))
            
            # Overtaking skill based on position improvement
            skill_overtaking = max(20, min(100, 70 + position_improvement * 3))
            
            # Experience estimation (simplified)
            experience = min(20, stats['total_races'] // 10)  # Rough estimate
            
            # Aggression based on overtaking ability
            aggression = max(20, min(100, skill_overtaking + np.random.normal(0, 10)))
            
            ratings[driver] = {
                'skill_dry': round(skill_dry),
                'skill_wet': round(skill_wet),
                'skill_overtaking': round(skill_overtaking),
                'consistency': round(consistency),
                'experience': round(experience),
                'aggression': round(aggression),
                'total_races': stats['total_races']
            }
        
        return ratings
    
    def calculate_realistic_team_ratings(self) -> Dict[str, Dict]:
        """Calculate realistic team ratings based on real performance data."""
        team_perf_data = self.get_team_performance_data()
        ratings = {}
        
        for team, stats in team_perf_data.items():
            if stats['total_races'] == 0:
                continue
                
            # Calculate average performance metrics
            avg_points = np.mean(stats['points']) if stats['points'] else 0
            avg_grid_pos = np.mean(stats['grid_positions']) if stats['grid_positions'] else 10
            avg_race_pos = np.mean(stats['race_positions']) if stats['race_positions'] else 10
            
            # Reliability (fewer DNFs = higher reliability)
            reliability_rate = 1 - (stats['reliability_issues'] / max(1, stats['total_races']))
            reliability = max(50, min(100, reliability_rate * 100))
            
            # Performance based on points and positions
            performance = max(20, min(100, 100 - (avg_race_pos - 1) * 4))
            
            # Aerodynamics and power (estimated from performance)
            aerodynamics = performance + np.random.normal(0, 5)
            aerodynamics = max(20, min(100, aerodynamics))
            
            power = performance + np.random.normal(0, 5)
            power = max(20, min(100, power))
            
            # Pit efficiency (simplified)
            pit_efficiency = max(60, min(100, 80 + np.random.normal(0, 10)))
            
            # Development rate (simplified)
            development_rate = max(60, min(100, 75 + np.random.normal(0, 10)))
            
            ratings[team] = {
                'performance': round(performance),
                'reliability': round(reliability),
                'aerodynamics': round(aerodynamics),
                'power': round(power),
                'pit_efficiency': round(pit_efficiency),
                'development_rate': round(development_rate),
                'total_races': stats['total_races']
            }
        
        return ratings


class RealDataEnhancer:
    """Enhances simulation with real F1 data."""
    
    def __init__(self):
        self.data_provider = RealDataProvider()
        self._driver_ratings = None
        self._team_ratings = None
        
    def get_enhanced_driver_data(self) -> Dict:
        """Get driver data enhanced with real performance metrics."""
        if self._driver_ratings is None:
            self._driver_ratings = self.data_provider.calculate_realistic_driver_ratings()
        return self._driver_ratings
        
    def get_enhanced_team_data(self) -> Dict:
        """Get team data enhanced with real performance metrics."""
        if self._team_ratings is None:
            self._team_ratings = self.data_provider.calculate_realistic_team_ratings()
        return self._team_ratings
    
    def enhance_race_prediction(self, drivers, teams, track) -> Tuple[List, List]:
        """
        Enhance race prediction with real data insights.
        
        Returns:
            Tuple of (enhanced_drivers, enhanced_teams)
        """
        real_driver_data = self.get_enhanced_driver_data()
        real_team_data = self.get_enhanced_team_data()
        
        enhanced_drivers = []
        enhanced_teams = []
        
        # Enhance drivers with real data
        for driver in drivers:
            enhanced_driver = driver
            
            # Find matching real data
            driver_abbrev = self._get_driver_abbreviation(driver.name)
            if driver_abbrev in real_driver_data:
                real_data = real_driver_data[driver_abbrev]
                
                # Update driver attributes with real data (weighted average)
                weight_real = 0.7  # 70% real data, 30% original fictional data
                weight_original = 0.3
                
                enhanced_driver.skill_dry = round(
                    real_data['skill_dry'] * weight_real + driver.skill_dry * weight_original
                )
                enhanced_driver.skill_wet = round(
                    real_data['skill_wet'] * weight_real + driver.skill_wet * weight_original
                )
                enhanced_driver.skill_overtaking = round(
                    real_data['skill_overtaking'] * weight_real + driver.skill_overtaking * weight_original
                )
                enhanced_driver.consistency = round(
                    real_data['consistency'] * weight_real + driver.consistency * weight_original
                )
                enhanced_driver.aggression = round(
                    real_data['aggression'] * weight_real + driver.aggression * weight_original
                )
                
                logger.info(f"Enhanced {driver.name} with real data from {real_data['total_races']} races")
            
            enhanced_drivers.append(enhanced_driver)
        
        # Enhance teams with real data
        for team in teams:
            enhanced_team = team
            
            # Find matching real data
            team_name_clean = self._clean_team_name(team.name)
            matching_team = None
            
            for real_team_name in real_team_data.keys():
                if team_name_clean.lower() in real_team_name.lower() or real_team_name.lower() in team_name_clean.lower():
                    matching_team = real_team_name
                    break
            
            if matching_team and matching_team in real_team_data:
                real_data = real_team_data[matching_team]
                
                # Update team attributes with real data
                weight_real = 0.7
                weight_original = 0.3
                
                enhanced_team.performance = round(
                    real_data['performance'] * weight_real + team.performance * weight_original
                )
                enhanced_team.reliability = round(
                    real_data['reliability'] * weight_real + team.reliability * weight_original
                )
                enhanced_team.aerodynamics = round(
                    real_data['aerodynamics'] * weight_real + team.aerodynamics * weight_original
                )
                enhanced_team.power = round(
                    real_data['power'] * weight_real + team.power * weight_original
                )
                enhanced_team.pit_efficiency = round(
                    real_data['pit_efficiency'] * weight_real + team.pit_efficiency * weight_original
                )
                
                logger.info(f"Enhanced {team.name} with real data from {real_data['total_races']} races")
            
            enhanced_teams.append(enhanced_team)
        
        return enhanced_drivers, enhanced_teams
    
    def get_track_insights(self, track_name: str) -> Dict:
        """Get real insights for track-specific predictions."""
        return self.data_provider.get_track_specific_data(track_name)
    
    def _get_driver_abbreviation(self, full_name: str) -> str:
        """Convert full driver name to common abbreviation."""
        name_map = {
            'Max Verstappen': 'VER',
            'Lewis Hamilton': 'HAM',
            'Charles Leclerc': 'LEC',
            'George Russell': 'RUS',
            'Lando Norris': 'NOR',
            'Oscar Piastri': 'PIA',
            'Fernando Alonso': 'ALO',
            'Lance Stroll': 'STR',
            'Pierre Gasly': 'GAS',
            'Nico Hulkenberg': 'HUL',
            'Yuki Tsunoda': 'TSU',
            'Liam Lawson': 'LAW',
            'Kimi Antonelli': 'ANT',
            'Jack Doohan': 'DOO',
            'Gabriel Bortoleto': 'BOR',
            'Isack Hadjar': 'HAD'
        }
        return name_map.get(full_name, full_name[:3].upper())
    
    def _clean_team_name(self, team_name: str) -> str:
        """Clean team name for matching."""
        # Remove common suffixes and normalize
        team_name = team_name.replace(' Racing', '').replace(' F1 Team', '').replace(' Team', '')
        return team_name.strip()


# Global instance for easy access
real_data_enhancer = RealDataEnhancer()
