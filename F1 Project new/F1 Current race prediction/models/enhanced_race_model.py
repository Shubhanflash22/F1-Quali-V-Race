"""
Enhanced Race Model with Real F1 Data Integration.

This enhanced model uses real F1 performance data to improve the accuracy
of race predictions and provide more realistic results.
"""

import random
import numpy as np
from dataclasses import dataclass
from typing import List, Dict, Optional
import logging
from models.race_model import RaceSimulator, DriverRaceResult, RaceIncident, format_time

logger = logging.getLogger(__name__)


class EnhancedRaceSimulator(RaceSimulator):
    """Enhanced race simulator that uses real F1 data for more accurate predictions."""
    
    def __init__(self, track, drivers, teams, weather, real_data_enhancer=None):
        super().__init__(track, drivers, teams, weather)
        self.real_data_enhancer = real_data_enhancer
        self.track_insights = {}
        
        if real_data_enhancer:
            self.track_insights = real_data_enhancer.get_track_insights(track.name)
            logger.info(f"Loaded track insights for {track.name}")
    
    def calculate_lap_time(self, driver, team, lap_number, weather, tire_degradation=0):
        """Calculate lap time with enhanced real data consideration."""
        # Start with the base calculation
        base_time = super().calculate_lap_time(driver, team, lap_number, weather, tire_degradation)
        
        # Apply real data adjustments if available
        if self.real_data_enhancer and self.track_insights.get('lap_times'):
            driver_abbrev = self.real_data_enhancer._get_driver_abbreviation(driver.name)
            
            if driver_abbrev in self.track_insights['lap_times']:
                # Get real track lap time for this driver
                real_lap_time = self.track_insights['lap_times'][driver_abbrev]
                
                # Blend real lap time with calculated time (70% real, 30% calculated)
                adjusted_time = real_lap_time * 0.7 + base_time * 0.3
                
                # Add some variation to make it realistic
                variation = np.random.normal(0, 0.5)  # ±0.5 second variation
                final_time = adjusted_time + variation
                
                logger.debug(f"{driver.name}: Base={base_time:.3f}s, Real={real_lap_time:.3f}s, Final={final_time:.3f}s")
                return max(final_time, base_time * 0.8)  # Don't make it too fast
        
        return base_time
    
    def simulate_qualifying(self):
        """Enhanced qualifying simulation with real data consideration."""
        print("🏎️ Running enhanced qualifying simulation...")
        
        # Get base qualifying results (list of drivers)
        grid_drivers = super().simulate_qualifying()
        
        # Apply real data insights for qualifying
        if self.real_data_enhancer:
            real_driver_data = self.real_data_enhancer.get_enhanced_driver_data()
            
            # Create qualifying results with times for adjustment
            qualifying_performances = []
            
            for position, driver in enumerate(grid_drivers):
                team = self.driver_teams[driver]
                
                # Calculate qualifying time (similar to base method)
                base_time = 90 + (self.track.length_km - 5) * 5
                driver_skill = driver.get_overall_rating()
                driver_factor = 5 * (1 - (driver_skill / 100))
                team_factor = 3 * (1 - (team.get_car_rating() / 100))
                
                # Weather and track conditions
                weather_factor = 1.0
                if self.weather.condition == "wet":
                    weather_factor *= random.uniform(1.02, 1.15)
                elif self.weather.condition == "mixed":
                    weather_factor *= random.uniform(1.0, 1.08)
                
                lap_time = (base_time + driver_factor + team_factor) * weather_factor
                
                # Add qualifying variation
                q1_time = lap_time * random.uniform(1.001, 1.01)
                q2_time = lap_time * random.uniform(0.995, 1.005)
                q3_time = lap_time * random.uniform(0.99, 1.005)
                best_time = min(q1_time, q2_time, q3_time)
                
                # Apply real data adjustments
                driver_abbrev = self.real_data_enhancer._get_driver_abbreviation(driver.name)
                
                if driver_abbrev in real_driver_data:
                    real_data = real_driver_data[driver_abbrev]
                    
                    # Apply performance adjustment based on real consistency
                    consistency_factor = real_data['consistency'] / 100
                    
                    # More consistent drivers perform more predictably in qualifying
                    if consistency_factor > 0.85:  # Very consistent
                        best_time *= random.uniform(0.995, 1.002)
                    elif consistency_factor < 0.65:  # Less consistent
                        best_time *= random.uniform(0.985, 1.015)
                    
                    logger.debug(f"Qualifying adjustment for {driver.name}: consistency={consistency_factor:.2f}")
                
                qualifying_performances.append((driver, best_time))
            
            # Re-sort by adjusted times
            qualifying_performances.sort(key=lambda x: x[1])
            
            # Update grid positions with adjusted order
            self.grid_positions = [driver for driver, _ in qualifying_performances]
            
            return self.grid_positions
        
        return grid_drivers
    
    def simulate_race(self):
        """Enhanced race simulation with real data insights."""
        print("🏁 Running enhanced race simulation...")
        
        # Get base race results
        results = super().simulate_race()
        
        # Apply real data post-processing
        if self.real_data_enhancer:
            results = self._apply_real_data_race_adjustments(results)
        
        return results
    
    def _apply_real_data_race_adjustments(self, results: List[DriverRaceResult]) -> List[DriverRaceResult]:
        """Apply race adjustments based on real F1 data."""
        real_driver_data = self.real_data_enhancer.get_enhanced_driver_data()
        real_team_data = self.real_data_enhancer.get_enhanced_team_data()
        
        logger.info("Applying real data race adjustments...")
        
        for result in results:
            driver_abbrev = self.real_data_enhancer._get_driver_abbreviation(result.driver.name)
            team_name = self.real_data_enhancer._clean_team_name(result.team.name)
            
            # Driver-based adjustments
            if driver_abbrev in real_driver_data:
                real_data = real_driver_data[driver_abbrev]
                
                # Adjust based on real overtaking ability
                overtaking_skill = real_data['skill_overtaking']
                if overtaking_skill > 85 and result.starting_position > result.finishing_position:
                    # Strong overtakers might gain additional positions
                    position_boost = random.choice([0, 0, 1]) if random.random() < 0.3 else 0
                    result.finishing_position = max(1, result.finishing_position - position_boost)
                
                # Adjust based on real consistency
                consistency = real_data['consistency']
                if consistency < 70 and random.random() < 0.15:
                    # Less consistent drivers more likely to have incidents
                    if result.incident == RaceIncident.NONE:
                        result.incident = random.choice([
                            RaceIncident.DRIVER_ERROR,
                            RaceIncident.PUNCTURE,
                            RaceIncident.PIT_ERROR
                        ])
                        result.incident_description = "Real data suggests higher incident probability"
            
            # Team-based adjustments  
            matching_team = None
            for real_team_name in real_team_data.keys():
                if team_name.lower() in real_team_name.lower() or real_team_name.lower() in team_name.lower():
                    matching_team = real_team_name
                    break
            
            if matching_team and matching_team in real_team_data:
                team_data = real_team_data[matching_team]
                
                # Reliability adjustments
                reliability = team_data['reliability']
                if reliability < 75 and random.random() < 0.1:
                    # Lower reliability teams more likely to have mechanical issues
                    if result.incident == RaceIncident.NONE and random.random() < 0.3:
                        result.incident = RaceIncident.MECHANICAL_FAILURE
                        result.incident_description = "Real data suggests reliability concerns"
                        result.status = "DNF"
                        result.finishing_position = len(results)
        
        # Re-sort results to account for adjustments
        finished_results = [r for r in results if r.status == 'Finished']
        dnf_results = [r for r in results if r.status == 'DNF']
        
        # Re-assign positions for finished drivers
        finished_results.sort(key=lambda x: (x.finishing_position, x.time))
        for i, result in enumerate(finished_results):
            result.finishing_position = i + 1
        
        # DNF drivers get positions after all finished drivers
        for i, result in enumerate(dnf_results):
            result.finishing_position = len(finished_results) + i + 1
        
        all_results = finished_results + dnf_results
        
        logger.info(f"Applied real data adjustments to {len(all_results)} drivers")
        return all_results
    
    def calculate_weather_impact(self, driver, weather, base_time):
        """Enhanced weather impact calculation using real data."""
        # Start with base weather impact
        weather_impact = super().calculate_weather_impact(driver, weather, base_time)
        
        # Apply real data weather insights if available
        if self.track_insights.get('weather_history'):
            weather_history = self.track_insights['weather_history']
            
            # If historical weather data shows this track tends to be rainy
            if weather_history.get('rain_probability', False) and weather.condition in ['wet', 'mixed']:
                # Drivers with high wet skill get even more advantage on historically wet tracks
                if driver.skill_wet > 85:
                    weather_impact *= 0.8  # Even better in wet conditions
                elif driver.skill_wet < 70:
                    weather_impact *= 1.15  # Even worse in wet conditions
        
        return weather_impact
    
    def get_simulation_metadata(self) -> Dict:
        """Get metadata about the simulation including real data usage."""
        metadata = {
            'enhanced_with_real_data': self.real_data_enhancer is not None,
            'track_insights_available': bool(self.track_insights),
            'simulation_type': 'Enhanced with Real F1 Data'
        }
        
        if self.real_data_enhancer:
            real_driver_data = self.real_data_enhancer.get_enhanced_driver_data()
            real_team_data = self.real_data_enhancer.get_enhanced_team_data()
            
            metadata.update({
                'drivers_with_real_data': len(real_driver_data),
                'teams_with_real_data': len(real_team_data),
                'track_lap_times_available': len(self.track_insights.get('lap_times', {}))
            })
        
        return metadata


def create_enhanced_simulator(track, drivers, teams, weather, real_data_enhancer=None):
    """Factory function to create an enhanced race simulator."""
    if real_data_enhancer:
        return EnhancedRaceSimulator(track, drivers, teams, weather, real_data_enhancer)
    else:
        return RaceSimulator(track, drivers, teams, weather)
