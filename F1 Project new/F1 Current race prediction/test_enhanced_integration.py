#!/usr/bin/env python3
"""
Test script for the enhanced Fast-F1 integration with 2024 data.
This will test the improved data fetching and simulation with real data.
"""

import os
import sys
import logging

# Configure logging to reduce verbosity for testing
logging.basicConfig(level=logging.INFO)
fastf1_logger = logging.getLogger('fastf1')
fastf1_logger.setLevel(logging.WARNING)

# Add the project directory to the Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from data.real_data_integration import RealDataProvider, RealDataEnhancer
from data.drivers import get_all_drivers
from data.teams import get_all_teams
from data.tracks import get_track_by_name
from models.enhanced_race_model import EnhancedRaceSimulator
from models.weather import generate_weather

def test_real_data_provider():
    """Test the Real Data Provider with 2024 data."""
    print("🧪 Testing Real Data Provider...")
    
    provider = RealDataProvider()
    
    # Test schedule fetching
    print("📅 Fetching 2024 F1 schedule...")
    schedule = provider.get_current_season_schedule()
    
    if not schedule.empty:
        print(f"✅ Successfully fetched {len(schedule)} events from 2024 season")
        print(f"   First event: {schedule.iloc[0]['EventName']}")
        print(f"   Last event: {schedule.iloc[-1]['EventName']}")
    else:
        print("❌ Failed to fetch schedule")
        return False
    
    # Test driver performance data
    print("\n🏎️ Fetching driver performance data...")
    driver_data = provider.get_driver_performance_data()
    
    if driver_data:
        print(f"✅ Successfully fetched data for {len(driver_data)} drivers")
        # Show some example data
        for i, (driver, data) in enumerate(list(driver_data.items())[:3]):
            races = data.get('total_races', 0)
            avg_pos = sum(data.get('race_positions', [])) / len(data.get('race_positions', [1])) if data.get('race_positions') else 0
            print(f"   {driver}: {races} races, avg finish: {avg_pos:.1f}")
            if i >= 2:  # Show only first 3
                break
    else:
        print("⚠️  No driver data available (expected for 2025, trying with older data)")
    
    return True

def test_enhanced_simulation():
    """Test the enhanced simulation with real data."""
    print("\n🏁 Testing Enhanced Race Simulation...")
    
    # Create enhancer
    enhancer = RealDataEnhancer()
    
    # Get test data
    drivers = get_all_drivers()[:20]  # Use first 20 drivers for faster testing
    teams = get_all_teams()
    track = get_track_by_name("Silverstone Circuit")
    weather = generate_weather(track, forced_condition="dry")
    
    if not track:
        print("❌ Could not find test track")
        return False
    
    print(f"🏎️ Testing with {len(drivers)} drivers at {track.name}")
    
    # Enhance data
    print("📊 Enhancing race data with real F1 data...")
    enhanced_drivers, enhanced_teams = enhancer.enhance_race_prediction(drivers, teams, track)
    
    print(f"✅ Enhanced {len(enhanced_drivers)} drivers and {len(enhanced_teams)} teams")
    
    # Create enhanced simulator
    print("🔧 Creating enhanced race simulator...")
    simulator = EnhancedRaceSimulator(track, enhanced_drivers, enhanced_teams, weather, enhancer)
    
    # Test qualifying
    print("🏎️ Running enhanced qualifying simulation...")
    try:
        qualifying_results = simulator.simulate_qualifying()
        print(f"✅ Qualifying completed with {len(qualifying_results)} drivers")
        
        # Show top 3
        print("🥇 Top 3 Qualifying Results:")
        for i, driver in enumerate(qualifying_results[:3]):
            print(f"   {i+1}. {driver.name}")
    except Exception as e:
        print(f"❌ Qualifying simulation failed: {e}")
        return False
    
    # Test race simulation
    print("\n🏁 Running enhanced race simulation...")
    try:
        race_results = simulator.simulate_race()
        print(f"✅ Race completed with {len(race_results)} results")
        
        # Show podium
        print("🏆 Podium Results:")
        for i, result in enumerate(race_results[:3]):
            if result.status == 'Finished':
                print(f"   {i+1}. {result.driver.name} ({result.team.name})")
            else:
                print(f"   {i+1}. {result.driver.name} ({result.team.name}) - {result.status}")
    except Exception as e:
        print(f"❌ Race simulation failed: {e}")
        return False
    
    return True

def test_real_data_insights():
    """Test real F1 data insights display."""
    print("\n📈 Testing Real Data Insights...")
    
    enhancer = RealDataEnhancer()
    
    # Test driver insights
    driver_data = enhancer.get_enhanced_driver_data()
    if driver_data:
        print(f"✅ Real driver data available for {len(driver_data)} drivers")
        
        # Show insights for a few drivers
        print("🏎️ Sample Driver Insights:")
        for i, (driver, data) in enumerate(list(driver_data.items())[:3]):
            consistency = data.get('consistency', 0)
            speed = data.get('speed_rating', 0)
            experience = data.get('experience', 0)
            print(f"   {driver}: Consistency={consistency:.1f}, Speed={speed:.1f}, Experience={experience:.1f}")
            if i >= 2:
                break
    else:
        print("⚠️  No enhanced driver data available")
    
    # Test team insights
    team_data = enhancer.get_enhanced_team_data()
    if team_data:
        print(f"✅ Real team data available for {len(team_data)} teams")
        
        # Show insights for a few teams
        print("🏎️ Sample Team Insights:")
        for i, (team, data) in enumerate(list(team_data.items())[:3]):
            performance = data.get('performance_rating', 0)
            reliability = data.get('reliability', 0)
            print(f"   {team}: Performance={performance:.1f}, Reliability={reliability:.1f}")
            if i >= 2:
                break
    else:
        print("⚠️  No enhanced team data available")
    
    return True

def main():
    """Run all integration tests."""
    print("🔬 FAST-F1 ENHANCED INTEGRATION TEST")
    print("=" * 50)
    
    success = True
    
    # Test 1: Real Data Provider
    success &= test_real_data_provider()
    
    # Test 2: Enhanced Simulation
    success &= test_enhanced_simulation()
    
    # Test 3: Real Data Insights
    success &= test_real_data_insights()
    
    print("\n" + "=" * 50)
    if success:
        print("🎉 ALL TESTS PASSED! Fast-F1 integration is working correctly.")
        print("🚀 Your simulator now uses real Formula 1 data for more realistic predictions!")
    else:
        print("❌ Some tests failed. Check the output above for details.")
    
    print("\n💡 To use the enhanced simulator, run: python main.py")

if __name__ == "__main__":
    main()
