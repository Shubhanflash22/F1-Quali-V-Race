"""
Test script for Real F1 Data Integration.

This script tests the Fast-F1 integration and verifies that real data
can be fetched and processed correctly.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from data.real_data_integration import RealDataProvider, real_data_enhancer
from data.drivers import get_all_drivers
from data.teams import get_all_teams
import logging

# Configure logging for testing
logging.basicConfig(level=logging.INFO)

def test_real_data_integration():
    """Test the real data integration functionality."""
    print("🧪 Testing Real F1 Data Integration")
    print("=" * 50)
    
    # Test 1: Initialize RealDataProvider
    print("\n1. Testing RealDataProvider initialization...")
    try:
        provider = RealDataProvider()
        print("✅ RealDataProvider initialized successfully")
        print(f"   Current year: {provider.current_year}")
        print(f"   Last season year: {provider.last_season_year}")
    except Exception as e:
        print(f"❌ Failed to initialize RealDataProvider: {e}")
        return False
    
    # Test 2: Get season schedule
    print("\n2. Testing season schedule retrieval...")
    try:
        schedule = provider.get_current_season_schedule()
        if not schedule.empty:
            print("✅ Season schedule retrieved successfully")
            print(f"   Found {len(schedule)} events")
            print(f"   Latest event: {schedule.iloc[-1]['EventName'] if len(schedule) > 0 else 'None'}")
        else:
            print("⚠️  Season schedule is empty (may be expected for future seasons)")
    except Exception as e:
        print(f"❌ Failed to get season schedule: {e}")
    
    # Test 3: Get driver performance data
    print("\n3. Testing driver performance data...")
    try:
        driver_data = provider.get_driver_performance_data()
        if driver_data:
            print("✅ Driver performance data retrieved successfully")
            print(f"   Found data for {len(driver_data)} drivers")
            
            # Show sample data
            sample_drivers = list(driver_data.keys())[:3]
            for driver in sample_drivers:
                data = driver_data[driver]
                print(f"   {driver}: {data['total_races']} races analyzed")
        else:
            print("⚠️  No driver performance data available")
    except Exception as e:
        print(f"❌ Failed to get driver performance data: {e}")
    
    # Test 4: Get team performance data
    print("\n4. Testing team performance data...")
    try:
        team_data = provider.get_team_performance_data()
        if team_data:
            print("✅ Team performance data retrieved successfully")
            print(f"   Found data for {len(team_data)} teams")
            
            # Show sample data
            sample_teams = list(team_data.keys())[:3]
            for team in sample_teams:
                data = team_data[team]
                print(f"   {team}: {data['total_races']} races analyzed")
        else:
            print("⚠️  No team performance data available")
    except Exception as e:
        print(f"❌ Failed to get team performance data: {e}")
    
    # Test 5: Test enhancement functionality
    print("\n5. Testing data enhancement...")
    try:
        drivers = get_all_drivers()
        teams = get_all_teams()
        
        enhanced_drivers, enhanced_teams = real_data_enhancer.enhance_race_prediction(drivers, teams, None)
        
        print("✅ Data enhancement completed successfully")
        print(f"   Enhanced {len(enhanced_drivers)} drivers")
        print(f"   Enhanced {len(enhanced_teams)} teams")
        
        # Show some enhancement examples
        print("\n   Sample Driver Enhancements:")
        for i, driver in enumerate(enhanced_drivers[:3]):
            print(f"   {driver.name}: Dry={driver.skill_dry}, Wet={driver.skill_wet}, Consistency={driver.consistency}")
            
    except Exception as e:
        print(f"❌ Failed to enhance data: {e}")
    
    # Test 6: Test realistic ratings calculation
    print("\n6. Testing realistic ratings calculation...")
    try:
        realistic_driver_ratings = provider.calculate_realistic_driver_ratings()
        realistic_team_ratings = provider.calculate_realistic_team_ratings()
        
        print("✅ Realistic ratings calculated successfully")
        print(f"   Driver ratings: {len(realistic_driver_ratings)} drivers")
        print(f"   Team ratings: {len(realistic_team_ratings)} teams")
        
    except Exception as e:
        print(f"❌ Failed to calculate realistic ratings: {e}")
    
    print("\n" + "=" * 50)
    print("🎯 Real F1 Data Integration Test Complete!")
    
    return True


if __name__ == "__main__":
    test_real_data_integration()
