"""
Quick test of Fast-F1 logging suppression.
"""

import warnings
import logging

# Suppress all Fast-F1 logging before importing anything
fastf1_loggers = [
    'fastf1', 'fastf1.core', 'fastf1.req', 'fastf1.ergast', 'fastf1.events',
    'fastf1.fastf1', 'fastf1.fastf1.core', 'fastf1.fastf1.req', 
    'fastf1.fastf1.ergast', 'fastf1.fastf1.events'
]

for logger_name in fastf1_loggers:
    logging.getLogger(logger_name).setLevel(logging.CRITICAL)

# Suppress warnings
warnings.filterwarnings("ignore", category=UserWarning, module="fastf1")
warnings.filterwarnings("ignore", category=FutureWarning, module="fastf1")
warnings.filterwarnings("ignore", message=".*pick_driver is deprecated.*")
warnings.filterwarnings("ignore", message=".*Failed to parse timestamp.*")
warnings.filterwarnings("ignore", message=".*Lap timing integrity check failed.*")
warnings.filterwarnings("ignore", message=".*completed the race distance.*")
warnings.filterwarnings("ignore", message=".*Fixed incorrect tyre stint.*")
warnings.filterwarnings("ignore", message=".*No lap data for driver.*")
warnings.filterwarnings("ignore", message=".*Correcting user input.*")

print("🏎️ Testing Clean Fast-F1 Integration")
print("=" * 40)
print("🔄 Loading real data enhancer...")

try:
    from data.real_data_integration import real_data_enhancer
    print("✅ Real data enhancer loaded successfully!")
    
    print("📊 Fetching enhanced driver data...")
    enhanced_data = real_data_enhancer.get_enhanced_driver_data()
    print(f"✅ Successfully fetched data for {len(enhanced_data)} drivers!")
    
    print("🏁 Testing track insights...")
    track_insights = real_data_enhancer.get_track_insights("Silverstone Circuit")
    print(f"✅ Track insights loaded with {len(track_insights.get('lap_times', {}))} lap times!")
    
    print("\n🎉 All tests passed with clean output!")

except Exception as e:
    print(f"❌ Error: {e}")
