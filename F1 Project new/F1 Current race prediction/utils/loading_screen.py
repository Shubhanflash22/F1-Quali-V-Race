"""
Loading screen utilities for the F1 Race Prediction Simulator.

Provides clean loading animations and progress indicators while
suppressing verbose logging output from Fast-F1 library.
"""

import time
import sys
import threading
import logging
import warnings
from contextlib import contextmanager


class LoadingAnimation:
    """A simple loading animation with rotating spinner."""
    
    def __init__(self, message="Loading"):
        self.message = message
        self.is_running = False
        self.spinner_chars = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏']
        self.current_char = 0
        
    def _animate(self):
        """Run the loading animation."""
        while self.is_running:
            char = self.spinner_chars[self.current_char]
            sys.stdout.write(f'\r{char} {self.message}...')
            sys.stdout.flush()
            self.current_char = (self.current_char + 1) % len(self.spinner_chars)
            time.sleep(0.1)
    
    def start(self):
        """Start the loading animation."""
        self.is_running = True
        self.thread = threading.Thread(target=self._animate)
        self.thread.daemon = True
        self.thread.start()
    
    def stop(self, success_message=None):
        """Stop the loading animation."""
        self.is_running = False
        if hasattr(self, 'thread'):
            self.thread.join(timeout=0.2)
        
        # Clear the line and show completion message
        sys.stdout.write('\r' + ' ' * (len(self.message) + 10) + '\r')
        if success_message:
            print(f"✅ {success_message}")
        sys.stdout.flush()


@contextmanager
def suppress_fastf1_logging():
    """Context manager to suppress Fast-F1 logging and warnings."""
    # Save original logging levels and handlers
    original_levels = {}
    original_handlers = {}
    original_warnings = warnings.filters.copy()
    original_root_level = logging.getLogger().level
    
    # More comprehensive list of Fast-F1 related loggers
    fastf1_loggers = [
        'fastf1',
        'fastf1.core',
        'fastf1.req', 
        'fastf1.ergast',
        'fastf1.events',
        'fastf1.fastf1',
        'fastf1.fastf1.core',
        'fastf1.fastf1.req',
        'fastf1.fastf1.ergast',
        'fastf1.fastf1.events',
        'req',  # This is the one causing issues
        'core',  # This too
        'ergast'
    ]
    
    try:
        # Set root logger to WARNING to catch any unhandled logs
        logging.getLogger().setLevel(logging.WARNING)
        
        # Suppress all Fast-F1 logging
        for logger_name in fastf1_loggers:
            logger = logging.getLogger(logger_name)
            original_levels[logger_name] = logger.level
            original_handlers[logger_name] = logger.handlers.copy()
            
            # Set to CRITICAL and remove all handlers
            logger.setLevel(logging.CRITICAL)
            logger.handlers.clear()
            logger.propagate = False
        
        # Suppress warnings comprehensively
        warnings.filterwarnings("ignore")
        warnings.filterwarnings("ignore", category=UserWarning)
        warnings.filterwarnings("ignore", category=FutureWarning)
        warnings.filterwarnings("ignore", category=DeprecationWarning)
        warnings.filterwarnings("ignore", message=".*pick_driver is deprecated.*")
        warnings.filterwarnings("ignore", message=".*Failed to parse timestamp.*")
        warnings.filterwarnings("ignore", message=".*Lap timing integrity check failed.*")
        warnings.filterwarnings("ignore", message=".*completed the race distance.*")
        warnings.filterwarnings("ignore", message=".*Fixed incorrect tyre stint.*")
        warnings.filterwarnings("ignore", message=".*No lap data for driver.*")
        warnings.filterwarnings("ignore", message=".*Correcting user input.*")
        warnings.filterwarnings("ignore", message=".*Using cached data.*")
        warnings.filterwarnings("ignore", message=".*Processing timing data.*")
        warnings.filterwarnings("ignore", message=".*Loading data for.*")
        warnings.filterwarnings("ignore", message=".*Finished loading data.*")
        
        yield
        
    finally:
        # Restore original logging levels and handlers
        for logger_name in fastf1_loggers:
            logger = logging.getLogger(logger_name)
            if logger_name in original_levels:
                logger.setLevel(original_levels[logger_name])
            if logger_name in original_handlers:
                logger.handlers = original_handlers[logger_name]
            logger.propagate = True
        
        # Restore root logger level
        logging.getLogger().setLevel(original_root_level)
        
        # Restore warnings
        warnings.filters = original_warnings


class ProgressBar:
    """A simple progress bar for showing completion percentage."""
    
    def __init__(self, total, width=50, prefix="Progress"):
        self.total = total
        self.width = width
        self.prefix = prefix
        self.current = 0
    
    def update(self, current=None):
        """Update the progress bar."""
        if current is not None:
            self.current = current
        else:
            self.current += 1
        
        # Calculate percentage
        percent = (self.current / self.total) * 100
        filled_width = int(self.width * self.current // self.total)
        
        # Create the bar
        bar = '█' * filled_width + '░' * (self.width - filled_width)
        
        # Display the progress bar
        sys.stdout.write(f'\r{self.prefix}: |{bar}| {percent:.1f}% ({self.current}/{self.total})')
        sys.stdout.flush()
        
        if self.current >= self.total:
            print()  # New line when complete


def show_data_loading_screen(stage="Fetching real F1 data"):
    """Show a professional loading screen for data operations."""
    print(f"\n🏎️  {stage.upper()}")
    print("=" * 50)
    print("Please wait while we fetch and process real Formula 1 data...")
    print("This may take a moment for the first run.")
    print("-" * 50)


def loading_with_animation(message, func, *args, **kwargs):
    """
    Execute a function with a loading animation.
    
    Args:
        message: Message to display during loading
        func: Function to execute
        *args, **kwargs: Arguments to pass to the function
    
    Returns:
        The result of the function call
    """
    animation = LoadingAnimation(message)
    
    try:
        animation.start()
        with suppress_fastf1_logging():
            result = func(*args, **kwargs)
        animation.stop(f"{message} completed")
        return result
    except Exception as e:
        animation.stop()
        print(f"❌ Error during {message.lower()}: {e}")
        raise


def loading_with_steps(steps, func_list):
    """
    Execute multiple functions with step-by-step progress display.
    
    Args:
        steps: List of step descriptions
        func_list: List of functions to execute
    
    Returns:
        List of results from each function
    """
    if len(steps) != len(func_list):
        raise ValueError("Number of steps must match number of functions")
    
    results = []
    total_steps = len(steps)
    
    print(f"\n🔄 Processing {total_steps} steps...")
    print("-" * 40)
    
    for i, (step, func) in enumerate(zip(steps, func_list), 1):
        print(f"Step {i}/{total_steps}: {step}")
        
        animation = LoadingAnimation(f"Step {i}")
        animation.start()
        
        try:
            with suppress_fastf1_logging():
                if callable(func):
                    result = func()
                else:
                    # If it's a tuple of (function, args, kwargs)
                    if isinstance(func, tuple):
                        if len(func) == 2:
                            result = func[0](*func[1])
                        elif len(func) == 3:
                            result = func[0](*func[1], **func[2])
                        else:
                            result = func[0]()
                    else:
                        result = func
            
            animation.stop(f"Step {i} completed")
            results.append(result)
            
        except Exception as e:
            animation.stop()
            print(f"❌ Error in step {i}: {e}")
            raise
    
    print("✅ All steps completed successfully!")
    return results


def setup_global_logging_suppression():
    """Set up global logging suppression for Fast-F1 to prevent any verbose output."""
    # Comprehensive list of loggers to suppress
    loggers_to_suppress = [
        'fastf1',
        'fastf1.core',
        'fastf1.req', 
        'fastf1.ergast',
        'fastf1.events',
        'fastf1.fastf1',
        'fastf1.fastf1.core',
        'fastf1.fastf1.req',
        'fastf1.fastf1.ergast',
        'fastf1.fastf1.events',
        'req',
        'core',
        'ergast'
    ]
    
    # Set all Fast-F1 loggers to CRITICAL level
    for logger_name in loggers_to_suppress:
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.CRITICAL)
        logger.propagate = False
    
    # Suppress all Fast-F1 warnings globally
    warnings.filterwarnings("ignore", module="fastf1")
    warnings.filterwarnings("ignore", message=".*pick_driver is deprecated.*")
    warnings.filterwarnings("ignore", message=".*Failed to parse timestamp.*")
    warnings.filterwarnings("ignore", message=".*Using cached data.*")
    warnings.filterwarnings("ignore", message=".*Processing timing data.*")
    warnings.filterwarnings("ignore", message=".*Loading data for.*")
    warnings.filterwarnings("ignore", message=".*Finished loading data.*")
    warnings.filterwarnings("ignore", message=".*completed the race distance.*")
