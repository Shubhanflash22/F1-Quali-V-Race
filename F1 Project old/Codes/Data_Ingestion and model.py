"""
==============================================
F1 Race Prediction System 
==============================================

"""
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
import os
from dotenv import load_dotenv
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split, RandomizedSearchCV
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.linear_model import LinearRegression, Ridge
from xgboost import XGBRegressor
from tensorflow import keras
from tensorflow.keras import layers
from tensorflow.keras.models import Model
from tensorflow.keras.layers import Input, LSTM, Dense, Concatenate
import keras_tuner as kt
import warnings
import pickle
warnings.filterwarnings('ignore')
load_dotenv()

# ============================
# CONFIGURATION CLASS
# ============================
class F1Config:
    """Central configuration for F1 prediction system - HIGH ACCURACY VERSION."""
    
    # Database credentials
    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = os.getenv("DB_PORT")
    DB_NAME = os.getenv("DB_NAME")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    
    # Model parameters - EXPANDED
    SEQUENCE_LENGTH = 5  # CHANGE: Increased from 3 to 5 for more historical context
    TEST_SIZE = 0.2
    RANDOM_STATE = 42
    
    # Feature definitions (keep as is)
    SEQ_FEATURES = [
        'driver_quali_race_delta', 'driver_track_perf', 'driver_dnf_rate',
        'driver_consistency', 'driver_recent_perf',
        'car_quali_race_delta', 'car_race_perf', 'car_track_perf',
        'car_dnf_rate', 'car_consistency', 'car_recent_perf',
        'avg_pos_change', 'overtaking_difficulty'
    ]
    
    STATIC_FEATURES = ['driver_code_enc', 'constructor_code_enc', 'track_code_enc', 'starting_grid']
    
    # XGBoost hyperparameters - MASSIVELY EXPANDED
    XGB_PARAM_DIST = {
        'n_estimators': [100, 200, 300, 400, 500, 600, 800, 1000],  # More options
        'max_depth': [2, 3, 4, 5, 6, 7, 8],  # Wider range
        'learning_rate': [0.001, 0.005, 0.01, 0.02, 0.05, 0.1, 0.15],  # More granular
        'subsample': [0.6, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95, 1.0],  # More options
        'colsample_bytree': [0.6, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95, 1.0],
        'min_child_weight': [1, 2, 3, 4, 5],  # NEW: Prevents overfitting
        'gamma': [0, 0.01, 0.05, 0.1, 0.2, 0.5],  # NEW: Regularization
        'reg_alpha': [0, 0.01, 0.05, 0.1, 0.5, 1.0],  # NEW: L1 regularization
        'reg_lambda': [0.5, 1.0, 1.5, 2.0, 3.0],  # NEW: L2 regularization
    }
    
    # XGBoost search parameters
    XGB_N_ITER = 100  # CHANGE: Increased from 20 to 100 (5x more trials)
    XGB_CV_FOLDS = 5  # CHANGE: Increased from 3 to 5 for better validation
    
    # ANN Tuner parameters - EXPANDED
    ANN_MAX_TRIALS = 30  # CHANGE: Increased from 5 to 30 (6x more trials)
    ANN_EPOCHS = 100  # CHANGE: Increased from 50 to 100
    ANN_BATCH_SIZE = 32  # CHANGE: Increased from 16 to 32 for stability
    ANN_EARLY_STOPPING_PATIENCE = 15  # NEW: Stop if no improvement
    
    # LSTM parameters - EXPANDED
    LSTM_EPOCHS = 100  # CHANGE: Increased from 50 to 100
    LSTM_BATCH_SIZE = 32  # CHANGE: Increased from 16 to 32
    LSTM_EARLY_STOPPING_PATIENCE = 15  # NEW
    
    # Ensemble parameters
    ENSEMBLE_CV_FOLDS = 10  # NEW: Cross-validation for meta-model
    
    # Model paths
    MODEL_DIR = 'models'
    XGB_MODEL_PATH = f'{MODEL_DIR}/xgb_model.pkl'
    ANN_MODEL_PATH = f'{MODEL_DIR}/ann_model.h5'
    LSTM_MODEL_PATH = f'{MODEL_DIR}/lstm_model.h5'
    SCALER_PATH = f'{MODEL_DIR}/scaler.pkl'
    ENCODERS_PATH = f'{MODEL_DIR}/encoders.pkl'
# ============================
# DATABASE UTILITIES
# ============================
def get_db_engine():
    """Create SQLAlchemy engine for PostgreSQL connection."""
    if not all([F1Config.DB_HOST, F1Config.DB_PORT, F1Config.DB_NAME, 
                F1Config.DB_USER, F1Config.DB_PASSWORD]):
        print("❌ Database credentials are incomplete.")
        return None
    
    db_url_object = URL.create(
        drivername="postgresql",
        username=F1Config.DB_USER,
        password=F1Config.DB_PASSWORD,
        host=F1Config.DB_HOST,
        port=F1Config.DB_PORT,
        database=F1Config.DB_NAME
    )
    return create_engine(db_url_object)


def get_data_from_postgresql(table_name, query=None):
    """Fetch data from PostgreSQL with error handling."""
    engine = get_db_engine()
    if engine is None:
        return pd.DataFrame()
    
    try:
        df = pd.read_sql(query if query else f"SELECT * FROM {table_name};", engine)
        print(f"✓ Loaded {len(df)} rows from '{table_name}'")
        return df
    except Exception as e:
        print(f"❌ Error fetching data from '{table_name}': {e}")
        return pd.DataFrame()
    finally:
        engine.dispose()
# ============================
# DATA CLEANING 
# ============================
def clean_positions(df):
    """
    Clean NC/DQ and missing race positions with optimized performance.
    
    CHANGES:
    - Vectorized operations where possible
    - Reduced redundant operations
    - Clearer logic flow
    """
    df = df.copy()
    
    # Normalize strings -> NaN for position fields
    for col in ['position_qual', 'starting_grid']:
        df[col] = df[col].replace({'NC': np.nan, 'DQ': np.nan}).astype(float)
    
    df['position_race'] = pd.to_numeric(
        df['position_race'].replace({'NC': np.nan, 'DQ': np.nan}), 
        errors='coerce'
    )
    
    # Create DNF flag
    df['dnf'] = df['position_race'].isna()
    
    def fill_positions(group):
        """Fill missing positions within a race group."""
        g = group.copy()
        
        # Ensure numeric columns
        for col in ['starting_grid', 'position_qual']:
            g[col] = pd.to_numeric(g[col], errors='coerce')
        
        # Fill missing race positions
        valid_pos = g['position_race'].dropna().astype(int)
        max_pos = int(valid_pos.max()) if not valid_pos.empty else 0
        
        missing_mask = g['position_race'].isna()
        if missing_mask.sum() > 0:
            missing_sorted_idx = g.loc[missing_mask].sort_values(
                by='starting_grid', na_position='last'
            ).index
            fill_positions = list(range(max_pos + 1, max_pos + missing_mask.sum() + 1))
            g.loc[missing_sorted_idx, 'position_race'] = fill_positions
        
        g['position_race'] = g['position_race'].astype(int)
        
        # Fill qualifying and grid positions
        g['position_qual'] = g['position_qual'].fillna(g['starting_grid'])
        g['starting_grid'] = g['starting_grid'].fillna(g['position_qual'])
        
        # Final fallback
        for col in ['position_qual', 'starting_grid']:
            if g[col].isna().any():
                median = g[col].median()
                g[col] = g[col].fillna(median if not np.isnan(median) else 99)
            g[col] = g[col].astype(int)
        
        return g
    
    cleaned = df.groupby(['year', 'track_code'], group_keys=False).apply(fill_positions)
    cleaned.reset_index(drop=True, inplace=True)
    return cleaned
# ============================
# FEATURE ENGINEERING 
# ============================
def get_driver_features(df):
    """Compute driver-specific derived features with optimized groupby operations."""
    driver_df = df.copy()
    
    # Vectorized calculations
    driver_df['driver_quali_race_delta'] = (
        driver_df.groupby('driver_code')['position_race'].transform('mean') -
        driver_df.groupby('driver_code')['position_qual'].transform('mean')
    )
    
    driver_df['driver_track_perf'] = (
        driver_df.groupby(['driver_code', 'track_code'])['position_race'].transform('mean')
    )
    
    driver_df['driver_dnf_rate'] = driver_df.groupby('driver_code')['dnf'].transform('mean')
    driver_df['driver_consistency'] = driver_df.groupby('driver_code')['position_race'].transform('std')
    
    # Recent performance (weighted by year)
    max_year = driver_df['year'].max()
    driver_df['driver_recent_perf'] = (
        driver_df.groupby('driver_code')
        .apply(lambda g: np.average(g['position_race'], weights=(g['year'] - (max_year - 4)).clip(lower=1)))
        .reindex(driver_df['driver_code']).values
    )
    
    driver_df = driver_df.fillna(driver_df.median(numeric_only=True))
    return driver_df

def get_constructor_features(df):
    """Compute constructor-level derived performance features."""
    car_df = df.copy()
    
    car_df['car_quali_race_delta'] = (
        car_df.groupby('constructor_code')['position_race'].transform('mean') -
        car_df.groupby('constructor_code')['position_qual'].transform('mean')
    )
    
    car_df['car_race_perf'] = car_df.groupby('constructor_code')['position_race'].transform('mean')
    
    car_df['car_track_perf'] = (
        car_df.groupby(['constructor_code', 'track_code'])['position_race'].transform('mean')
    )
    
    if 'dnf' in car_df.columns:
        car_df['car_dnf_rate'] = car_df.groupby('constructor_code')['dnf'].transform('mean')
    else:
        car_df['car_dnf_rate'] = 0.0
    
    car_df['car_consistency'] = car_df.groupby('constructor_code')['position_race'].transform('std')
    
    # Recent performance
    max_year = car_df['year'].max()
    car_recent = (
        car_df.groupby('constructor_code')
        .apply(lambda g: np.average(g['position_race'], weights=(g['year'] - (max_year - 4)).clip(lower=1)))
        .reindex(car_df['constructor_code']).values
    )
    car_df['car_recent_perf'] = car_recent
    
    car_df = car_df.fillna(car_df.median(numeric_only=True))
    return car_df

def get_track_features(df):
    """Compute overtaking difficulty and average position change for each track."""
    track_df = df.copy()
    
    # Ensure numeric
    track_df['position_qual'] = pd.to_numeric(track_df['position_qual'], errors='coerce')
    track_df['position_race'] = pd.to_numeric(track_df['position_race'], errors='coerce')
    
    track_df['pos_change'] = track_df['position_qual'] - track_df['position_race']
    
    track_stats = (
        track_df.groupby(['year', 'track_code'])
        .agg(
            avg_pos_change=('pos_change', 'mean'),
            avg_abs_change=('pos_change', lambda x: np.mean(np.abs(x))),
            races=('driver_code', 'count')
        )
        .reset_index()
    )
    
    # Normalize overtaking difficulty
    max_change = track_stats['avg_abs_change'].max()
    track_stats['overtaking_difficulty'] = 1 - (track_stats['avg_abs_change'] / max_change)
    
    # Multi-year stability
    track_agg = (
        track_stats.groupby('track_code')[['avg_pos_change', 'overtaking_difficulty']]
        .mean()
        .reset_index()
    )
    
    track_df = pd.merge(track_df, track_agg, on='track_code', how='left')
    
    return track_df, track_agg
# ============================
# DATA PREPARATION 
# ============================
def prepare_race_features(df_clean, driver_stats, cons_stats, track_agg, track_code):
    """
    Prepare driver, constructor, and track features for a specific race.
    
    CHANGES:
    - Simplified merge logic
    - Better column handling
    - More efficient filtering
    """
    # Get race grid
    race_grid = df_clean[df_clean['track_code'] == track_code][
        ['driver_code', 'constructor_code', 'position_qual', 'starting_grid', 'track_code', 'year']
    ].drop_duplicates()
    
    # Filter features for this track
    driver_features = driver_stats[driver_stats['track_code'] == track_code].drop_duplicates()
    cons_features = cons_stats[cons_stats['track_code'] == track_code].drop_duplicates(
        subset=['constructor_code', 'track_code', 'year'], keep='first'
    )
    track_features = track_agg[track_agg['track_code'] == track_code]
    
    # Ensure consistent types
    for df in [race_grid, driver_features, cons_features]:
        df['constructor_code'] = df['constructor_code'].astype(str)
        df['driver_code'] = df['driver_code'].astype(str)
        df['track_code'] = df['track_code'].astype(str)
    
    # Merge features
    race_features = (
        race_grid
        .merge(driver_features, on=['driver_code', 'track_code', 'year'], how='left', suffixes=('', '_driver'))
        .merge(cons_features, on=['constructor_code', 'track_code', 'year'], how='left', suffixes=('', '_cons'))
    )
    
    # Add track features
    for col in track_features.columns:
        if col != 'track_code':
            race_features[col] = track_features[col].values[0]
    
    race_features = race_features.fillna(race_features.median(numeric_only=True))
    
    return race_features

def build_master_dataframe(df_clean, driver_stats, cons_stats, track_agg, df_tracks):
    """
    Build master DataFrame with all features (OPTIMIZED - CHANGE #7).
    
    CHANGES:
    - Removed duplicate column issue by cleaning after merge
    - Better column management
    """
    master_df = pd.DataFrame()
    
    print("\n📊 Building master DataFrame...")
    for track_code in df_tracks['track_code'].unique():
        race_df = prepare_race_features(df_clean, driver_stats, cons_stats, track_agg, track_code)
        
        # Clean up columns - remove duplicates and unwanted columns
        cols_to_drop = [
            'driver_code_x', 'driver_code_driver', 'driver_code_cons',
            'constructor_code_y', 'constructor_code_driver', 'constructor_code_cons',
            'year_x', 'year_driver', 'year_cons',
            'position_qual_x', 'position_qual_driver', 'position_qual_cons',
            'position_race_x', 'position_race_driver', 'position_race_cons',
            'starting_grid_x', 'starting_grid_driver', 'starting_grid_cons',
            'dnf_x', 'dnf_y', 'dnf_driver', 'dnf_cons'
        ]
        race_df = race_df.drop(columns=[c for c in cols_to_drop if c in race_df.columns], errors='ignore')
        
        # Rename remaining columns to standard names
        rename_map = {
            'driver_code_y': 'driver_code',
            'constructor_code_x': 'constructor_code',
            'position_race_y': 'position_race',
            'position_qual_y': 'position_qual',
            'starting_grid_y': 'starting_grid',
            'year_y': 'year'
        }
        race_df = race_df.rename(columns={k: v for k, v in rename_map.items() if k in race_df.columns})
        
        race_df = race_df.drop_duplicates().reset_index(drop=True)
        master_df = pd.concat([master_df, race_df], ignore_index=True)
    
    # CRITICAL FIX: Remove duplicate columns (CHANGE #8)
    master_df = master_df.loc[:, ~master_df.columns.duplicated()]
    
    print(f"✓ Master DataFrame built: {master_df.shape}")
    print(f"✓ Columns: {master_df.columns.tolist()}")
    
    return master_df
# ============================
# MODEL TRAINING
# ============================
class F1ModelTrainer:
    """Encapsulated model training logic."""
    
    def __init__(self, master_df, config=F1Config):
        self.config = config
        self.master_df = master_df.copy()
        
        # Initialize encoders (CHANGE #10 - Single source of truth)
        self.le_driver = LabelEncoder()
        self.le_cons = LabelEncoder()
        self.le_track = LabelEncoder()
        self.scaler = StandardScaler()
        
        # Models
        self.best_xgb = None
        self.best_ann = None
        self.lstm_model = None
        self.meta_model = None
        
        self._prepare_data()
    
    def _prepare_data(self):
        """Prepare training data."""
        print("\n🔧 Preparing training data...")
        
        df = self.master_df.copy()
        
        # Encode categorical features
        df['driver_code_enc'] = self.le_driver.fit_transform(df['driver_code'])
        df['constructor_code_enc'] = self.le_cons.fit_transform(df['constructor_code'])
        df['track_code_enc'] = self.le_track.fit_transform(df['track_code'])
        
        # Prepare sequences
        X_seq, y_seq, X_static, static_info = [], [], [], []
        
        df_sorted = df.sort_values(['driver_code', 'year'])
        for driver, group in df_sorted.groupby('driver_code'):
            group = group.sort_values('year')
            seq_data = group[self.config.SEQ_FEATURES].values
            static_data = group[self.config.STATIC_FEATURES].values
            y_driver = group['position_race'].values
            
            for i in range(len(group) - self.config.SEQUENCE_LENGTH + 1):
                X_seq.append(seq_data[i:i+self.config.SEQUENCE_LENGTH])
                y_seq.append(y_driver[i+self.config.SEQUENCE_LENGTH-1])
                X_static.append(static_data[i+self.config.SEQUENCE_LENGTH-1])
                static_info.append(group.iloc[i+self.config.SEQUENCE_LENGTH-1][
                    ['driver_code', 'constructor_code']
                ].to_dict())
        
        self.X_seq = np.array(X_seq)
        self.y_seq = np.array(y_seq)
        self.X_static = np.array(X_static)
        self.static_info = static_info
        
        print(f"✓ Sequences: {self.X_seq.shape}")
        print(f"✓ Static: {self.X_static.shape}")
        print(f"✓ Labels: {self.y_seq.shape}")
        
        # Split data
        (self.X_seq_train, self.X_seq_test, 
         self.X_static_train, self.X_static_test, 
         self.y_train, self.y_test,
         self.static_train_info, self.static_test_info) = train_test_split(
            self.X_seq, self.X_static, self.y_seq, self.static_info,
            test_size=self.config.TEST_SIZE, random_state=self.config.RANDOM_STATE
        )
        
        # Prepare XGBoost/ANN inputs
        self.X_xgb_train = np.hstack([self.X_seq_train[:, -1, :], self.X_static_train])
        self.X_xgb_test = np.hstack([self.X_seq_test[:, -1, :], self.X_static_test])
        
        # Standardize
        self.X_xgb_train_scaled = self.scaler.fit_transform(self.X_xgb_train)
        self.X_xgb_test_scaled = self.scaler.transform(self.X_xgb_test)
        
    def train_xgboost(self):
        """Train XGBoost model with EXTENSIVE hyperparameter tuning."""
        print("\n🚀 Training XGBoost with extensive search...")
        print(f"   Search space: {self.config.XGB_N_ITER} iterations")
        print(f"   Cross-validation: {self.config.XGB_CV_FOLDS} folds")
        
        xgb = XGBRegressor(
            random_state=self.config.RANDOM_STATE,
            tree_method='hist',  # Faster for large datasets
            enable_categorical=False
        )
        
        xgb_search = RandomizedSearchCV(
            xgb, 
            param_distributions=self.config.XGB_PARAM_DIST, 
            n_iter=self.config.XGB_N_ITER,  # 100 iterations
            scoring='neg_mean_absolute_error', 
            cv=self.config.XGB_CV_FOLDS,  # 5-fold CV
            n_jobs=-1,
            random_state=self.config.RANDOM_STATE,
            verbose=2  # Show progress
        )
        
        xgb_search.fit(self.X_xgb_train, self.y_train)
        self.best_xgb = xgb_search.best_estimator_
        
        # Show top 5 parameter combinations
        results_df = pd.DataFrame(xgb_search.cv_results_)
        top_5 = results_df.nsmallest(5, 'rank_test_score')[
            ['params', 'mean_test_score', 'std_test_score']
        ]
        
        print(f"\n✓ Best XGBoost Score: {-xgb_search.best_score_:.4f} MAE")
        print(f"✓ Best params: {xgb_search.best_params_}")
        print("\n📊 Top 5 configurations:")
        for idx, row in top_5.iterrows():
            print(f"   {-row['mean_test_score']:.4f} ± {row['std_test_score']:.4f} | {row['params']}")
        
        return self.best_xgb
    
    def train_ann(self):
        """Train ANN model with EXTENSIVE Bayesian hyperparameter search."""
        print("\n🧠 Training ANN with Bayesian optimization...")
        print(f"   Max trials: {self.config.ANN_MAX_TRIALS}")
        print(f"   Epochs per trial: {self.config.ANN_EPOCHS}")
        
        # Clear old tuner cache
        import shutil
        tuner_dir = 'ann_tuner'
        if os.path.exists(tuner_dir):
            print(f"⚠️  Removing old tuner cache at '{tuner_dir}'...")
            shutil.rmtree(tuner_dir)
        
        def build_ann_model(hp):
            model = keras.Sequential()
            model.add(layers.Input(shape=(self.X_xgb_train_scaled.shape[1],)))
            
            # EXPANDED: More architecture options
            num_layers = hp.Int('num_layers', 2, 6)  # 2-6 layers instead of 1-5
            
            for i in range(num_layers):
                # EXPANDED: Wider range of units
                units = hp.Int(f'units_{i}', 32, 256, step=32)  # 32-256 instead of 16-128
                
                # EXPANDED: More activation options
                activation = hp.Choice(f'activation_{i}', ['relu', 'tanh', 'elu', 'selu'])
                
                model.add(layers.Dense(units=units, activation=activation))
                
                # EXPANDED: Batch normalization option
                if hp.Boolean(f'batch_norm_{i}'):
                    model.add(layers.BatchNormalization())
                
                # EXPANDED: More dropout options
                dropout = hp.Float(f'dropout_{i}', 0.05, 0.5, step=0.05)  # 0.05-0.5
                model.add(layers.Dropout(rate=dropout))
            
            # Output layer
            model.add(layers.Dense(1, activation='linear'))
            
            # EXPANDED: More optimizer options
            learning_rate = hp.Float('lr', 1e-5, 1e-2, sampling='log')
            optimizer_choice = hp.Choice('optimizer', ['adam', 'adamw', 'nadam'])
            
            if optimizer_choice == 'adam':
                optimizer = keras.optimizers.Adam(learning_rate=learning_rate)
            elif optimizer_choice == 'adamw':
                optimizer = keras.optimizers.AdamW(learning_rate=learning_rate)
            else:
                optimizer = keras.optimizers.Nadam(learning_rate=learning_rate)
            
            model.compile(optimizer=optimizer, loss='mean_absolute_error', metrics=['mae'])
            return model
        
        # Use Bayesian Optimization instead of Random Search for better convergence
        tuner = kt.BayesianOptimization(  # CHANGE: Was RandomSearch
            build_ann_model,
            objective='val_loss',
            max_trials=self.config.ANN_MAX_TRIALS,  # 30 trials
            directory=tuner_dir,
            project_name='race_pred',
            overwrite=True
        )
        
        # Early stopping callback
        early_stop = keras.callbacks.EarlyStopping(
            monitor='val_loss',
            patience=self.config.ANN_EARLY_STOPPING_PATIENCE,
            restore_best_weights=True
        )
        
        # Learning rate reduction callback
        reduce_lr = keras.callbacks.ReduceLROnPlateau(
            monitor='val_loss',
            factor=0.5,
            patience=5,
            min_lr=1e-7
        )
        
        tuner.search(
            self.X_xgb_train_scaled, self.y_train,
            validation_split=0.2,
            epochs=self.config.ANN_EPOCHS,
            batch_size=self.config.ANN_BATCH_SIZE,
            callbacks=[early_stop, reduce_lr],
            verbose=0
        )
        
        self.best_ann = tuner.get_best_models(num_models=1)[0]
        
        # Show best hyperparameters
        best_hp = tuner.get_best_hyperparameters(1)[0]
        print("\n✓ Best ANN architecture:")
        print(f"   Layers: {best_hp.get('num_layers')}")
        print(f"   Learning rate: {best_hp.get('lr'):.6f}")
        print(f"   Optimizer: {best_hp.get('optimizer')}")
        
        return self.best_ann
    
    def train_lstm(self):
        """Train LSTM model with hyperparameter tuning."""
        print("\n🔄 Training LSTM with hyperparameter search...")
        
        # Clear old tuner cache
        import shutil
        tuner_dir = 'lstm_tuner'
        if os.path.exists(tuner_dir):
            shutil.rmtree(tuner_dir)
        
        def build_lstm_model(hp):
            num_seq_features = self.X_seq_train.shape[2]
            
            seq_input = Input(shape=(self.config.SEQUENCE_LENGTH, num_seq_features), name='seq_input')
            static_input = Input(shape=(self.X_static_train.shape[1],), name='static_input')
            
            # EXPANDED: LSTM architecture search
            lstm_units_1 = hp.Int('lstm_units_1', 32, 128, step=32)
            dropout_1 = hp.Float('lstm_dropout_1', 0.1, 0.4, step=0.1)
            recurrent_dropout_1 = hp.Float('lstm_recurrent_dropout_1', 0.1, 0.3, step=0.1)
            
            use_second_lstm = hp.Boolean('use_second_lstm')
            
            x = LSTM(
                lstm_units_1,
                dropout=dropout_1,
                recurrent_dropout=recurrent_dropout_1,
                return_sequences=use_second_lstm  # NEW: Option for stacked LSTM
            )(seq_input)
            
            # Optional second LSTM layer
            if use_second_lstm:
                lstm_units_2 = hp.Int('lstm_units_2', 32, 96, step=32)
                dropout_2 = hp.Float('lstm_dropout_2', 0.1, 0.4, step=0.1)
                x = LSTM(lstm_units_2, dropout=dropout_2)(x)
            else:
                # If return_sequences=True but no second LSTM, flatten
                x = layers.Flatten()(x)
            
            # Concatenate with static features
            x = Concatenate()([x, static_input])
            
            # Dense layers with search
            num_dense = hp.Int('num_dense_layers', 1, 3)
            for i in range(num_dense):
                units = hp.Int(f'dense_units_{i}', 32, 128, step=32)
                activation = hp.Choice(f'dense_activation_{i}', ['relu', 'tanh', 'elu'])
                x = Dense(units, activation=activation)(x)
                
                if hp.Boolean(f'dense_dropout_{i}'):
                    dropout = hp.Float(f'dense_dropout_rate_{i}', 0.1, 0.4, step=0.1)
                    x = layers.Dropout(dropout)(x)
            
            output = Dense(1, activation='linear')(x)
            
            model = Model(inputs=[seq_input, static_input], outputs=output)
            
            # Optimizer search
            lr = hp.Float('learning_rate', 1e-5, 1e-2, sampling='log')
            optimizer_type = hp.Choice('optimizer', ['adam', 'adamw'])
            
            if optimizer_type == 'adam':
                optimizer = keras.optimizers.Adam(learning_rate=lr)
            else:
                optimizer = keras.optimizers.AdamW(learning_rate=lr)
            
            model.compile(optimizer=optimizer, loss='mean_absolute_error', metrics=['mae'])
            return model
        
        # Bayesian optimization for LSTM
        tuner = kt.BayesianOptimization(
            build_lstm_model,
            objective='val_loss',
            max_trials=20,  # 20 trials for LSTM (fewer because it's slower)
            directory=tuner_dir,
            project_name='lstm_pred',
            overwrite=True
        )
        
        # Callbacks
        early_stop = keras.callbacks.EarlyStopping(
            monitor='val_loss',
            patience=self.config.LSTM_EARLY_STOPPING_PATIENCE,
            restore_best_weights=True
        )
        
        reduce_lr = keras.callbacks.ReduceLROnPlateau(
            monitor='val_loss',
            factor=0.5,
            patience=5,
            min_lr=1e-7
        )
        
        tuner.search(
            [self.X_seq_train, self.X_static_train], self.y_train,
            validation_split=0.2,
            epochs=self.config.LSTM_EPOCHS,
            batch_size=self.config.LSTM_BATCH_SIZE,
            callbacks=[early_stop, reduce_lr],
            verbose=0
        )
        
        self.lstm_model = tuner.get_best_models(num_models=1)[0]
        
        # Show best hyperparameters
        best_hp = tuner.get_best_hyperparameters(1)[0]
        print("\n✓ Best LSTM architecture:")
        print(f"   LSTM units: {best_hp.get('lstm_units_1')}")
        print(f"   Stacked LSTM: {best_hp.get('use_second_lstm')}")
        print(f"   Dense layers: {best_hp.get('num_dense_layers')}")
        print(f"   Learning rate: {best_hp.get('learning_rate'):.6f}")
        
        return self.lstm_model
    
    def train_ensemble(self):
        """Train all models and create ensemble with cross-validation."""
        self.train_xgboost()
        self.train_ann()
        self.train_lstm()
        
        print("\n🎯 Creating stacked ensemble with cross-validation...")
        
        # Get predictions
        xgb_pred_train = self.best_xgb.predict(self.X_xgb_train)
        ann_pred_train = self.best_ann.predict(self.X_xgb_train_scaled, verbose=0).flatten()
        lstm_pred_train = self.lstm_model.predict(
            [self.X_seq_train, self.X_static_train], verbose=0
        ).flatten()
        
        xgb_pred_test = self.best_xgb.predict(self.X_xgb_test)
        ann_pred_test = self.best_ann.predict(self.X_xgb_test_scaled, verbose=0).flatten()
        lstm_pred_test = self.lstm_model.predict(
            [self.X_seq_test, self.X_static_test], verbose=0
        ).flatten()
        
        # Stack predictions
        X_stack_train = np.vstack([xgb_pred_train, ann_pred_train, lstm_pred_train]).T
        X_stack_test = np.vstack([xgb_pred_test, ann_pred_test, lstm_pred_test]).T
        
        # NEW: Try multiple meta-models and choose the best
        from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
        from sklearn.svm import SVR
        
        meta_models = {
            'Linear Regression': LinearRegression(),
            'Ridge': Ridge(alpha=1.0),
            'Random Forest': RandomForestRegressor(n_estimators=100, max_depth=5, random_state=42),
            'Gradient Boosting': GradientBoostingRegressor(n_estimators=100, max_depth=3, random_state=42),
        }
        
        best_meta_mae = float('inf')
        best_meta_name = None
        
        print("\n📊 Testing meta-models:")
        for name, model in meta_models.items():
            from sklearn.model_selection import cross_val_score
            scores = cross_val_score(
                model, X_stack_train, self.y_train,
                cv=5, scoring='neg_mean_absolute_error', n_jobs=-1
            )
            mae = -scores.mean()
            std = scores.std()
            print(f"   {name}: {mae:.4f} ± {std:.4f} MAE")
            
            if mae < best_meta_mae:
                best_meta_mae = mae
                best_meta_name = name
                self.meta_model = model
        
        print(f"\n✓ Best meta-model: {best_meta_name}")
        
        # Train best meta-model
        self.meta_model.fit(X_stack_train, self.y_train)
        final_pred = self.meta_model.predict(X_stack_test)
        
        # Comprehensive evaluation
        mae = mean_absolute_error(self.y_test, final_pred)
        r2 = r2_score(self.y_test, final_pred)
        
        # Individual model performance
        xgb_mae = mean_absolute_error(self.y_test, xgb_pred_test)
        ann_mae = mean_absolute_error(self.y_test, ann_pred_test)
        lstm_mae = mean_absolute_error(self.y_test, lstm_pred_test)
        
        print("\n📈 Final Results:")
        print(f"   XGBoost MAE: {xgb_mae:.4f}")
        print(f"   ANN MAE: {ann_mae:.4f}")
        print(f"   LSTM MAE: {lstm_mae:.4f}")
        print("   ─────────────────────")
        print(f"   Ensemble MAE: {mae:.4f} ⭐")
        print(f"   Ensemble R²: {r2:.4f}")
        print(f"   Improvement: {min(xgb_mae, ann_mae, lstm_mae) - mae:.4f} positions better")
        
        return mae, r2
    
    def save_models(self):
        """Save all models and encoders (NEW - CHANGE #11)."""
        os.makedirs(self.config.MODEL_DIR, exist_ok=True)
        
        # Save XGBoost
        with open(self.config.XGB_MODEL_PATH, 'wb') as f:
            pickle.dump(self.best_xgb, f)
        
        # Save Keras models
        self.best_ann.save(self.config.ANN_MODEL_PATH)
        self.lstm_model.save(self.config.LSTM_MODEL_PATH)
        
        # Save scaler
        with open(self.config.SCALER_PATH, 'wb') as f:
            pickle.dump(self.scaler, f)
        
        # Save encoders
        encoders = {
            'driver': self.le_driver,
            'constructor': self.le_cons,
            'track': self.le_track
        }
        with open(self.config.ENCODERS_PATH, 'wb') as f:
            pickle.dump(encoders, f)
        
        print(f"\n💾 Models saved to '{self.config.MODEL_DIR}/'")
# ============================
# PREDICTION 
# ============================
class F1Predictor:
    """Unified prediction interface."""
    
    def __init__(self, trainer, master_df):
        self.trainer = trainer
        self.master_df = master_df.copy()
        self.config = trainer.config
        
        # Add encoded columns if not present
        if 'driver_code_enc' not in self.master_df.columns:
            self.master_df['driver_code_enc'] = trainer.le_driver.transform(
                self.master_df['driver_code']
            )
            self.master_df['constructor_code_enc'] = trainer.le_cons.transform(
                self.master_df['constructor_code']
            )
            self.master_df['track_code_enc'] = trainer.le_track.transform(
                self.master_df['track_code']
            )
    
    def predict_custom_lineup(self, custom_lineup, verbose=True):
        """
        Predict race results for a custom lineup.
        
        CHANGES:
        - Handles duplicate columns properly
        - Better error messages
        - Consistent feature extraction
        """
        X_seq, X_static, info_list = [], [], []
        
        for idx, row in custom_lineup.iterrows():
            driver = row['driver_code']
            cons = row['constructor_code']
            starting_grid = row['starting_grid']
            track_code = row.get('track_code', 'UNKNOWN')
            
            # Get historical data
            group = self.master_df[self.master_df['driver_code'] == driver].sort_values('year')
            
            if len(group) < self.config.SEQUENCE_LENGTH:
                if verbose:
                    print(f"⚠️  Skipping {driver} - insufficient history")
                continue
            
            # Get sequence data
            seq_data = group[self.config.SEQ_FEATURES].values[-self.config.SEQUENCE_LENGTH:]
            
            # Encode custom lineup values
            try:
                driver_enc = self.trainer.le_driver.transform([driver])[0]
                cons_enc = self.trainer.le_cons.transform([cons])[0]
                track_enc = self.trainer.le_track.transform([track_code])[0]
            except ValueError as e:
                if verbose:
                    print(f"⚠️  Skipping {driver} - encoding error: {e}")
                continue
            
            # Get static data from history (handles duplicate columns)
            static_data_from_history = group[self.config.STATIC_FEATURES].values[-1]
            
            # Update with custom values
            static_data = static_data_from_history.copy()
            static_data[0] = driver_enc
            static_data[1] = cons_enc
            static_data[2] = track_enc
            static_data[3] = float(starting_grid)
            # If duplicate starting_grid column exists (5th element)
            if len(static_data) > 4:
                static_data[4] = float(starting_grid)
            
            X_seq.append(seq_data)
            X_static.append(static_data)
            info_list.append({
                'driver_code': driver,
                'constructor_code': cons,
                'starting_grid': starting_grid
            })
        
        if len(X_seq) == 0:
            print("❌ No valid drivers found in custom lineup!")
            return pd.DataFrame()
        
        X_seq = np.array(X_seq)
        X_static = np.array(X_static)
        
        if verbose:
            print(f"✓ X_seq shape: {X_seq.shape}")
            print(f"✓ X_static shape: {X_static.shape}")
        
        # Prepare input
        X_input = np.hstack([X_seq[:, -1, :], X_static])
        X_input_scaled = self.trainer.scaler.transform(X_input)
        
        if verbose:
            print(f"✓ X_input shape: {X_input.shape}")
            print(f"✓ Feature count matches: {X_input.shape[1]} == {self.trainer.scaler.n_features_in_}")
        
        # Make predictions
        xgb_pred = self.trainer.best_xgb.predict(X_input)
        ann_pred = self.trainer.best_ann.predict(X_input_scaled, verbose=0).flatten()
        lstm_pred = self.trainer.lstm_model.predict([X_seq, X_static], verbose=0).flatten()
        
        # Ensemble
        X_stack = np.vstack([xgb_pred, ann_pred, lstm_pred]).T
        final_pred = self.trainer.meta_model.predict(X_stack)
        
        # Build results
        results = pd.DataFrame(info_list)
        results['predicted_position'] = final_pred
        results = results.sort_values('predicted_position').reset_index(drop=True)
        results['rank'] = results['predicted_position'].rank(method='first').astype(int)
        
        return results
    
    def predict_full_grid(self, track_code, year=None, verbose=True):
        """
        Predict results for all drivers at a specific track.
        
        NEW FEATURE - CHANGE #13
        """
        if year is None:
            year = self.master_df['year'].max()
        
        # Get all drivers who raced at this track
        track_drivers = self.master_df[
            self.master_df['track_code'] == track_code
        ]['driver_code'].unique()
        
        # Build custom lineup from historical data
        custom_lineup = []
        for driver in track_drivers:
            driver_data = self.master_df[
                (self.master_df['driver_code'] == driver) & 
                (self.master_df['track_code'] == track_code)
            ].iloc[-1]
            
            custom_lineup.append({
                'driver_code': driver,
                'constructor_code': driver_data['constructor_code'],
                'starting_grid': driver_data['starting_grid'],
                'track_code': track_code
            })
        
        custom_lineup_df = pd.DataFrame(custom_lineup)
        return self.predict_custom_lineup(custom_lineup_df, verbose=verbose)
# ============================
# MAIN EXECUTION
# ============================
"""Main execution pipeline."""

print("="*60)
print("F1 RACE PREDICTION SYSTEM - OPTIMIZED VERSION")
print("="*60)

# 1. Load data
print("\n📥 Loading data from database...")
df_constructors = get_data_from_postgresql('constructors')
df_drivers = get_data_from_postgresql('drivers')
df_tracks = get_data_from_postgresql('tracks')
df_qual = get_data_from_postgresql('qualifying_results')
df_race = get_data_from_postgresql('race_results')

# Rename columns
df_constructors.rename(columns={'constructor_id': 'constructor_code'}, inplace=True)
df_drivers.rename(columns={'driver_id': 'driver_code'}, inplace=True)
df_tracks.rename(columns={'track_id': 'track_code'}, inplace=True)

# Drop duplicates
df_qual = df_qual.drop_duplicates(
    subset=['year', 'track_code', 'driver_code', 'constructor_code', 'position'],
    keep='first'
).drop(columns=['id'], errors='ignore')

df_race = df_race.drop_duplicates(
    subset=['year', 'track_code', 'driver_code', 'constructor_code', 'position', 'starting_grid'],
    keep='first'
).drop(columns=['id'], errors='ignore')

# 2. Merge and clean
print("\n🧹 Cleaning data...")
df_merged = pd.merge(
    df_qual, df_race,
    on=['year', 'track_code', 'driver_code', 'constructor_code'],
    how='right',
    suffixes=('_qual', '_race')
)
df_clean = clean_positions(df_merged)

# 3. Feature engineering
print("\n⚙️  Engineering features...")
track_df, track_agg = get_track_features(df_clean)
driver_stats = get_driver_features(df_clean)
cons_stats = get_constructor_features(df_clean)

# 4. Build master DataFrame
master_df = build_master_dataframe(df_clean, driver_stats, cons_stats, track_agg, df_tracks)

# 5. Train models
print("\n🎓 Training models...")
trainer = F1ModelTrainer(master_df)
mae, r2 = trainer.train_ensemble()

# 6. Save models
trainer.save_models()

# 7. Make predictions
print("\n🔮 Making predictions...")
predictor = F1Predictor(trainer, master_df)

# Example: Custom lineup
custom_lineup = pd.DataFrame({
    'driver_code': ['AA23', 'CS55', 'CL16', 'LH44', 'MV01'],
    'constructor_code': ['AWR', 'AWR', 'SFR', 'SFR', 'RBR'],
    'starting_grid': [17, 12, 2, 3, 5],
    'track_code': ['MEXICO'] * 5
})

print("\n" + "="*60)
print("CUSTOM LINEUP PREDICTION")
print("="*60)
results = predictor.predict_custom_lineup(custom_lineup)
print(results.to_string(index=False))

# Example: Full grid prediction
print("\n" + "="*60)
print("FULL GRID PREDICTION - MEXICO")
print("="*60)
full_grid = predictor.predict_full_grid('MEXICO', verbose=False)
print(full_grid.head(20).to_string(index=False))

print("\n✅ Pipeline complete!")