import streamlit as st
import pandas as pd
import tensorflow as tf
import pickle
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
import os
from dotenv import load_dotenv
import warnings
warnings.filterwarnings("ignore", message="missing ScriptRunContext")

load_dotenv()

# =========================
# Database connection
# =========================
def get_db_engine():
    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = os.getenv("DB_PORT")
    DB_NAME = os.getenv("DB_NAME")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    
    db_url_object = URL.create(
        drivername="postgresql",
        username=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME
    )
    return create_engine(db_url_object)

def get_data_from_postgresql(table_name):
    engine = get_db_engine()
    df = pd.read_sql(f"SELECT * FROM {table_name};", engine)
    engine.dispose()
    return df

# =========================
# Load model and artifacts
# =========================
@st.cache_resource
def load_model_artifacts():
    model = tf.keras.models.load_model("best_f1_race_predictor.h5")
    with open("scaler.pkl", "rb") as f:
        scaler = pickle.load(f)
    with open("training_columns.pkl", "rb") as f:
        training_columns = pickle.load(f)
    return model, scaler, training_columns

# =========================
# Load data
# =========================
@st.cache_data
def load_data():
    df_drivers = get_data_from_postgresql('drivers')
    df_tracks = get_data_from_postgresql('tracks')
    df_constructors = get_data_from_postgresql('constructors')
    df_race_results = get_data_from_postgresql('race_results')
       
    return df_drivers, df_tracks, df_constructors, df_race_results

# =========================
# Prediction function
# =========================
def predict_position(model, scaler, training_columns, year, track, driver, constructor, starting_grid):
    user_input = pd.DataFrame([{
        'year': year,
        'track_id': track,
        'driver_id': driver,
        'constructor_id': constructor,
        'position_qual': starting_grid,
        'starting_grid': starting_grid 
    }])
    
    # One-hot encode
    user_input = pd.get_dummies(user_input, columns=['driver_id','track_id','constructor_id'])
    user_input = user_input.reindex(columns=training_columns, fill_value=0)
    
    # Scale
    user_input[['year','position_qual','starting_grid']] = scaler.transform(user_input[['year','position_qual','starting_grid']])
    
    # Predict
    y_pred = model.predict(user_input, verbose=0)
    
    return float(y_pred[0][0])

# =========================
# Streamlit App
# =========================
# Driver → Constructor mapping
driver_team_map = {
    "AA23": "AWR",
    "CS55": "AWR",
    "CL16": "SFR",
    "EO31": "HFR",
    "FA14": "AMR",
    "FC43": "ARR",
    "GB05": "KSS",
    "GR63": "MER",
    "IH06": "VCA",
    "KA12": "MER",
    "LS18": "AMR",
    "LN04": "MCL",
    "LH44": "SFR",
    "LL30": "VCA",
    "MV01": "RBR",
    "NH27": "KSS",
    "OB87": "HFR",
    "OP81": "MCL",
    "PG10": "ARR",
    "YT22": "RBR"
}

st.title("🏎️ F1 Race Position Predictor")

# Load model and data
model, scaler, training_columns = load_model_artifacts()
drivers, tracks, constructors, race_results = load_data()
# Step 1: Create mappings from IDs to names
driver_name_map = dict(zip(drivers['driver_id'], drivers['driver_name']))
track_name_map = dict(zip(tracks['track_id'], tracks['track_name']))
constructor_name_map = dict(zip(constructors['constructor_id'], constructors['constructor_name']))

# Master table: Driver name & code, Constructor name & code
df_master = pd.DataFrame(list(driver_team_map.items()), columns=['driver_code', 'constructor_code'])
df_master['constructor_name'] = df_master['constructor_code'].map(constructor_name_map)
df_master['driver_name'] = df_master['driver_code'].map(driver_name_map)
df_master = df_master[['driver_name', 'driver_code', 'constructor_name', 'constructor_code']]

# Step 1: Fixed year
year = 2025
st.text_input("Year", value=year, disabled=True)

# Step 2: Track selection using full names and get the selected track code internally
track_name = st.selectbox(
    "Select Track",
    options=[name for code, name in track_name_map.items()]  # show full names
)
track_id_selected = [code for code, name in track_name_map.items() if name == track_name][0]

# Step 3: Driver dropdown
st.subheader("Enter Starting Positions for Each Driver (1-20)")

driver_inputs = []
selected_positions = []  # keep track of already chosen positions

for i, row in df_master.iterrows():
    driver_display = f"{row['driver_name']} ({row['constructor_name']})"
    
    # Available positions are 1-20 minus already selected
    available_positions = [p for p in range(1, 21) if p not in selected_positions]
    
    # Default index is 0 if first time
    pos = st.selectbox(
        f"Starting Position - {driver_display}",
        options=available_positions,
        key=f"driver_{row['driver_code']}"
    )
    
    selected_positions.append(pos)  # mark this position as taken
    
    driver_inputs.append({
        'driver_code': row['driver_code'],
        'driver_name': row['driver_name'],
        'constructor_code': row['constructor_code'],
        'constructor_name': row['constructor_name'],
        'starting_grid': pos
    })

if st.button("Predict Race Results"):
    results = []
    for d in driver_inputs:
        pred_score = predict_position(
            model,
            scaler,
            training_columns,
            year,
            track_id_selected,
            d['driver_code'],
            d['constructor_code'],
            d['starting_grid']
        )
        results.append({
            'Driver': d['driver_name'],
            'Constructor': d['constructor_name'],
            'Starting Position': d['starting_grid'],
            'Predicted Score': pred_score  # keep as float for sorting
        })
    
    df_results = pd.DataFrame(results)
    
    # Convert to DataFrame and sort by predicted finish
    df_results = df_results.sort_values(by='Predicted Score').reset_index(drop=True)
    
    # Assign final positions 1 to 20
    df_results['Predicted Finish'] = df_results.index + 1
    
    # Calculate positions gained/lost
    df_results['Position Change'] = df_results['Starting Position'] - df_results['Predicted Finish']
    
    df_results['Predicted Score'] = pd.to_numeric(df_results['Predicted Score'], errors='coerce')
    df_results.index = range(1, len(df_results) + 1)
    
    st.subheader("Predicted Full Race Grid")
    st.dataframe(df_results, use_container_width=True)