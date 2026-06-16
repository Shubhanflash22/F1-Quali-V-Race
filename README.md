# F1-Quali-V-Race 🏎️🏁

A self-built Formula 1 data pipeline and prediction system that tracks how a driver's qualifying position translates into a race result, and uses that historical relationship to forecast finishing order for any grid. The project owns the full stack end to end: a Dockerized PostgreSQL warehouse seeded by a custom Node.js collector, a pandas/SQL feature-engineering layer, and an ensemble of gradient-boosted trees, a feed-forward neural net, and an LSTM — stacked together with a learned meta-model — exposed through a Streamlit prediction app.

## Table of Contents

* [Project Overview](#project-overview)
* [Repository Structure](#repository-structure)
* [Dataset](#dataset)
* [Architecture](#architecture)
* [Features](#features)
* [Installation](#installation)
* [Usage](#usage)
* [Methods](#methods)
* [Results](#results)
* [Future Work](#future-work)
* [License](#license)

---

## Project Overview

F1-Quali-V-Race started from a simple question: given where a driver starts, what does history say about where they'll finish — and how much can a model add on top of that baseline once it also knows the driver, the constructor, and the track?

The repo is organized as two generations of the project:

* **`F1 Project old`** is the original, fully custom build: a hand-rolled PostgreSQL schema, a Node.js scraper/loader that turns raw season CSVs into normalized tables, a pandas feature-engineering layer that derives driver/constructor/track form metrics, and a three-model ensemble (XGBoost + ANN + LSTM, stacked via a meta-regressor) trained directly against the database. A Streamlit app sits on top for interactive single-race predictions.
* **`F1 Project new`** holds two reference projects (a Fast-F1-powered race simulator and a from-scratch ML teaching app covering linear/logistic regression, KNN, k-means, and decision trees) that were pulled in as comparison points and learning material while iterating on the modeling approach above. They are vendored as-is for reference rather than authored from scratch in this repo.

This README focuses on the original pipeline in `F1 Project old`, since that's the system this repository is actually named for.

---

## Repository Structure

```
F1-Quali-V-Race/
├── F1 Project old/                          # The core, original project
│   ├── F1-Master_Database/                  # Dockerized Postgres + data collector
│   │   ├── Dockerfile
│   │   ├── docker-compose.yml                # postgres + pgAdmin + collector services
│   │   ├── init.sql                          # Table schema (constructors, drivers, tracks, results)
│   │   ├── f1_data_collector.js               # Pulls season CSVs from GitHub, loads Postgres
│   │   └── package.json
│   ├── Files/                                # Source season data (the collector's input)
│   │   ├── Formula1_2022season_*Results.csv
│   │   ├── Formula1_2023season_*Results.csv
│   │   ├── Formula1_2024season_*Results.csv
│   │   ├── Formula1_2025Season_*Results.csv
│   │   └── Unique codes {Drivers,Constructors,Tracks}.csv   # ID mapping tables
│   ├── Documentations and Processes/         # Setup guides, schema notes, SQL reference
│   │   ├── sql_queries.sql                    # Hand-written analytical queries
│   │   ├── f1_database_structure.js           # Earlier OpenF1-API-based schema design
│   │   ├── Setting up.pdf / Setup.pdf
│   │   └── Steps for Development.docx
│   └── Codes/
│       ├── Data_Ingestion and model.py        # Main pipeline: DB → features → ensemble training
│       ├── f1_streamlit_app.py                # Interactive race grid predictor
│       ├── Learings/Other data ingestion functions.py  # CSV/Excel/Ergast-API ingestion helpers
│       ├── models/                            # Saved XGBoost, ANN, LSTM weights + encoders/scaler
│       ├── ann_tuner/ , lstm_tuner/            # Keras-Tuner Bayesian search trial checkpoints
│       └── best_f1_race_predictor.h5, scaler.pkl, training_columns.pkl  # Streamlit app artifacts
│
├── F1 Project new/                           # Reference projects used for comparison/learning
│   ├── F1 Current race prediction/            # Fast-F1-based race simulator (vendored reference)
│   └── F1 Stats/                              # Streamlit app with from-scratch ML algorithms (vendored reference)
│
└── LICENSE
```

---

## Dataset

* **Race & qualifying results**: 2022–2025 F1 seasons, scraped into CSV form with columns for track, finishing/qualifying position, car number, driver, team, starting grid, laps, time/retired status, points, and fastest lap — roughly 440–480 rows per season for race years with a full calendar, and a partial set for the in-progress 2025 season.
* **ID mapping tables**: hand-built lookup CSVs (`Unique codes Drivers/Constructors/Tracks.csv`) that assign each driver, constructor, and circuit a short alphanumeric code (e.g. `MV01` for Max Verstappen, `RBR` for Red Bull Racing), used to keep the database schema compact and joins fast.
* **Storage**: a normalized PostgreSQL schema with `drivers`, `constructors`, `tracks`, `qualifying_results`, and `race_results` tables, loaded by the Node.js collector directly from the raw CSVs hosted in this same repository.

---

## Architecture

**Data flow**: raw season CSVs (`Files/`) → Node.js collector (`f1_data_collector.js`) reads them straight from this repo's GitHub raw URLs, maps driver/constructor/track names to short codes, and inserts into Postgres → Python pipeline (`Data_Ingestion and model.py`) pulls the normalized tables back out via SQLAlchemy, cleans and merges qualifying with race results, engineers features, and trains the model ensemble → trained models and the Streamlit app reload directly from the saved artifacts for inference.

**Database**: a three-container Docker Compose stack — `postgres` (the warehouse), `pgAdmin` (browser-based DB management on port 8080), and `app` (the Node collector, which waits for Postgres to be healthy before running its load job).

**Modeling**: rather than a single regressor, the pipeline trains three structurally different models on the same engineered feature set and combines them:
* **XGBoost** on a flat feature vector, tuned via `RandomizedSearchCV` over a wide hyperparameter grid (tree depth, learning rate, subsampling, L1/L2 regularization) with 5-fold cross-validation.
* **A feed-forward ANN** (Keras), whose depth, width, activation, batch normalization, dropout, and optimizer are all searched via Keras-Tuner Bayesian optimization.
* **An LSTM** that consumes a 5-race rolling sequence of each driver's form features alongside static driver/constructor/track encodings, also tuned via Bayesian search, with an optional second stacked recurrent layer.

Predictions from all three are stacked into a small feature vector and fed to a meta-model; several candidate meta-models (linear regression, ridge, random forest, gradient boosting) are cross-validated and the best one is kept.

---

## Features

**Feature Engineering**

For every driver, constructor, and track, the pipeline derives form metrics directly from historical qualifying/race pairs: the average qualifying-to-race position delta, track-specific average finishing position, DNF rate, finishing-position consistency (standard deviation), and a recency-weighted performance score that favors the last few seasons. Track-level features include average position change and a normalized overtaking-difficulty score computed from how much grid order historically gets reshuffled at that circuit.

**Data Cleaning**

Qualifying and race position fields come in as a mix of numeric finishes, `DQ`, and `NC` strings; these are normalized to numeric positions, missing race results are flagged as DNFs and back-filled with sensible placeholder finishing positions (so a DNF doesn't just disappear from the dataset), and qualifying/grid positions are cross-filled from each other when one is missing.

**Prediction Interfaces**

The trained ensemble supports two prediction modes: a custom lineup (any set of driver/constructor/starting-grid combinations for a track) and a full-grid prediction that reconstructs the most recent known lineup for a given circuit. The Streamlit app (`f1_streamlit_app.py`) wraps this in a simple UI — pick a track, assign each of the 20 drivers a unique starting position from 1–20, and get back a predicted finishing order with position-change deltas.

**Database Layer**

A from-scratch relational schema (not borrowed from an existing F1 dataset) with a working SQL query library covering qualifying-vs-race position deltas, podium counts, a simplified points table, constructor standings, best-finish-per-driver, and teammate qualifying head-to-heads — written as a reference for ad hoc analysis on top of the same Postgres instance the models train from.

---

## Installation

```bash
# 1. Clone the repository
git clone https://github.com/Shubhanflash22/F1-Quali-V-Race.git
cd F1-Quali-V-Race

# 2. Spin up the database (Postgres + pgAdmin + data collector)
cd "F1 Project old/F1-Master_Database"
docker compose up -d
# pgAdmin available at localhost:8080; Postgres at localhost:5432

# 3. Create a Python virtual environment for the modeling pipeline
cd "../Codes"
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# 4. Install Python dependencies
pip install pandas numpy sqlalchemy python-dotenv scikit-learn xgboost tensorflow keras-tuner streamlit psycopg2-binary

# 5. Configure database credentials
# Create a .env file in the Codes/ directory with:
#   DB_HOST=localhost
#   DB_PORT=5432
#   DB_NAME=f1-db
#   DB_USER=<your_postgres_user>
#   DB_PASSWORD=<your_postgres_password>
```

---

## Usage

1. **Load the database**: once the Docker stack is running, the `app` service automatically waits for Postgres, creates tables from `init.sql`, and loads all four seasons of qualifying/race CSVs plus the driver/constructor/track code mappings.
2. **Run the full training pipeline**:
   ```bash
   python "Data_Ingestion and model.py"
   ```
   This pulls data back out of Postgres, cleans and merges it, builds the engineered feature set, trains the XGBoost/ANN/LSTM ensemble with hyperparameter search, saves all model artifacts to `models/`, and runs two example predictions (a custom lineup and a full-grid forecast for Mexico).
3. **Launch the interactive predictor**:
   ```bash
   streamlit run f1_streamlit_app.py
   ```
   Select a track and assign starting grid positions for all 20 drivers to get a predicted finishing order.
4. **Explore the data directly**: connect to the Postgres instance with any SQL client (or pgAdmin at `localhost:8080`) and run the queries in `Documentations and Processes/sql_queries.sql` for standings, podium counts, and qualifying head-to-heads.

---

## Methods

**Database Design**

Two schema iterations exist in the repo: an earlier design (`f1_database_structure.js`) sketched against the OpenF1 API, storing per-race qualifying and race positions as JSON blobs, and the schema actually shipped in `init.sql`, which normalizes results into one row per driver per session with explicit `driver_code`/`constructor_code`/`track_code` foreign-key-style references — a cleaner fit for the SQL analytics layer and for feeding pandas directly.

**Sequence Construction for the LSTM**

For each driver, the pipeline sorts their historical races chronologically and builds overlapping 5-race windows of the sequential form features (qualifying/race delta, consistency, recent performance, etc.), pairing each window with the actual finishing position of the race immediately following it. Static features (driver, constructor, and track encodings plus starting grid) are attached to the final race in each window.

**Ensemble Stacking**

XGBoost and the ANN both consume the flattened, most-recent-race feature vector; the LSTM additionally sees the full 5-race sequence. All three produce a finishing-position prediction on the held-out test set, and those three predictions become the input features for a meta-regressor, which is selected by 5-fold cross-validated MAE across linear regression, ridge regression, random forest, and gradient boosting candidates.

**Inference**

`F1Predictor.predict_custom_lineup` re-derives each driver's most recent 5-race sequence from the master dataframe, overrides the constructor/track/starting-grid values with whatever the user specifies, and runs all three base models followed by the meta-model to produce a ranked finishing order. `predict_full_grid` is a convenience wrapper that automatically reconstructs the most recent known lineup for a given track instead of requiring it to be specified by hand.

---

## Results

The training script reports per-model and ensemble mean absolute error (MAE, in finishing positions) and R² on a held-out test split, along with the cross-validated MAE for each candidate meta-model so the best stacking strategy is chosen empirically rather than fixed in advance. Individual base-model MAEs (XGBoost, ANN, LSTM) are also printed alongside the final ensemble MAE so the improvement from stacking is directly visible run to run, since results depend on the hyperparameter search outcome and are not hard-coded.

**Example output shape** (values vary by run/search outcome):
```
📈 Final Results:
   XGBoost MAE: <value>
   ANN MAE: <value>
   LSTM MAE: <value>
   ─────────────────────
   Ensemble MAE: <value> ⭐
   Ensemble R²: <value>
   Improvement: <value> positions better
```

**Saved artifacts** include the tuned XGBoost model, ANN and LSTM weights, the fitted scaler and label encoders (`models/`), and a separate, already-trained Keras model (`best_f1_race_predictor.h5`) with its own scaler and training-column list specifically for the Streamlit app's one-hot-encoded inference path.

---

## Future Work

* **Live data refresh**: replace the static CSV snapshots with a scheduled job pulling each race weekend's results as soon as they're available, so the database and models stay current through a live season.
* **Richer feature set**: incorporate practice-session pace, tire strategy, and weather data (the kind of signal the reference `F1 Project new` simulator uses) directly into the core ensemble rather than keeping it as a separate comparison project.
* **Model explainability**: add SHAP-based feature attribution for the XGBoost leg of the ensemble so qualifying-to-race predictions come with an explanation of which factors (track difficulty, driver consistency, constructor recent form) drove a given forecast.
* **Automated retraining**: wire the Docker Compose stack to a CI job that retrains and redeploys the ensemble whenever new race results land in Postgres.
* **Unify the two project generations**: fold the from-scratch ML algorithm implementations and the Fast-F1 simulation features from `F1 Project new` into the core pipeline as optional model backends.

---

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
