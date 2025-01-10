import duckdb
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.model_selection import train_test_split
import matplotlib.pyplot as plt
import numpy as np
import os
from script5_1 import feature_selection


# Ensure model_zone directory exists
os.makedirs("model_zone", exist_ok=True)
os.makedirs("plots/model_interpretation", exist_ok=True)

# Fetch Data from DuckDB
def fetch_data(database_path, table_name, columns, target):
    conn = duckdb.connect(database_path)
    column_str = ", ".join(columns + [target])
    query = f"SELECT {column_str} FROM {table_name} WHERE {target} IS NOT NULL;"
    data = conn.execute(query).fetchdf()
    conn.close()
    data = data.dropna()  # Remove rows with any NaN or null values

    # Remove specific column if it exists
    if 'propertyCode_93195119' in data.columns:
        data = data.drop(columns=['propertyCode_93195119'])

    return data

# Preprocess Data
def preprocess_data(data, predictors, target):
    # Ensure target is present in the dataset
    if target not in data.columns:
        raise KeyError(f"Target column '{target}' not found in the dataset.")

    # Identify categorical and datetime columns
    categorical_columns = data[predictors].select_dtypes(include=['object']).columns.tolist()
    datetime_columns = data[predictors].select_dtypes(include=['datetime64[ns]', 'datetime']).columns.tolist()

    # Remove categorical and datetime columns and log them
    print("Removing Categorical Columns:", categorical_columns)
    print("Removing Datetime Columns:", datetime_columns)
    predictors = [col for col in predictors if col not in categorical_columns + datetime_columns]

    # Return the updated data and predictors
    return data[predictors + [target]], predictors

# Train and Evaluate Models
def train_and_evaluate(X_train, X_test, y_train, y_test, use_feature_selection, suffix):
    print(f"\n{'='*20} {'With Selected Features' if use_feature_selection else 'With All Features'} {'='*20}")

    # Linear Regression
    print("\n=== Linear Regression ===")
    lr = LinearRegression()
    lr.fit(X_train, y_train)
    y_pred_lr = lr.predict(X_test)
    print("R² Score:", r2_score(y_test, y_pred_lr))
    print("MSE:", mean_squared_error(y_test, y_pred_lr))

    # Random Forest
    print("\n=== Random Forest ===")
    rf = RandomForestRegressor(n_estimators=100, random_state=42)
    rf.fit(X_train, y_train)
    y_pred_rf = rf.predict(X_test)
    print("R² Score:", r2_score(y_test, y_pred_rf))
    print("MSE:", mean_squared_error(y_test, y_pred_rf))

    # Feature Importance Analysis for Random Forest
    feature_importances = rf.feature_importances_
    features = X_train.columns

    plt.barh(features, feature_importances)
    plt.xlabel("Importance")
    plt.ylabel("Features")
    plt.title("Feature Importance - Random Forest")
    plt.savefig(f"plots/model_interpretation/feature_importance_{suffix}.png")
    plt.close()

    # Plot Regression Results
    plt.scatter(range(len(y_test)), y_test, color='blue', label='Actual Values')
    plt.scatter(range(len(y_test)), y_pred_rf, color='red', label='Predicted Values', alpha=0.5)
    plt.xlabel("Row Index")
    plt.ylabel("Target Value")
    plt.title(f"Regression Results - {suffix.capitalize()} Features")
    plt.legend()
    plt.savefig(f"plots/model_interpretation/regression_results_{suffix}.png")
    plt.close()

    return rf, y_pred_rf

# Comparison using Selected and All Features
def main():
    database_path = "model_zone/model.db"
    table_name = "analytical_sandbox"

    # Target variable
    target = "price"

    # Columns selected from feature selection
    selected_features = feature_selection('model_zone/model.db', 'analytical_sandbox')

    # Fetch data with selected features
    data_selected = fetch_data(database_path, table_name, selected_features, target)
    X_train_s, X_test_s, y_train_s, y_test_s = train_test_split(
        data_selected[selected_features], data_selected[target], test_size=0.2, random_state=42
    )

    # Train and evaluate with selected features
    rf_selected, y_pred_rf_selected = train_and_evaluate(X_train_s, X_test_s, y_train_s, y_test_s, use_feature_selection=True, suffix="selected")

    # Fetch all columns dynamically from DuckDB
    conn = duckdb.connect(database_path)
    all_columns = conn.execute(f"PRAGMA table_info({table_name});").fetchdf()["name"].tolist()
    conn.close()
    predictors = [col for col in all_columns if col != target]

    # Fetch data with all features
    data_all = fetch_data(database_path, table_name, predictors, target)

    # Preprocess data to handle categorical and datetime columns
    data_all, predictors = preprocess_data(data_all, predictors, target)

    # Train-test split
    X_train_a, X_test_a, y_train_a, y_test_a = train_test_split(
        data_all.drop(columns=[target]), data_all[target], test_size=0.2, random_state=42
    )

    # Train and evaluate with all features
    rf_all, y_pred_rf_all = train_and_evaluate(X_train_a, X_test_a, y_train_a, y_test_a, use_feature_selection=False, suffix="all")

def run():
    main()

if __name__ == "__main__":
    main()
