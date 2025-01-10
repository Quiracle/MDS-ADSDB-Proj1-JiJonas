import duckdb
import pandas as pd
from sklearn.linear_model import LassoCV
from sklearn.feature_selection import SelectFromModel
from sklearn.impute import SimpleImputer

def summarize_data(data):
    """Generate a summary of the dataset, including NaN counts."""
    summary = data.isna().sum().to_frame(name='NaN Count')
    summary['Percentage of NaNs'] = (summary['NaN Count'] / len(data)) * 100
    return summary

# Function to perform feature selection
def feature_selection(database_path, target_table):
    # Connect to the DuckDB database
    conn = duckdb.connect(database_path)

    # Query the data from the analytical sandbox table
    query = f"""
    SELECT 
        size, rooms, bathrooms, latitude, longitude, income_level, 
        month, year, distance_to_center, price
        FROM {target_table};
    """
    data = conn.execute(query).fetchdf()

    # Define features (X) and target (y)
    X = data.drop(columns=['price'])
    y = data['price']

    print("Summary of Missing Values in Features:")
    print(summarize_data(X))

    # Handle missing values in X
    imputer = SimpleImputer(strategy='mean')  # Replace missing values with the column mean
    X_imputed = pd.DataFrame(imputer.fit_transform(X), columns=X.columns)
    print("Original X shape:", X.shape)
    print("Imputed data shape:", imputer.fit_transform(X).shape)
    # Normalize data (optional, depending on your model's requirements)
    X_normalized = (X_imputed - X_imputed.mean()) / X_imputed.std()

    # Perform Lasso regression for feature selection
    lasso = LassoCV(cv=5).fit(X_normalized, y)
    model = SelectFromModel(lasso, prefit=True)

    # Get selected features
    selected_features = X.columns[model.get_support()]

    # Output selected features
    print("Selected Features:")
    print(selected_features.tolist())

    return selected_features.tolist()

    # Disconnect from the database
    conn.close()

def run():
    feature_selection('model_zone/model.db', 'analytical_sandbox')

if __name__ == "__main__":
    feature_selection('model_zone/model.db', 'analytical_sandbox')    