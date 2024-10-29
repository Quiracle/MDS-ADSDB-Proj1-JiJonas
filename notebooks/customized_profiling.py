import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import numpy as np

def customized_profiling(df, table_name, output_path):
    # Ensure output directory exists
    if not os.path.exists(output_path):
        os.makedirs(output_path)
    
    # Create a single HTML report file
    output_html = os.path.join(output_path, f"{table_name}_profiling_report.html")
    with open(output_html, 'w') as report_file:
        report_file.write("<html><head><title>Profiling Report</title></head><body>")
        
        # Basic statistics
        stats = df.describe(include='all').to_html()
        report_file.write(f"<h3>Basic Statistics for {table_name}</h3>{stats}")
        
        # Missing value count
        missing_count = df.isna().sum().to_frame(name='Missing Count').to_html()
        report_file.write(f"<h3>Missing Value Count for {table_name}</h3>{missing_count}")
        
        # Sum of numeric columns
        sum_stats = df.select_dtypes(include='number').sum().to_frame().T.to_html()
        report_file.write(f"<h3>Sum of Numeric Columns for {table_name}</h3>{sum_stats}")
        
        # Variance of numeric columns
        var_stats = df.select_dtypes(include='number').var().to_frame().T.to_html()
        report_file.write(f"<h3>Variance of Numeric Columns for {table_name}</h3>{var_stats}")
        
        # Correlation matrix
        numeric_df = df.select_dtypes(include='number')
        if not numeric_df.empty:
            corr_matrix = numeric_df.corr()
            plt.figure(figsize=(10, 8))
            sns.heatmap(corr_matrix, annot=True, cmap='coolwarm')
            plt.title(f'Correlation Matrix for {table_name}')
            corr_image_path = os.path.join(output_path, f"{table_name}_correlation.png")
            plt.savefig(corr_image_path)
            plt.close()
            report_file.write(f"<h3>Correlation Matrix</h3><img src='{corr_image_path}' alt='Correlation Matrix'>")
        
        # Iterate through each column for feature-wise operations
        for col in df.columns:
            report_file.write(f"<h3>Feature: {col}</h3>")
            
            # Missing value count for the feature
            missing_count_col = df[col].isna().sum()
            report_file.write(f"<p>Missing Count: {missing_count_col}</p>")
            
            # If the feature is numeric, generate sum, variance, and histogram
            if pd.api.types.is_numeric_dtype(df[col]):
                sum_col = df[col].sum()
                var_col = df[col].var()
                report_file.write(f"<p>Sum: {sum_col}</p>")
                report_file.write(f"<p>Variance: {var_col}</p>")
                
                # Histogram
                plt.figure(figsize=(8, 5))
                sns.histplot(df[col].dropna(), kde=True)
                plt.title(f'Histogram for {col} in {table_name}')
                hist_image_path = os.path.join(output_path, f"{table_name}_{col}_histogram.png")
                plt.savefig(hist_image_path)
                plt.close()
                report_file.write(f"<img src='{hist_image_path}' alt='Histogram'>")
            
            # If the feature is a string, generate value counts and histogram
            elif pd.api.types.is_string_dtype(df[col]):
                value_counts = df[col].value_counts().to_frame(name='Count').to_html()
                report_file.write(f"<h4>Value Counts for {col}</h4>{value_counts}")
                
                # Histogram for string feature (only if there are fewer unique values to avoid clutter)
                if df[col].nunique() <= 50:
                    plt.figure(figsize=(8, 5))
                    sns.countplot(y=df[col], order=df[col].value_counts().index)
                    plt.title(f'Count Plot for {col} in {table_name}')
                    count_image_path = os.path.join(output_path, f"{table_name}_{col}_countplot.png")
                    plt.savefig(count_image_path)
                    plt.close()
                    report_file.write(f"<img src='{count_image_path}' alt='Count Plot'>")
                else:
                    report_file.write(f"<p>Too many unique values to plot for {col}</p>")
        
        report_file.write("</body></html>")