import os

import pandas as pd
import matplotlib.pyplot as plt

DATASET_FILE = './1_PAUSA.csv'

# Load the dataset
df = pd.read_csv(DATASET_FILE)

# Display basic information about the dataset
print(df.info())

# Show the first few rows of the dataset
print(df.head())

# Generate summary statistics
print(df.describe())

# Check for missing values
print(df.isnull().sum())

# Plot a histogram of a numerical column (replace 'column_name' with an actual column name)
plt.figure(figsize=(10, 6))
df['column_name'].hist()
plt.title('Histogram of column_name')
plt.xlabel('Value')
plt.ylabel('Frequency')
plt.show()

# Create a correlation matrix
correlation_matrix = df.corr()
plt.figure(figsize=(12, 10))
plt.imshow(correlation_matrix, cmap='coolwarm')
plt.colorbar()
plt.xticks(range(len(correlation_matrix.columns)), correlation_matrix.columns, rotation=90)
plt.yticks(range(len(correlation_matrix.columns)), correlation_matrix.columns)
plt.title('Correlation Matrix')
plt.show()

# Group by a categorical column and calculate mean of a numerical column
grouped_data = df.groupby('categorical_column')['numerical_column'].mean()
print(grouped_data)
