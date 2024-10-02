import os
import textwrap

import pandas as pd
import joblib
from xgboost import XGBClassifier
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.metrics import accuracy_score, classification_report, r2_score

DATASET_PATH = './currentApproach/dataset/pausaDataset.csv'
SAMPLE_SIZE = 1000

print("Beginning data loading process", end="...")
# Load the dataset
data = pd.read_csv(
    DATASET_PATH, 
    sep=',', nrows=SAMPLE_SIZE, index_col=0
)
print("Done")

print("Beginning data preprocessing", end="...")
# Assuming the last column is the target variable
X = data.iloc[:, :-1]
y = data.iloc[:, -1]

# Split the data into training and testing sets
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

# Scale the features
scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train)
X_test_scaled = scaler.transform(X_test)
print("Done")

print("Beginning model training", end="...")
# Train the Random Forest model
rf_model = RandomForestClassifier(
    n_estimators=100, random_state=42
)
rf_model.fit(X_train_scaled, y_train)

# Train a Gradient Boosting model for comparison
gb_model = GradientBoostingClassifier(
    n_estimators=100,
    random_state=42
)
gb_model.fit(X_train_scaled, y_train)

# Train an XGBoost model as an additional efficient predictor
xgb_model = XGBClassifier(
    n_estimators=100,
    random_state=42
)
xgb_model.fit(X_train_scaled, y_train)
print("Done")

print("Beginning model evaluation", end="...")
# Make predictions on the test set
rf_pred = rf_model.predict(X_test_scaled)
gb_pred = gb_model.predict(X_test_scaled)
xgb_pred = xgb_model.predict(X_test_scaled)

# Evaluate the models
rf_accuracy = accuracy_score(y_test, rf_pred)
gb_accuracy = accuracy_score(y_test, gb_pred)
xgb_accuracy = accuracy_score(y_test, xgb_pred)
rf_r2 = r2_score(y_test, rf_pred)
gb_r2 = r2_score(y_test, gb_pred)
xgb_r2 = r2_score(y_test, xgb_pred)

print(
    f"\tRandom Forest Accuracy: {rf_accuracy:.2f}",
    f"\tRandom Forest R2 Score: {rf_r2:.2f}",
    "\n\tRandom Forest Classification Report:",
    "\033[5FDone",
    textwrap.indent(classification_report(y_test, rf_pred), "\t\t"),
    f"\n\tGradient Boosting Accuracy: {gb_accuracy:.2f}",
    f"\tGradient Boosting R2 Score: {gb_r2:.2f}",
    "\n\tGradient Boosting Classification Report:",
    textwrap.indent(classification_report(y_test, gb_pred), "\t\t"),
    f"\n\tXGBoost Accuracy: {xgb_accuracy:.2f}",
    f"\tXGBoost R2 Score: {xgb_r2:.2f}",
    "\n\tXGBoost Classification Report:",
    textwrap.indent(classification_report(y_test, xgb_pred), "\t\t"),
    sep="\n"
)

print("Beginning model and scaler saving process", end="...")
# Save the models and scaler
joblib.dump(rf_model, './models/rf_model.joblib')
joblib.dump(gb_model, './models/gb_model.joblib')
joblib.dump(xgb_model, './models/xgb_model.joblib')
joblib.dump(scaler, './models/scaler.joblib')
print("Done")