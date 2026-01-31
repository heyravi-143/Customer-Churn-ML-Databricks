Customer Churn Prediction – Databricks ML Project
📌 Project Overview

This project demonstrates an end-to-end Customer Churn Prediction system built using Databricks, PySpark, MLflow, and Delta Lake.

The objective is to identify customers who are likely to churn, understand why they churn, and provide actionable business insights using dashboards and machine learning.

🧠 Problem Statement

Customer churn leads to revenue loss.
Businesses want to:

Predict which customers are likely to churn

Understand key churn drivers

Take proactive retention actions

🏗️ Solution Architecture (Medallion Pattern)

Bronze → Silver → Gold

Bronze: Raw customer data ingestion

Silver: Cleaned, transformed, feature-ready data

Gold:

Business metrics

ML predictions

Churn risk classification

Dashboards

🛠️ Tools & Technologies

Databricks Community Edition

PySpark

Spark MLlib (Logistic Regression)

MLflow (experiment tracking)

Delta Lake

Databricks SQL Dashboards

GitHub

📂 Project Structure
customer-churn-databricks/
│
├── notebooks/
│   ├── 01_Bronze_Data.py
│   ├── 02_Silver_Data.py
│   ├── 03_Gold_Business_Analysis.py
│   ├── 04_ML_Churn_Model.py
│   └── 05_Orchestration_Governance.py
│
├── dashboards/
│   └── customer_churn_dashboard.png
│
├── README.md

🤖 Machine Learning Details

Model Used: Logistic Regression

Target Variable: customer_status

Churned → 1

Not Churned → 0

Features Used

Numerical

Age

Tenure in months

Monthly charge

Total revenue

Number of referrals

Categorical

Contract type

State

Payment method

Internet type

Support & security services

📊 Churn Risk Classification
Churn Probability	Risk Level
≥ 0.80	High
0.50 – 0.79	Medium
< 0.50	Low

Predictions are saved in a Gold Delta Table for reporting and dashboards.

📈 Dashboard Insights

The Databricks dashboard provides:

Total customers

Retained vs churned customers

Revenue lost due to churn

Top churn reasons

High churn states

Contract & payment method impact

Tenure vs churn trend

🔍 MLflow Experiment Tracking

MLflow is used to:

Track model runs

Log accuracy metrics

Store trained models

Enable reproducibility

📌 Key Insights

Customers on month-to-month contracts have the highest churn risk.

High monthly charges increase churn probability.

Low-tenure customers are more likely to churn.

Customers without premium support or security services churn more.

Certain regions/states show higher churn patterns.

Manual payment methods are linked to higher churn.

🎯 Recommendations

Focus retention efforts on high-risk customers (≥80% churn probability).

Encourage long-term contracts with discounts.

Improve early customer onboarding.

Bundle premium support and security services.

Promote auto-payment methods to reduce churn.

👤 Author 
Ravi Mule
Data Analyst | Databricks & ML Enthusiast

LinkedIn: (https://www.linkedin.com/in/ravi-mule-199680233/)
