# Social Network Analytics

Welcome to the Social Network Analytics! This project is a personal exploration of setting up comprehensive analytics for a hypothetical social network startup consisting of two large pieces: feed and messenger.  
Through a series of hands-on steps, I've dived into various analytical techniques and tools to gain insights and drive data-informed decisions.  

---
Databases, access to the tools, and expert guidance have been generously provided by the team at Karpov.Courses (https://karpov.courses/).

## Project Overview

The core objective of this project is to simulate the end-to-end process of setting up analytics for a social network startup.  
I've taken on different roles — data analyst, scientist, and engineer — to create a full-fledged analytics ecosystem.

## Major Steps

### 1. Unveiling the Story

- Explored the distribution of product metrics and performed time series decomposition to uncover hidden patterns.
  
### 2. Crafting Dynamic Dashboards

- Created interactive strategic and operational dashboards using Apache Superset to visualize key product metrics and trends.

### 3. Retention Analysis: Decoding User Behavior

- Delved into user cohorts to understand and visualize user retention dynamics.

### 4. A/B Testing: Experimenting with Recommendations

- Formulated hypotheses, split the users into the groups, performed A/A test.
- Designed A/B tests to evaluate the impact of novel recommendation algorithms on CTR.
- Employed t-test, Mann-Whitney U test, Poisson bootstrap, and bucket transformations, and analyzed the results.

### 5. Marketing Insights: Causal Impact Analysis

- Employed Google's Causal Impact tool to assess the causal effect of marketing campaigns.

### 6. Predicting User Engagement

- Utilized Uber's Orbit Forecast library to estimate user activity, providing insights for future server capacity planning.

### 7. Automating Insights Delivery

- Engineered ETL pipelines using Apache Airflow and established automated report delivery via messengers.

### 8. Staying Alert to Anomalies

- Implemented a real-time anomaly detection system that triggers alerts upon detecting outliers in product metrics.

## Skills Cultivated

- Mastered Python's data manipulation and visualization libraries, enhanced OOP skills writing classes for the ETL tasks code.
- Enhanced SQL skills (Clickhouse) for comprehensive data exploration and analysis.
- Mastered BI tools (Superset) to visualize the data in an automated way.
- Learned ETL pipeline buiding with Airflow and real-time system notification with Telegram bot API.
- Developed a deep understanding of A/B testing methodologies and statistical analysis.
- Gained hands-on experience with predictive analytics tools like Google's Causal Impact and Uber's Orbit package.
  
## Project Structure

- `notebooks/`: Jupyter notebooks containing code and insights for each project phase.
- `etl-setup/`: Python code for DAG setting up and its final output
- `automated-reports/`: Code to set up reporting systems and a report example
- `alert-system-report/`: Code to set up Anomaly detection system and a report example
- `dashboards/`: Screenshots of the dashboards.

## Get Started

1. Clone this repository.
2. Install the required libraries using `pip install -r requirements.txt`.
3. Navigate to specific directories for each phase and run the code in Jupyter notebooks or scripts.

## Contact

For inquiries or collaboration opportunities, you can reach out to me at dmitrii.shiriaev@outlook.com or at my linkedin page: https://www.linkedin.com/in/dmitrii-shiriaev-713aab154/
