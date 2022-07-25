# US-accidents-analysis

A Data Engineering project to analyse US accidents and visualize them.

The dataset comes from [Kaggle](https://www.kaggle.com/datasets/sobhanmoosavi/us-accidents). I wanted this project to have a streaming data source, but unfortunately must of the APIs are for crypto and finance. So instead I decided to stream the dataset based on the Start_Time and End_Time columns.

The processing is done with Pyspark, the data is streamed to Kafka Topic, written to a MongoDB collection, then to a PostgreSQL data warehouse and finally visualized with Superset.
