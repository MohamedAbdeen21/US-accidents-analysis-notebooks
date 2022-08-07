# US-accidents-analysis

A Data Engineering project to analyse US accidents and visualize them.

The dataset comes from [Kaggle](https://www.kaggle.com/datasets/sobhanmoosavi/us-accidents). I wanted this project to have a streaming data source, but unfortunately must of the APIs are for crypto and finance. So instead I decided to stream the dataset based on the Start_Time and End_Time columns.

The initial plan was that the processing will be done with Pyspark, the data will be streamed to Kafka Topic, written to a MongoDB collection, then to a PostgreSQL data warehouse and finally visualized with Superset.

I also wanted to include Kafka Streams, but it is not supported in Python. So I used faust. However, faust's throughput is extremely low. The faust code snippet will be included in the walkthrough version, but will be excluded in the final project. Also, mongoDB could not handle the high volume high velocity write which caused latency that reached up to 10 mins while streaming over 1 hour. That's why mongoDB was ditched and istead I decided to write directly to the postgreSQL warehouse.
