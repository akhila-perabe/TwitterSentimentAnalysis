# Twitter Sentiment Analysis

Sentiment Analysis of tweets in real time using Apache Spark Streaming, ElasticSearch and Kibana. The framework performs sentiment analysis of the specified hashtags in twitter data in real time. The findings of the sentiment analysis is to be visualized using ElasticSearch/Kibana.

For example, we want to do the sentiment analysis for all the tweets for #guncontrolnow and show their (e.g.,positive, neutral, negative) statistics in Kibana.

stream.py -> Module to fetch the tweets from twitter in real time
spark.py  -> Module processing the tweets for sentiment and visualization.
