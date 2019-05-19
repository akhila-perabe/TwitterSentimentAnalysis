from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
import json
from elasticsearch import Elasticsearch
import sys
import time
from textblob import TextBlob

TCP_IP = 'localhost'
TCP_PORT = 9001

NEGATIVE = "negative"
POSITIVE = "positive"
NEUTRAL = "neutral"

# Pyspark
# create spark configuration
conf = SparkConf()
conf.setAppName('TwitterApp')
conf.setMaster('local[2]')

# create spark context with the above configuration
sc = SparkContext(conf=conf)

# create the Streaming Context from spark context with interval size 2 seconds
ssc = StreamingContext(sc, 4)
ssc.checkpoint("checkpoint_TwitterApp")

# Read data from port 9001
dataStream = ssc.socketTextStream(TCP_IP, TCP_PORT)

def getSentiment(sentence):
    tweet = TextBlob(sentence)
    score = tweet.sentiment.polarity
    if score < 0:
        return NEGATIVE
    elif score > 0:
        return POSITIVE
    else:
        return NEUTRAL


def sendPartition(iter):
    es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    for record in iter:
        try:
            es.index(index='tweetrepository', doc_type='tweet', body={
                'status': record[0],
                'location': [record[1]["lng"], record[1]["lat"]],
                'sentiment': record[2],
                'timestamp' : 'T'.join(record[3].split(' ')) + 'Z'
            })
        except:
            print ("Error" , sys.exc_info()[0])


#Process each sentence in the stream using sentiment analyzer
output = dataStream.map(lambda x: json.loads(x)).map(lambda x: (x["status"], x["location"], getSentiment(x["status"]), x["timestamp"]))
#output.pprint()
output.foreachRDD(lambda rdd: rdd.foreachPartition(sendPartition))

ssc.start()
ssc.awaitTermination()
