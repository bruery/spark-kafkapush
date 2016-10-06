# This file is part of the Bruery Platform.
#
# (c) Viktore Zara <viktore.zara@gmail.com>
#
# Copyright (c) 2016. For the full copyright and license information,
# please view the LICENSE  file that was distributed with this source code.


"""
 Spark Streaming for kafka.
 Fetches Data on PIWIK and save the log data to HADOOP and pushes it back to KAFKA as Training data for ML
 Usage: piwik_consumer.py <broker_list> <topic>

 To run this on your local machine, you need to setup Kafka and create a producer first, see
 http://kafka.apache.org/documentation.html#quickstart

 and then run the example
    `$ bin/spark-submit --jars \
      external/kafka-assembly/target/scala-*/spark-streaming-kafka-assembly-*.jar \
      spark-kafkapush/src/piwik_consumer.py \
      localhost:9092 piwik_topic`
"""

from __future__ import print_function

import os
import sys
import json

# Path for spark source folder
os.environ['SPARK_HOME'] = "PATH_TO_YOUR_SPARK"
os.environ["PYSPARK_PYTHON"] = "PATH_TO_YOUR_PYTHON3_INSTALL"

# Append pyspark  to Python Path
sys.path.append("PATH_TO_PYSPARK")

try:
    from pyspark import SparkContext
    from pyspark import SparkConf
    from pyspark.streaming import StreamingContext
    from pyspark.streaming.kafka import KafkaUtils

    print("Successfully imported Spark Modules")

except ImportError as e:
    print("Can not import Spark Modules", e)
    sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: piwik_consumer.py <broker_list> <topic>", sys.stderr)
        exit(-1)

    pconf = SparkConf() \
        .setAppName("simple-app-streaming") \
        .setMaster("local[*]")
    sc = SparkContext(conf=pconf)
    ssc = StreamingContext(sc, 2)

    brokers, topic = sys.argv[1:]
    directKafkaStream = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    streamData = directKafkaStream.map(lambda x: x[1])


    # TODO this is an initial commit
    def process(data):
        obj = json.loads(data)
        print(obj['userid'])

    # TODO this is an initial commit
    def loop(rdd):
        try:
            rdd.foreach(process)
        except:
            pass


    streamData.foreachRDD(loop)
    ssc.start()
    ssc.awaitTermination()
