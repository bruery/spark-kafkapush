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
import pymysql.cursors
import pymongo
import bson
import datetime
import yaml


# Path for spark source folder
os.environ['SPARK_HOME'] = "/Users/rmzamora/Documents/Programming/server/apache/spark/2.0.0-hadoop2.7"
os.environ["PYSPARK_PYTHON"] = "/usr/local/bin/python3"

# Append pyspark  to Python Path
sys.path.append("/Users/rmzamora/Documents/Programming/server/apache/spark/2.0.0-hadoop2.7/python/pyspark")

try:
    from pyspark import SparkContext
    from pyspark import SparkConf
    from pyspark.streaming import StreamingContext
    from pyspark.streaming.kafka import KafkaUtils
    from pymysql import cursors
    from myquerybuilder import QueryBuilder
    from pymongo import MongoClient
    from bson.codec_options import CodecOptions
    from datetime import timedelta

    print("Successfully imported Spark Modules")

except ImportError as e:
    print("Can not import Spark Modules", e)
    sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: piwik_consumer.py  <spark> <broker_list> <topic>", sys.stderr)
        exit(-1)

    spark, brokers, topic = sys.argv[1:]

    config = yaml.safe_load(open("config.yml"))

    pconf = SparkConf() \
        .setAppName(config['piwik_consumer']['appname']) \
        .setMaster(spark)
    sc = SparkContext(conf=pconf)
    sc.addPyFile("/usr/local/lib/python3.5/site-packages/pymysql/cursors.py")
    sc.addPyFile("/usr/local/lib/python3.5/site-packages/pymongo/mongo_client.py")
    ssc = StreamingContext(sc, 1)

    directKafkaStream = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    # Return the payload value {payload[0], payload[1]}
    streamData = directKafkaStream.map(lambda payload: payload[1])


    def loop(rdd):
        try:
            rdd.foreach(transform)
        except Exception as e:
            print(e)

    def transform(data):
        obj = json.loads(data)
        result = fetchPiwikData(obj['uuid'])
        if result != 'None':
            client = MongoClient(config['piwik_consumer']['mongodb']['url'])
            db = client[config['piwik_consumer']['mongodb']['db']]
            collection = db[config['piwik_consumer']['mongodb']['collection']]

            server_time = datetime.datetime.strptime(str(result["server_time"]), "%Y-%m-%d %H:%M:%S")
            visit_first_action_time = datetime.datetime.strptime(str(result["visit_first_action_time"]), "%Y-%m-%d %H:%M:%S")
            visitor_localtime = datetime.datetime.strptime(str(result["visit_first_action_time"]),"%Y-%m-%d %H:%M:%S")

            data = {
                "idlink_va": result["idlink_va"],
                "idsite": result["idsite"],
                "kafka_push_uuid": result["kafka_push_uuid"],
                "idvisitor": result["idvisitor"],
                "idvisit": result["idvisit"],
                "user_id": result["user_id"],
                "config_id": result["config_id"],
                "server_time": server_time,
                "url_id": result["url_id"],
                "url": result["url"],
                "url_prefix": result["url_prefix"],
                "url_action_type": result["url_action_type"],
                "page_title_id": result["page_title_id"],
                "page_title": result["page_title"],
                "page_title_action_type": result["page_title_action_type"],
                "event_action_id": result["event_action_id"],
                "event_action": result["event_action"],
                "event_action_type": result["event_action_type"],
                "event_category_id": result["event_category_id"],
                "event_category": result["event_category"],
                "event_categoryn_type": result["event_categoryn_type"],
                "content_interaction_id": result["content_interaction_id"],
                "content_interaction": result["content_interaction"],
                "content_interaction_type": result["content_interaction_type"],
                "content_name_id": result["content_name_id"],
                "content_name": result["content_name"],
                "content_name_type": result["content_name_type"],
                "content_piece_id": result["content_piece_id"],
                "content_piece": result["content_piece"],
                "content_piece_type": result["content_piece_type"],
                "content_target_id": result["content_target_id"],
                "content_target": result["content_target"],
                "content_target_type": result["content_target_type"],
                "url_ref_id": result["url_ref_id"],
                "url_ref": result["url_ref"],
                "url_ref_type": result["url_ref_type"],
                "name_ref_id": result["name_ref_id"],
                "name_ref": result["name_ref"],
                "name_ref_type": result["name_ref_type"],
                "entry_name_id": result["entry_name_id"],
                "entry_name": result["entry_name"],
                "entry_name_type": result["entry_name_type"],
                "entry_url_id": result["entry_url_id"],
                "entry_url": result["entry_url"],
                "entry_url_type": result["entry_url_type"],
                "exit_name_id": result["exit_name_id"],
                "exit_name": result["exit_name"],
                "exit_name_type": result["exit_name_type"],
                "exit_url_id": result["exit_url_id"],
                "exit_url": result["exit_url"],
                "exit_url_type": result["exit_url_type"],
                "custom_float": result["custom_float"],
                "custom_var_k1": result["custom_var_k1"],
                "custom_var_v1": result["custom_var_v1"],
                "custom_var_k2": result["custom_var_k2"],
                "custom_var_v2": result["custom_var_v2"],
                "custom_var_k3": result["custom_var_k3"],
                "custom_var_v3": result["custom_var_v3"],
                "custom_var_k4": result["custom_var_k4"],
                "custom_var_v4": result["custom_var_v4"],
                "custom_var_k5": result["custom_var_k5"],
                "custom_var_v5": result["custom_var_v5"],
                "custom_dimension_1": result["custom_dimension_1"],
                "custom_dimension_2": result["custom_dimension_2"],
                "custom_dimension_3": result["custom_dimension_3"],
                "custom_dimension_4": result["custom_dimension_4"],
                "custom_dimension_5": result["custom_dimension_5"],
                "time_spent": result["time_spent"],
                "time_spent_ref_action": result["time_spent_ref_action"],
                "location_ip ": result["location_ip"],
                "visit_first_action_time": visit_first_action_time,
                "visit_goal_buyer": result["visit_goal_buyer"],
                "visit_goal_converted": result["visit_goal_converted"],
                "visitor_days_since_first": result["visitor_days_since_first"],
                "visitor_days_since_order": result["visitor_days_since_order"],
                "visitor_returning": result["visitor_returning"],
                "visitor_count_visits": result["visitor_count_visits"],
                "visit_total_actions": result["visit_total_actions"],
                "visit_total_searches": result["visit_total_searches"],
                "referer_keyword": result["referer_keyword"],
                "referer_name": result["referer_name"],
                "referer_type": result["referer_type"],
                "referer_url": result["referer_url"],
                "location_browser_lang": result["location_browser_lang"],
                "config_browser_engine": result["config_browser_engine"],
                "config_browser_name": result["config_browser_name"],
                "config_browser_version": result["config_browser_version"],
                "config_device_brand": result["config_device_brand"],
                "config_device_model": result["config_device_model"],
                "config_device_type": result["config_device_type"],
                "config_os": result["config_os"],
                "config_os_version": result["config_os_version"],
                "visit_total_events": result["visit_total_events"],
                "visitor_localtime": visitor_localtime,
                "visitor_days_since_last": result["visitor_days_since_last"],
                "config_resolution": result["config_resolution"],
                "config_cookie": result["config_cookie"],
                "config_director": result["config_director"],
                "config_flash": result["config_flash"],
                "config_gears": result["config_gears"],
                "config_java": result["config_java"],
                "config_pdf": result["config_pdf"],
                "config_quicktime": result["config_quicktime"],
                "config_realplayer": result["config_realplayer"],
                "config_silverlight": result["config_silverlight"],
                "config_windowsmedia": result["config_windowsmedia"],
                "visit_total_time": result["visit_total_time"],
                "location_city": result["location_city"],
                "location_country": result["location_country"],
                "location_latitude": result["location_latitude"],
                "location_longitude": result["location_longitude"],
                "location_region": result["location_region"]
            }
            _id = collection.insert_one(data).inserted_id
            print(_id)


    def fetchPiwikData(uuid):
        # Connect to the database
        connection = pymysql.connect(host=config['piwik_consumer']['mysql']['host'],
                                     user=config['piwik_consumer']['mysql']['user'],
                                     password=config['piwik_consumer']['mysql']['pass'],
                                     db=config['piwik_consumer']['mysql']['db'],
                                     charset=config['piwik_consumer']['mysql']['charset'],
                                     cursorclass=pymysql.cursors.DictCursor)

        try:
            with connection.cursor() as cursor:
                # Read a single record
                sql = """SELECT
                        LVA.idlink_va,
                        LVA.idsite,
                        LVA.kafka_push_uuid,
                        conv(hex(LVA.idvisitor), 16, 10) as idvisitor,
                        LVA.idvisit,
                        LVA.idaction_url as url_id,
                        LA_URL.name as url,
                        LA_URL.url_prefix as url_prefix,
                        LA_URL.type as url_action_type,
                        LVA.idaction_name as page_title_id,
                        LA_NAME.name as page_title,
                        LA_NAME.type as page_title_action_type,
                        LVA.idaction_event_action as event_action_id,
                        LA_EVENT_ACTION.name as event_action,
                        LA_EVENT_ACTION.type as event_action_type,
                        LVA.idaction_event_category as event_category_id,
                        LA_EVENT_ACTION_CATEGORY.name as event_category,
                        LA_EVENT_ACTION_CATEGORY.type as event_categoryn_type,
                        LVA.idaction_content_interaction as content_interaction_id,
                        LA_CONTENT_INTERACTION.name as content_interaction,
                        LA_CONTENT_INTERACTION.type as content_interaction_type,
                        LVA.idaction_content_name as content_name_id,
                        LA_CONTENT_NAME.name as content_name,
                        LA_CONTENT_NAME.type as content_name_type,
                        LVA.idaction_content_piece as content_piece_id,
                        LA_CONTENT_PIECE.name as content_piece,
                        LA_CONTENT_PIECE.type as content_piece_type,
                        LVA.idaction_content_target as content_target_id,
                        LA_CONTENT_TARGET.name as content_target,
                        LA_CONTENT_TARGET.type as content_target_type,
                        LVA.idaction_url_ref as url_ref_id,
                        LA_URL_REF.name as url_ref,
                        LA_URL_REF.type as url_ref_type,
                        LVA.idaction_name_ref as name_ref_id,
                        LA_NAME_REF.name as name_ref,
                        LA_NAME_REF.type as name_ref_type,
                        LV.visit_entry_idaction_name as entry_name_id,
                        LA_ENTRY_NAME.name as entry_name,
                        LA_ENTRY_NAME.type as entry_name_type,
                        LV.visit_entry_idaction_url as entry_url_id,
                        LA_ENTRY_URL.name as entry_url,
                        LA_ENTRY_URL.type as entry_url_type,
                        LV.visit_exit_idaction_name as exit_name_id,
                        LA_EXIT_NAME.name as exit_name,
                        LA_EXIT_NAME.type as exit_name_type,
                        LV.visit_exit_idaction_url as exit_url_id,
                        LA_EXIT_URL.name as exit_url,
                        LA_EXIT_URL.type as exit_url_type,
                        LVA.custom_float,
                        LVA.server_time,
                        LVA.time_spent_ref_action,
                        LVA.custom_var_k1,
                        LVA.custom_var_v1,
                        LVA.custom_var_k2,
                        LVA.custom_var_v2,
                        LVA.custom_var_k3,
                        LVA.custom_var_v3,
                        LVA.custom_var_k4,
                        LVA.custom_var_v4,
                        LVA.custom_var_k5,
                        LVA.custom_var_v5,
                        LVA.time_spent,
                        LVA.custom_dimension_1,
                        LVA.custom_dimension_2,
                        LVA.custom_dimension_3,
                        LVA.custom_dimension_4,
                        LVA.custom_dimension_5,
                        LV.user_id,
                        to_base64(unhex(md5(LV.config_id))) config_id,
                        inet_ntoa(conv(hex(LV.location_ip), 16, 10)) as location_ip,
                        LV.visit_first_action_time,
                        LV.visit_goal_buyer,
                        LV.visit_goal_converted,
                        LV.visitor_days_since_first,
                        LV.visitor_days_since_order,
                        LV.visitor_returning,
                        LV.visitor_count_visits,
                        LV.visit_total_actions,
                        LV.visit_total_searches,
                        LV.referer_keyword,
                        LV.referer_name,
                        LV.referer_type,
                        LV.referer_url,
                        LV.location_browser_lang,
                        LV.config_browser_engine,
                        LV.config_browser_name,
                        LV.config_browser_version,
                        LV.config_device_brand,
                        LV.config_device_model,
                        LV.config_device_type,
                        LV.config_os,
                        LV.config_os_version,
                        LV.visit_total_events,
                        LV.visitor_localtime,
                        LV.visitor_days_since_last,
                        LV.config_resolution,
                        LV.config_cookie,
                        LV.config_director,
                        LV.config_flash,
                        LV.config_gears,
                        LV.config_java,
                        LV.config_pdf,
                        LV.config_quicktime,
                        LV.config_realplayer,
                        LV.config_silverlight,
                        LV.config_windowsmedia,
                        LV.visit_total_time,
                        LV.location_city,
                        LV.location_country,
                        LV.location_latitude,
                        LV.location_longitude,
                        LV.location_region
                        FROM piwik_log_link_visit_action LVA
                        LEFT JOIN piwik_log_visit LV ON LVA.idvisit = LV.idvisit
                        LEFT JOIN piwik_log_action LA_URL ON LVA.idaction_url = LA_URL.idaction
                        LEFT JOIN piwik_log_action LA_NAME ON LVA.idaction_name = LA_NAME.idaction
                        LEFT JOIN piwik_log_action LA_EVENT_ACTION ON LVA.idaction_event_action = LA_EVENT_ACTION.idaction
                        LEFT JOIN piwik_log_action LA_EVENT_ACTION_CATEGORY ON LVA.idaction_event_category = LA_EVENT_ACTION_CATEGORY.idaction
                        LEFT JOIN piwik_log_action LA_CONTENT_INTERACTION ON LVA.idaction_content_interaction = LA_CONTENT_INTERACTION.idaction
                        LEFT JOIN piwik_log_action LA_CONTENT_NAME ON LVA.idaction_content_name = LA_CONTENT_NAME.idaction
                        LEFT JOIN piwik_log_action LA_CONTENT_PIECE ON LVA.idaction_content_piece = LA_CONTENT_PIECE.idaction
                        LEFT JOIN piwik_log_action LA_CONTENT_TARGET ON LVA.idaction_content_target = LA_CONTENT_TARGET.idaction
                        LEFT JOIN piwik_log_action LA_URL_REF ON LVA.idaction_url_ref = LA_URL_REF.idaction
                        LEFT JOIN piwik_log_action LA_NAME_REF ON LVA.idaction_name_ref = LA_NAME_REF.idaction
                        LEFT JOIN piwik_log_action LA_ENTRY_NAME ON LV.visit_entry_idaction_name = LA_ENTRY_NAME.idaction
                        LEFT JOIN piwik_log_action LA_ENTRY_URL ON LV.visit_entry_idaction_url = LA_ENTRY_URL.idaction
                        LEFT JOIN piwik_log_action LA_EXIT_NAME ON LV.visit_exit_idaction_name = LA_EXIT_NAME.idaction
                        LEFT JOIN piwik_log_action LA_EXIT_URL ON LV.visit_exit_idaction_url = LA_EXIT_URL.idaction
                        WHERE LVA.kafka_push_uuid = %s
                      """
                cursor.execute(sql, (uuid,))
                # should return only one data
                result = cursor.fetchone()
                return result
        finally:
            connection.close()

    streamData.foreachRDD(loop)
    ssc.start()
    ssc.awaitTermination()
