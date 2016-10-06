/*
 * This file is part of the Bruery Platform.
 *
 * (c) Viktore Zara <viktore.zara@gmail.com>
 *
 * Copyright (c) 2016. For the full copyright and license information, please view the LICENSE  file that was distributed with this source code.
 */

package io.bruery.spark;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;
import org.json.simple.parser.JSONParser;

/**
 * Created by rmzamora on 10/5/16.
 */
public class PiwikConsumer {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("simple-app-streaming")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(2000));

        Set<String> topics = Collections.singleton("sample_topic");
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", "kafka.localhost:9092");

        JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils.createDirectStream(ssc,
                String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);

        directKafkaStream.foreachRDD(rdd -> {
            rdd.foreach(record -> {
                /*
                    USE JSON PARSER FOR NOW UNTIL WE FIND A WAY FOR PHP AVRO TO BE COMPATIBLE TO AVRO 1.8+
                 */
                JSONParser parser = new JSONParser();
                try{
                    Object obj = parser.parse(record._2);
                    JSONObject jsonObject = (JSONObject) obj;
                    System.out.println("USERID="+jsonObject.get("userid")+" ACTION="+jsonObject.get("action"));

                }catch(ParseException pe){
                    System.out.println("position: " + pe.getPosition());
                    System.out.println(pe);
                }
            });
        });

        ssc.start();
        try {
            ssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
