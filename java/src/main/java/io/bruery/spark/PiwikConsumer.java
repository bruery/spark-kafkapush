/*
 * This file is part of the Bruery Platform.
 *
 * (c) Viktore Zara <viktore.zara@gmail.com>
 *
 * Copyright (c) 2016. For the full copyright and license information, please view the LICENSE  file that was distributed with this source code.
 */

package io.bruery.spark;

import com.mongodb.client.MongoDatabase;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import org.apache.spark.streaming.Durations;

import org.json.simple.parser.*;
import org.json.simple.*;

import com.mongodb.*;
import org.bson.Document;

import java.sql.*;
import java.io.IOException;
import java.io.FileInputStream;
import java.io.File;
import java.util.Properties;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;



public class PiwikConsumer {

    public static void main(String[] args) throws ClassNotFoundException, ParseException, IOException {

        if (args.length < 2) {
            System.err.println("Usage: PiwikConsumer <spark> <broker> <topic>");
            System.exit(1);
        }


        String APPNAME = ConfigProperty.getValue("appname");

        final SparkConf conf = new SparkConf()
                .setAppName(APPNAME)
                .setMaster(args[0]);

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(1));
        Set<String> topics = Collections.singleton(args[2]);
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", args[1]);

        Class.forName("com.mysql.cj.jdbc.Driver");

        JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils.createDirectStream(ssc,
                String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);

        directKafkaStream.foreachRDD(rdd -> {
            rdd.foreach(record -> {
                try{
                    JSONParser parser = JavaSparkJSONParserSingleton.getInstance();
                    Object obj = parser.parse(record._2);
                    JSONObject jsonObject = (JSONObject) obj;
                    ResultSet rs = JavaSparkProcessStreamData.fetchData(jsonObject);
                    JavaSparkSaveData.save(rs);
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

class JavaSparkSaveData {
    public static void save(ResultSet rs) throws SQLException {

        String mongo_db = null;
        String mongo_collection = null;
        String mongo_host = null;
        int mongo_port = 0;

        try {
            mongo_host = ConfigProperty.getValue("mongo.host");
            mongo_port = Integer.parseInt(ConfigProperty.getValue("mongo.port"));
            mongo_db = ConfigProperty.getValue("mongo.db");
            mongo_collection = ConfigProperty.getValue("mongo.collection");

        } catch (IOException e) {
            e.printStackTrace();
        }

        while (rs.next()) {
            try {
                // To connect to mongodb server
                MongoClient mongoClient = new MongoClient(mongo_host, mongo_port);
                // Now connect to your databases
                MongoDatabase db = mongoClient.getDatabase(mongo_db);
                db.getCollection(mongo_collection).insertOne(
                        new Document()
                                .append("idlink_va", rs.getString("idlink_va"))
                                .append("idsite", rs.getString("idsite"))
                                .append("kafka_push_uuid", rs.getString("kafka_push_uuid"))
                                .append("idvisitor", rs.getString("idvisitor"))
                                .append("idvisit", rs.getString("idvisit"))
                                .append("user_id", rs.getString("user_id"))
                                .append("config_id", rs.getString("config_id"))
                                .append("server_time", rs.getTimestamp("server_time"))
                                .append("url_id", rs.getString("url_id"))
                                .append("url", rs.getString("url"))
                                .append("url_prefix", rs.getString("url_prefix"))
                                .append("url_action_type", rs.getString("url_action_type"))
                                .append("page_title_id", rs.getString("page_title_id"))
                                .append("page_title", rs.getString("page_title"))
                                .append("page_title_action_type", rs.getString("page_title_action_type"))
                                .append("event_action_id", rs.getString("event_action_id"))
                                .append("event_action", rs.getString("event_action"))
                                .append("event_action_type", rs.getString("event_action_type"))
                                .append("event_category_id", rs.getString("event_category_id"))
                                .append("event_category", rs.getString("event_category"))
                                .append("event_categoryn_type", rs.getString("event_categoryn_type"))
                                .append("content_interaction_id", rs.getString("content_interaction_id"))
                                .append("content_interaction", rs.getString("content_interaction"))
                                .append("content_interaction_type", rs.getString("content_interaction_type"))
                                .append("content_name_id", rs.getString("content_name_id"))
                                .append("content_name", rs.getString("content_name"))
                                .append("content_name_type", rs.getString("content_name_type"))
                                .append("content_piece_id", rs.getString("content_piece_id"))
                                .append("content_piece", rs.getString("content_piece"))
                                .append("content_piece_type", rs.getString("content_piece_type"))
                                .append("content_target_id", rs.getString("content_target_id"))
                                .append("content_target", rs.getString("content_target"))
                                .append("content_target_type", rs.getString("content_target_type"))
                                .append("url_ref_id", rs.getString("url_ref_id"))
                                .append("url_ref", rs.getString("url_ref"))
                                .append("url_ref_type", rs.getString("url_ref_type"))
                                .append("name_ref_id", rs.getString("name_ref_id"))
                                .append("name_ref", rs.getString("name_ref"))
                                .append("name_ref_type", rs.getString("name_ref_type"))
                                .append("entry_name_id", rs.getString("entry_name_id"))
                                .append("entry_name", rs.getString("entry_name"))
                                .append("entry_name_type", rs.getString("entry_name_type"))
                                .append("entry_url_id", rs.getString("entry_url_id"))
                                .append("entry_url", rs.getString("entry_url"))
                                .append("entry_url_type", rs.getString("entry_url_type"))
                                .append("exit_name_id", rs.getString("exit_name_id"))
                                .append("exit_name", rs.getString("exit_name"))
                                .append("exit_name_type", rs.getString("exit_name_type"))
                                .append("exit_url_id", rs.getString("exit_url_id"))
                                .append("exit_url", rs.getString("exit_url"))
                                .append("exit_url_type", rs.getString("exit_url_type"))
                                .append("custom_float", rs.getString("custom_float"))
                                .append("custom_var_k1", rs.getString("custom_var_k1"))
                                .append("custom_var_v1", rs.getString("custom_var_v1"))
                                .append("custom_var_k2", rs.getString("custom_var_k2"))
                                .append("custom_var_v2", rs.getString("custom_var_v2"))
                                .append("custom_var_k3", rs.getString("custom_var_k3"))
                                .append("custom_var_v3", rs.getString("custom_var_v3"))
                                .append("custom_var_k4", rs.getString("custom_var_k4"))
                                .append("custom_var_v4", rs.getString("custom_var_v4"))
                                .append("custom_var_k5", rs.getString("custom_var_k5"))
                                .append("custom_var_v5", rs.getString("custom_var_v5"))
                                .append("custom_dimension_1", rs.getString("custom_dimension_1"))
                                .append("custom_dimension_2", rs.getString("custom_dimension_2"))
                                .append("custom_dimension_3", rs.getString("custom_dimension_3"))
                                .append("custom_dimension_4", rs.getString("custom_dimension_4"))
                                .append("custom_dimension_5", rs.getString("custom_dimension_5"))
                                .append("time_spent", rs.getString("time_spent"))
                                .append("time_spent_ref_action", rs.getString("time_spent_ref_action"))
                                .append("location_ip, ", rs.getString("location_ip"))
                                .append("visit_first_action_time", rs.getTimestamp("visit_first_action_time"))
                                .append("visit_goal_buyer", rs.getString("visit_goal_buyer"))
                                .append("visit_goal_converted", rs.getString("visit_goal_converted"))
                                .append("visitor_days_since_first", rs.getString("visitor_days_since_first"))
                                .append("visitor_days_since_order", rs.getString("visitor_days_since_order"))
                                .append("visitor_returning", rs.getString("visitor_returning"))
                                .append("visitor_count_visits", rs.getString("visitor_count_visits"))
                                .append("visit_total_actions", rs.getString("visit_total_actions"))
                                .append("visit_total_searches", rs.getString("visit_total_searches"))
                                .append("referer_keyword", rs.getString("referer_keyword"))
                                .append("referer_name", rs.getString("referer_name"))
                                .append("referer_type", rs.getString("referer_type"))
                                .append("referer_url", rs.getString("referer_url"))
                                .append("location_browser_lang", rs.getString("location_browser_lang"))
                                .append("config_browser_engine", rs.getString("config_browser_engine"))
                                .append("config_browser_name", rs.getString("config_browser_name"))
                                .append("config_browser_version", rs.getString("config_browser_version"))
                                .append("config_device_brand", rs.getString("config_device_brand"))
                                .append("config_device_model", rs.getString("config_device_model"))
                                .append("config_device_type", rs.getString("config_device_type"))
                                .append("config_os", rs.getString("config_os"))
                                .append("config_os_version", rs.getString("config_os_version"))
                                .append("visit_total_events", rs.getString("visit_total_events"))
                                .append("visitor_localtime", rs.getTimestamp("visitor_localtime"))
                                .append("visitor_days_since_last", rs.getString("visitor_days_since_last"))
                                .append("config_resolution", rs.getString("config_resolution"))
                                .append("config_cookie", rs.getString("config_cookie"))
                                .append("config_director", rs.getString("config_director"))
                                .append("config_flash", rs.getString("config_flash"))
                                .append("config_gears", rs.getString("config_gears"))
                                .append("config_java", rs.getString("config_java"))
                                .append("config_pdf", rs.getString("config_pdf"))
                                .append("config_quicktime", rs.getString("config_quicktime"))
                                .append("config_realplayer", rs.getString("config_realplayer"))
                                .append("config_silverlight", rs.getString("config_silverlight"))
                                .append("config_windowsmedia", rs.getString("config_windowsmedia"))
                                .append("visit_total_time", rs.getString("visit_total_time"))
                                .append("location_city", rs.getString("location_city"))
                                .append("location_country", rs.getString("location_country"))
                                .append("location_latitude", rs.getString("location_latitude"))
                                .append("location_longitude", rs.getString("location_longitude"))
                                .append("location_region", rs.getString("location_region"))
                );
                System.out.println("Document created successfully");
            } catch (Exception e) {
                System.err.println(e.getClass().getName() + ": " + e.getMessage());
            }
        }
    }
}

class JavaSparkProcessStreamData {

    public static ResultSet fetchData(JSONObject jsonObject) throws IOException {

        String MYSQL_CONNECTION_URL = ConfigProperty.getValue("mysql.url");
        String MYSQL_USERNAME = ConfigProperty.getValue("mysql.user");
        String MYSQL_PWD = ConfigProperty.getValue("mysql.pass");

        try {
            Connection conn = DriverManager.getConnection(MYSQL_CONNECTION_URL, MYSQL_USERNAME, MYSQL_PWD);
            PreparedStatement stmt = null;

            stmt = conn.prepareStatement("SELECT \n" +
                    "LVA.idlink_va,\n" +
                    "LVA.idsite,\n" +
                    "LVA.kafka_push_uuid,\n" +
                    "conv(hex(LVA.idvisitor), 16, 10) as idvisitor,\n" +
                    "LVA.idvisit,\n" +
                    "LVA.idaction_url as url_id,\n" +
                    "LA_URL.name as url,\n" +
                    "LA_URL.url_prefix as url_prefix,\n" +
                    "LA_URL.type as url_action_type,\n" +
                    "LVA.idaction_name as page_title_id,\n" +
                    "LA_NAME.name as page_title,\n" +
                    "LA_NAME.type as page_title_action_type,\n" +
                    "LVA.idaction_event_action as event_action_id,\n" +
                    "LA_EVENT_ACTION.name as event_action,\n" +
                    "LA_EVENT_ACTION.type as event_action_type,\n" +
                    "LVA.idaction_event_category as event_category_id,\n" +
                    "LA_EVENT_ACTION_CATEGORY.name as event_category,\n" +
                    "LA_EVENT_ACTION_CATEGORY.type as event_categoryn_type,\n" +
                    "LVA.idaction_content_interaction as content_interaction_id,\n" +
                    "LA_CONTENT_INTERACTION.name as content_interaction,\n" +
                    "LA_CONTENT_INTERACTION.type as content_interaction_type,\n" +
                    "LVA.idaction_content_name as content_name_id,\n" +
                    "LA_CONTENT_NAME.name as content_name,\n" +
                    "LA_CONTENT_NAME.type as content_name_type,\n" +
                    "LVA.idaction_content_piece as content_piece_id,\n" +
                    "LA_CONTENT_PIECE.name as content_piece,\n" +
                    "LA_CONTENT_PIECE.type as content_piece_type,\n" +
                    "LVA.idaction_content_target as content_target_id,\n" +
                    "LA_CONTENT_TARGET.name as content_target,\n" +
                    "LA_CONTENT_TARGET.type as content_target_type,\n" +
                    "LVA.idaction_url_ref as url_ref_id,\n" +
                    "LA_URL_REF.name as url_ref,\n" +
                    "LA_URL_REF.type as url_ref_type,\n" +
                    "LVA.idaction_name_ref as name_ref_id,\n" +
                    "LA_NAME_REF.name as name_ref,\n" +
                    "LA_NAME_REF.type as name_ref_type,\n" +
                    "LV.visit_entry_idaction_name as entry_name_id,\n" +
                    "LA_ENTRY_NAME.name as entry_name,\n" +
                    "LA_ENTRY_NAME.type as entry_name_type,\n" +
                    "LV.visit_entry_idaction_url as entry_url_id,\n" +
                    "LA_ENTRY_URL.name as entry_url,\n" +
                    "LA_ENTRY_URL.type as entry_url_type,\n" +
                    "LV.visit_exit_idaction_name as exit_name_id,\n" +
                    "LA_EXIT_NAME.name as exit_name,\n" +
                    "LA_EXIT_NAME.type as exit_name_type,\n" +
                    "LV.visit_exit_idaction_url as exit_url_id,\n" +
                    "LA_EXIT_URL.name as exit_url,\n" +
                    "LA_EXIT_URL.type as exit_url_type,\n" +
                    "LVA.custom_float,\n" +
                    "LVA.server_time,\n" +
                    "LVA.time_spent_ref_action,\n" +
                    "LVA.custom_var_k1,\n" +
                    "LVA.custom_var_v1,\n" +
                    "LVA.custom_var_k2,\n" +
                    "LVA.custom_var_v2,\n" +
                    "LVA.custom_var_k3,\n" +
                    "LVA.custom_var_v3,\n" +
                    "LVA.custom_var_k4,\n" +
                    "LVA.custom_var_v4,\n" +
                    "LVA.custom_var_k5,\n" +
                    "LVA.custom_var_v5,\n" +
                    "LVA.time_spent,\n" +
                    "LVA.custom_dimension_1,\n" +
                    "LVA.custom_dimension_2,\n" +
                    "LVA.custom_dimension_3,\n" +
                    "LVA.custom_dimension_4,\n" +
                    "LVA.custom_dimension_5,\n" +
                    "LV.user_id,\n" +
                    "to_base64(unhex(md5(LV.config_id))) config_id,\n" +
                    "inet_ntoa(conv(hex(LV.location_ip), 16, 10)) as location_ip, \n" +
                    "LV.visit_first_action_time,\n" +
                    "LV.visit_goal_buyer,\n" +
                    "LV.visit_goal_converted,\n" +
                    "LV.visitor_days_since_first,\n" +
                    "LV.visitor_days_since_order,\n" +
                    "LV.visitor_returning,\n" +
                    "LV.visitor_count_visits,\n" +
                    "LV.visit_total_actions,\n" +
                    "LV.visit_total_searches,\n" +
                    "LV.referer_keyword,\n" +
                    "LV.referer_name,\n" +
                    "LV.referer_type,\n" +
                    "LV.referer_url,\n" +
                    "LV.location_browser_lang,\n" +
                    "LV.config_browser_engine,\n" +
                    "LV.config_browser_name,\n" +
                    "LV.config_browser_version,\n" +
                    "LV.config_device_brand,\n" +
                    "LV.config_device_model,\n" +
                    "LV.config_device_type,\n" +
                    "LV.config_os,\n" +
                    "LV.config_os_version,\n" +
                    "LV.visit_total_events,\n" +
                    "LV.visitor_localtime,\n" +
                    "LV.visitor_days_since_last,\n" +
                    "LV.config_resolution,\n" +
                    "LV.config_cookie,\n" +
                    "LV.config_director,\n" +
                    "LV.config_flash,\n" +
                    "LV.config_gears,\n" +
                    "LV.config_java,\n" +
                    "LV.config_pdf,\n" +
                    "LV.config_quicktime,\n" +
                    "LV.config_realplayer,\n" +
                    "LV.config_silverlight,\n" +
                    "LV.config_windowsmedia,\n" +
                    "LV.visit_total_time,\n" +
                    "LV.location_city,\n" +
                    "LV.location_country,\n" +
                    "LV.location_latitude,\n" +
                    "LV.location_longitude,\n" +
                    "LV.location_region\n" +
                    "FROM piwik_log_link_visit_action LVA\n" +
                    "LEFT JOIN piwik_log_visit LV ON LVA.idvisit = LV.idvisit\n" +
                    "LEFT JOIN piwik_log_action LA_URL ON LVA.idaction_url = LA_URL.idaction\n" +
                    "LEFT JOIN piwik_log_action LA_NAME ON LVA.idaction_name = LA_NAME.idaction\n" +
                    "LEFT JOIN piwik_log_action LA_EVENT_ACTION ON LVA.idaction_event_action = LA_EVENT_ACTION.idaction\n" +
                    "LEFT JOIN piwik_log_action LA_EVENT_ACTION_CATEGORY ON LVA.idaction_event_category = LA_EVENT_ACTION_CATEGORY.idaction\n" +
                    "LEFT JOIN piwik_log_action LA_CONTENT_INTERACTION ON LVA.idaction_content_interaction = LA_CONTENT_INTERACTION.idaction\n" +
                    "LEFT JOIN piwik_log_action LA_CONTENT_NAME ON LVA.idaction_content_name = LA_CONTENT_NAME.idaction\n" +
                    "LEFT JOIN piwik_log_action LA_CONTENT_PIECE ON LVA.idaction_content_piece = LA_CONTENT_PIECE.idaction\n" +
                    "LEFT JOIN piwik_log_action LA_CONTENT_TARGET ON LVA.idaction_content_target = LA_CONTENT_TARGET.idaction\n" +
                    "LEFT JOIN piwik_log_action LA_URL_REF ON LVA.idaction_url_ref = LA_URL_REF.idaction\n" +
                    "LEFT JOIN piwik_log_action LA_NAME_REF ON LVA.idaction_name_ref = LA_NAME_REF.idaction\n" +
                    "LEFT JOIN piwik_log_action LA_ENTRY_NAME ON LV.visit_entry_idaction_name = LA_ENTRY_NAME.idaction\n" +
                    "LEFT JOIN piwik_log_action LA_ENTRY_URL ON LV.visit_entry_idaction_url = LA_ENTRY_URL.idaction\n" +
                    "LEFT JOIN piwik_log_action LA_EXIT_NAME ON LV.visit_exit_idaction_name = LA_EXIT_NAME.idaction\n" +
                    "LEFT JOIN piwik_log_action LA_EXIT_URL ON LV.visit_exit_idaction_url = LA_EXIT_URL.idaction WHERE LVA.kafka_push_uuid = ?");

            stmt.setString(1, jsonObject.get("uuid").toString());
            ResultSet rs = stmt.executeQuery();
            return rs;

        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }
}

class ConfigProperty {
    public static String getValue(String index) throws IOException {
        Properties prop = JavaSparkPropertiesSingleton.getInstance();
        return prop.getProperty(index);
    }
}

class JavaSparkJSONParserSingleton {
    private static transient JSONParser instance = null;
    public static JSONParser getInstance() {
        if (instance == null) {
            instance =  new JSONParser();
        }
        return instance;
    }
}

class JavaSparkPropertiesSingleton {
    private static transient Properties instance = null;
    public static Properties getInstance() throws IOException {
        if (instance == null) {
            instance =  new Properties();
            instance.load(new FileInputStream(new File("src/main/resources/config.properties")));
        }
        return instance;
    }
}
