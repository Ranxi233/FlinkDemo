package com.wx.atu;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkSql_fromKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.enableCheckpointing(5000L);
        env.getCheckpointConfig().setCheckpointTimeout(10000L);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 定义 Kafka 数据源表
        tableEnv.executeSql("" +
                "CREATE TABLE common_order (" +
                "  order_id INT PRIMARY KEY not ENFORCED,\n" +
                "  create_time TIMESTAMP,\n" +
                "  pay_time TIMESTAMP,\n" +
                "  user_code bigint,\n" +
                "  business_code bigint,\n" +
                "  pay_amount decimal(18,2)\n" +
                ") " +
                "WITH (\n" +
                "'connector' = 'kafka',\n" +
                "'topic' = 'order_topic2',\n" +
                "'properties.bootstrap.servers' = 'xnn1:9092',\n" +
                //"'properties.group.id' = 'test-consumer-group',\n" +
                "'format' = 'json',\n" +
                "'scan.startup.mode' = 'earliest-offset'\n" +
                ")"
        );

        String sinkDDL = "CREATE TABLE dws_trd_amount_1d (\n" +
                " order_date DATE,\n" +
                " total_amount DECIMAL(10, 2),\n" +
                " order_count BIGINT,\n" +
                " avg_order_value DECIMAL(10, 2),\n" +
                " user_cnt BIGINT,\n" +
                " PRIMARY KEY (order_date) NOT ENFORCED\n" +
                ") " +
                "WITH (\n" +
                " 'connector' = 'jdbc',\n" +
                " 'url' = 'jdbc:mysql://192.168.0.103:3306/mall',\n" +
                " 'table-name' = 'dws_trd_amount_1d',\n" +
                " 'username' = 'root',\n" +
                " 'password' = '000000'\n" +
                ")";
        tableEnv.executeSql(sinkDDL);


//        Table resultTable = tableEnv.sqlQuery(
//                "SELECT \n" +
//                        "   TO_DATE(DATE_FORMAT(t1.pay_time, 'yyyy-MM-dd')) AS order_date, \n" +
//                        "   SUM(t1.pay_amount) AS total_amount, \n" +
//                        "   COUNT(t1.order_id) AS order_count, \n" +
//                        "   SUM(t1.pay_amount) / COUNT(t1.order_id) AS avg_order_value, \n" +
//                        "   COUNT(DISTINCT t1.user_code) AS user_cnt \n" +
//                        "FROM common_order t1 \n" +
//                        "GROUP BY TO_DATE(DATE_FORMAT(t1.pay_time, 'yyyy-MM-dd'))"
//        );

        Table resultTable = tableEnv.sqlQuery(
                "SELECT \n" +
                        "* \n" +
                        "FROM common_order t1 \n"
        );
        //resultTable.executeInsert("dws_trd_amount_1d");
        resultTable.execute().print();

//        // 定义结果表
//        String sinkDDL = "CREATE TABLE sink_table (\n" +
//                " window_start TIMESTAMP(3),\n" +
//                " window_end TIMESTAMP(3),\n" +
//                " order_count BIGINT,\n" +
//                " total_amount DECIMAL(18, 2),\n" +
//                " user_cnt BIGINT\n" +
//                ") " +
//                "WITH (\n" +
//                " 'connector' = 'jdbc',\n" +
//                " 'url' = 'jdbc:mysql://192.168.0.103:3306/mall',\n" +
//                " 'table-name' = 'hourly_sales',\n" +
//                " 'username' = 'root',\n" +
//                " 'password' = '000000'\n" +
//                ")";
//        tableEnv.executeSql(sinkDDL);
//
//
//        Table resultTable = tableEnv.sqlQuery(
//                "SELECT \n" +
//                        " * \n" +
////                        "    COUNT(order_id) AS order_count,\n" +
////                        "    SUM(pay_amount) AS total_amount,\n" +
////                        "    COUNT(DISTINCT user_code) AS user_cnt\n" +
//                        "FROM common_order\n"
//        );
//
//        // 将结果插入到结果表中
//        //resultTable.executeInsert("sink_table");
//        resultTable.execute().print();


    }
}
