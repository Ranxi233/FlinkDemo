package com.wx.test;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkKafka_Order_window {

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
                "CREATE TABLE maxwell_source (" +
                "   `event_time` TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'," +
                "   `database` STRING,\n" +
                "   `table` STRING,\n" +
                "   `type` STRING,\n" +
                "   `ts` TIMESTAMP(3),\n" +
                "   `xid` BIGINT,\n" +
                "   `commit` BOOLEAN,\n" +
                "   `data` ROW<`order_id` INT, `user_code` BIGINT, `business_code` BIGINT, `pay_amount` DECIMAL(18,2), `create_time` TIMESTAMP(3), `pay_time` TIMESTAMP(3)>\n" +
                //"   WATERMARK FOR create_time AS create_time - INTERVAL '5' SECOND" +
                ") " +
                "WITH (\n" +
                "'connector' = 'kafka',\n" +
                "'topic' = 'maxwell',\n" +
                "'properties.bootstrap.servers' = '192.168.0.103:9092',\n" +
                "'properties.group.id' = 'test-consumer-group',\n" +
                "'format' = 'json',\n" +
                "'scan.startup.mode' = 'earliest-offset'\n" +
                ")"
        );

        // 创建订单表视图，只处理 insert 操作
        tableEnv.executeSql("CREATE VIEW order_view AS\n" +
                "SELECT \n" +
                "    data.order_id,\n" +
                "    data.user_code,\n" +
                "    data.pay_amount,\n" +
                "    data.create_time\n" +
                "FROM maxwell_source\n" +
                "WHERE `table` = 'common_order' AND `type` = 'insert';"
        );


        // 统计实时交易额、客单价和商品数量
//        Table resultTable = tableEnv.sqlQuery(
//                "SELECT \n" +
//                        "    window_start,\n" +
//                        "    window_end,\n" +
//                        "    COUNT(order_id) AS order_count,\n" +
//                        "    SUM(pay_amount) AS total_amount,\n" +
//                        "    COUNT(DISTINCT user_code) AS user_cnt\n" +
//                        "FROM TABLE(TUMBLE(TABLE order_view, DESCRIPTOR(create_time), INTERVAL '1' HOUR))\n" +
//                        "GROUP BY window_start, window_end"
//        );
        Table resultTable = tableEnv.sqlQuery(
                "SELECT \n" +
                        "    COUNT(order_id) AS order_count,\n" +
                        "    SUM(pay_amount) AS total_amount,\n" +
                        "    COUNT(DISTINCT user_code) AS user_cnt\n" +
                        "FROM order_view\n"
        );

        // 定义结果表
        String sinkDDL = "CREATE TABLE sink_table (\n" +
                " window_start TIMESTAMP(3),\n" +
                " window_end TIMESTAMP(3),\n" +
                " order_count BIGINT,\n" +
                " total_amount DECIMAL(18, 2),\n" +
                " user_cnt BIGINT\n" +
                ") " +
                "WITH (\n" +
                " 'connector' = 'jdbc',\n" +
                " 'url' = 'jdbc:mysql://192.168.0.103:3306/mall',\n" +
                " 'table-name' = 'hourly_sales',\n" +
                " 'username' = 'root',\n" +
                " 'password' = '000000'\n" +
                ")";
        //tableEnv.executeSql(sinkDDL);

        // 将结果插入到结果表中
        //resultTable.executeInsert("sink_table");
        resultTable.execute().print();
    }
}