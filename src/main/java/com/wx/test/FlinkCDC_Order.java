package com.wx.test;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkCDC_Order {


    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.enableCheckpointing(5000L);
        env.getCheckpointConfig().setCheckpointTimeout(10000L);
        // env.getCheckpointConfig().setCheckpointStorage("hdfs://xnn1:8020/flinkCDC/ck");
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);




        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //创建订单表
        tableEnv.executeSql(""
                + "CREATE TABLE `dwd_common_order_df` (\n"
                + "  order_id INT PRIMARY KEY not ENFORCED,\n"
                + "  create_time TIMESTAMP,\n"
                + "  pay_time TIMESTAMP,\n"
                + "  user_code bigint,\n"
                + "  business_code bigint,\n"
                + "  pay_amount decimal(18,2)\n"
                + ") "
                + "WITH (\n"
                + "'connector' = 'mysql-cdc',\n"
                + "'hostname' = '192.168.0.103',\n"
                + "'username' = 'root',\n"
                + "'password' = '000000',\n"
                + "'port' = '3306',\n"
                + "'database-name' = 'mall',\n"
                + "'table-name' = 'common_order'\n"
                + ")"
        );

        // 创建订单商品表
        tableEnv.executeSql(
                "CREATE TABLE dwd_common_order_product_df (" +
                "       order_product_id INT,\n" +
                "       create_time TIMESTAMP,\n" +
                "       pay_time TIMESTAMP,\n" +
                "       order_id INT,\n" +
                "       user_code bigint,\n" +
                "       business_code bigint,\n" +
                "       product_id INT,\n" +
                "       product_name VARCHAR(255),\n" +
                "       product_amount decimal(18,2),\n" +
                "       PRIMARY KEY (order_product_id) NOT ENFORCED" +
                ") WITH (" +
                " 'connector' = 'mysql-cdc'," +
                " 'hostname' = '192.168.0.103'," +
                " 'port' = '3306'," +
                " 'username' = 'root'," +
                " 'password' = '000000'," +
                " 'database-name' = 'mall'," +
                " 'table-name' = 'common_order_product'" +
                ")"
        );



        // 统计实时交易额、客单价和商品数量
        Table resultTable = tableEnv.sqlQuery(
                "SELECT \n" +
                        "   TO_DATE(DATE_FORMAT(t1.pay_time, 'yyyy-MM-dd')) AS order_date, \n" +
                        "   SUM(t1.pay_amount) AS total_amount, \n" +
                        "   COUNT(t1.order_id) AS order_count, \n" +
                        "   SUM(t1.pay_amount) / COUNT(t1.order_id) AS avg_order_value, \n" +
                        "   COUNT(DISTINCT t1.user_code) AS user_cnt \n" +
                        "FROM dwd_common_order_df t1 \n" +
                        "LEFT JOIN dwd_common_order_product_df t2 ON t1.order_id = t2.order_id \n"
                        + "GROUP BY TO_DATE(DATE_FORMAT(t1.pay_time, 'yyyy-MM-dd'))"
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

        resultTable.executeInsert("dws_trd_amount_1d");

        resultTable.execute().print();

    }

}
