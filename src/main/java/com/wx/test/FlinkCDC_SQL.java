package com.wx.test;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkCDC_SQL {


    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.enableCheckpointing(5000L);
        env.getCheckpointConfig().setCheckpointTimeout(10000L);
        // env.getCheckpointConfig().setCheckpointStorage("hdfs://xnn1:8020/flinkCDC/ck");
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);




        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

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

        Table table = tableEnv.sqlQuery("select * from dwd_common_order_df");

        table.execute().print();

    }

}
