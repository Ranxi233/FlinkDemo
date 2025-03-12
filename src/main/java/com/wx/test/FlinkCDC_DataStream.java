package com.wx.test;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkCDC_DataStream {


    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        MySqlSource mysqlDataSource = MySqlSource.<String>builder()
                .hostname("192.168.0.103")
                .port(3306)
                .username("root")
                .password("000000")
                .databaseList("mall")
                .tableList("mall.common_order") //不写监控所有表
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();
        DataStreamSource mysqkDs = env.fromSource(mysqlDataSource, WatermarkStrategy.noWatermarks(), "mysql-source");

        mysqkDs.print();

        env.execute();

    }

}
