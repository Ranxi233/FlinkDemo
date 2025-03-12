package com.wx.test;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import com.alibaba.fastjson.JSONObject;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class FlinkKafka_DataSteam {

    public static KafkaSource<String> getKafkaSource(String groupId, String topic) {
        return KafkaSource.<String>builder()
                .setBootstrapServers("xnn1:9092")
                .setGroupId(groupId)
                .setTopics(topic)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new DeserializationSchema<String>() {
                    @Override
                    public String deserialize(byte[] message) throws IOException {
                        if (message != null) {
                            return new String(message, StandardCharsets.UTF_8);
                        }
                        return null;
                    }

                    @Override
                    public boolean isEndOfStream(String nextElement) {
                        return false;
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return Types.STRING;
                    }
                })
                .build();
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        KafkaSource<String> maxwell = getKafkaSource("test-consumer-group", "maxwell");
        DataStreamSource<String> stream = env.fromSource(maxwell, WatermarkStrategy.noWatermarks(), "kafka_source");

        SingleOutputStreamOperator<Order> map = stream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(value);
                String table = jsonObject.getString("table");
                return "common_order".equals(table);
            }
        }).map(new MapFunction<String, Order>() {
            @Override
            public Order map(String value) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(value);
                JSONObject data = jsonObject.getJSONObject("data");
                return new Order(
                        data.getString("create_time"),
                        data.getString("pay_time"),
                        data.getIntValue("user_code"),
                        data.getIntValue("business_code"),
                        data.getDouble("pay_amount")
                );
            }
        });

        tableEnv.createTemporaryView("inputtable", map);
        Table resultTable = tableEnv.sqlQuery("SELECT * FROM inputtable");
        resultTable.execute().print();

        env.execute("Flink Kafka DataStream Example");
    }

    public static class Order {
        public String create_time;
        public String pay_time;
        public int user_code;
        public int business_code;
        public double pay_amount;

        public Order(String create_time, String pay_time, int user_code, int business_code, double pay_amount) {
            this.create_time = create_time;
            this.pay_time = pay_time;
            this.user_code = user_code;
            this.business_code = business_code;
            this.pay_amount = pay_amount;
        }

        // Getters and setters (optional, but recommended for POJOs)
        public String getCreate_time() {
            return create_time;
        }

        public void setCreate_time(String create_time) {
            this.create_time = create_time;
        }

        public String getPay_time() {
            return pay_time;
        }

        public void setPay_time(String pay_time) {
            this.pay_time = pay_time;
        }

        public int getUser_code() {
            return user_code;
        }

        public void setUser_code(int user_code) {
            this.user_code = user_code;
        }

        public int getBusiness_code() {
            return business_code;
        }

        public void setBusiness_code(int business_code) {
            this.business_code = business_code;
        }

        public double getPay_amount() {
            return pay_amount;
        }

        public void setPay_amount(double pay_amount) {
            this.pay_amount = pay_amount;
        }
    }
}