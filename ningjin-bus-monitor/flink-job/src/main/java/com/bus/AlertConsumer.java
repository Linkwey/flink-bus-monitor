package com.bus;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * 独立告警消费者：从 Kafka 的 bus_alerts 主题读取告警 JSON，写入 MySQL 历史表。
 * 实现设计文档要求的告警三路输出（Redis、Kafka、MySQL）中的 MySQL 持久化部分。
 */
public class AlertConsumer {
    // Kafka 配置
    private static final String KAFKA_BOOTSTRAP = "192.168.13.129:9092";   // Kafka 服务器地址
    private static final String TOPIC = "bus_alerts";                       // 订阅的告警主题
    private static final String GROUP_ID = "alert-mysql-consumer";          // 固定消费者组 ID，便于记录偏移量

    // MySQL 配置
    private static final String MYSQL_URL = "jdbc:mysql://192.168.13.129:3306/bus_monitor?useSSL=false&serverTimezone=Asia/Shanghai&allowPublicKeyRetrieval=true";
    private static final String MYSQL_USER = "root";
    private static final String MYSQL_PASSWORD = "123456";

    public static void main(String[] args) {
        // 1. 配置 Kafka 消费者参数
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP);   // Kafka 地址
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);                    // 消费者组 ID
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());   // key 反序列化
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); // value 反序列化
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");        // 从最早消息开始消费（确保不丢失历史告警）
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");            // 自动提交偏移量（简化演示）
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");       // 每 1 秒提交一次偏移量

        // 2. 创建 Kafka 消费者（使用 try-with-resources 自动关闭）
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            // 订阅主题
            consumer.subscribe(Collections.singletonList(TOPIC));
            System.out.println("[AlertConsumer] Subscribed to topic: " + TOPIC);

            // 3. 建立 MySQL 连接
            try (Connection conn = DriverManager.getConnection(MYSQL_URL, MYSQL_USER, MYSQL_PASSWORD)) {
                // 预编译 SQL 插入语句（字段顺序与表结构一致）
                String insertSql = "INSERT INTO bus_alerts_history (plate, speed, alert_type, alert_time, lng, lat, line) VALUES (?, ?, ?, ?, ?, ?, ?)";
                PreparedStatement stmt = conn.prepareStatement(insertSql);

                // 4. 循环拉取 Kafka 消息，持续处理
                while (true) {
                    // 每 1 秒拉取一次，避免空转
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                    for (ConsumerRecord<String, String> record : records) {
                        String json = record.value();
                        System.out.println("[AlertConsumer] Received alert: " + json);

                        // 5. 解析 JSON，提取字段
                        try {
                            ObjectMapper mapper = new ObjectMapper();
                            JsonNode node = mapper.readTree(json);
                            String plate = node.get("plate").asText();          // 车号
                            double speed = node.get("speed").asDouble();       // 速度
                            String alertType = node.get("type").asText();      // 告警类型（超速/低速）
                            String timeStr = node.get("time").asText().split("\\.")[0]; // 去掉毫秒，格式 yyyy-MM-dd HH:mm:ss
                            double lng = node.get("lng").asDouble();           // 经度（BD09）
                            double lat = node.get("lat").asDouble();           // 纬度（BD09）
                            String line = node.get("line").asText();           // 线路

                            // 6. 设置 PreparedStatement 参数，执行插入
                            stmt.setString(1, plate);
                            stmt.setDouble(2, speed);
                            stmt.setString(3, alertType);
                            stmt.setTimestamp(4, Timestamp.valueOf(timeStr));
                            stmt.setDouble(5, lng);
                            stmt.setDouble(6, lat);
                            stmt.setString(7, line);
                            stmt.executeUpdate();   // 执行 INSERT
                            System.out.println("[AlertConsumer] MySQL insert success: " + plate + " " + alertType);
                        } catch (Exception e) {
                            // 单条告警处理失败，打印错误但不影响后续消息（避免程序退出）
                            System.err.println("[AlertConsumer] Failed to process alert: " + e.getMessage());
                            e.printStackTrace();
                        }
                    }
                }
            } catch (Exception e) {
                // MySQL 连接失败，打印错误（程序退出）
                System.err.println("[AlertConsumer] MySQL connection failed: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }
}