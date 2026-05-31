package com.bus;

import com.bus.aggregation.*;
import com.bus.model.BusGPSRecord;
import com.bus.process.AlertProcessFunction;
import com.bus.process.VehicleStateFunction;
import com.bus.source.BusGPSDeserializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.*;
import java.util.stream.Collectors;

public class StreamingJob {
    public static void main(String[] args) throws Exception {
        // 1. 创建 Flink 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度为1，保证顺序处理，避免复杂的分区问题（演示环境足够）
        env.setParallelism(1);
        // 临时注释 Checkpoint（避免演示时周期性卡顿）
        // env.enableCheckpointing(300000); // 5分钟

        // ==================== Kafka Source ====================
        // 配置 Kafka 消费者，从 bus_gps_raw 主题读取原始 GPS 数据
        KafkaSource<BusGPSRecord> source = KafkaSource.<BusGPSRecord>builder()
                .setBootstrapServers("192.168.13.129:9092")   // Kafka 服务器地址
                .setTopics("bus_gps_raw")                     // 订阅的主题
                .setGroupId("bus-monitor-final-time")         // 消费者组 ID
                .setStartingOffsets(OffsetsInitializer.latest()) // 只消费最新数据，忽略历史
                .setDeserializer(new BusGPSDeserializer())      // 自定义反序列化器，JSON → POJO
                .build();

        // 使用处理时间（不需要 Watermark），窗口基于系统时钟触发
        WatermarkStrategy<BusGPSRecord> ws = WatermarkStrategy.noWatermarks();
        DataStream<BusGPSRecord> stream = env.fromSource(source, ws, "GPS Source");

        // ==================== M-01：实时在线车辆总数 ====================
        // 6秒处理时间滚动窗口，每6秒触发一次
        // 10倍速下等效数据事件时间1分钟
        stream.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(6)))
                .aggregate(new OnlineCountAggregator())   // 增量去重计数
                .map(String::valueOf)                     // 整数转字符串
                .addSink(new RedisSink("bus:indicators:online_count")); // 写入 Redis

        // ==================== M-02：各线路在线车辆 Top5 ====================
        // 36秒滑动窗口，步长12秒（10倍速下对应数据事件时间6分钟/步长2分钟）
        stream.windowAll(SlidingProcessingTimeWindows.of(Time.seconds(36), Time.seconds(12)))
                .process(new ProcessAllWindowFunction<BusGPSRecord, String, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<BusGPSRecord> elements, Collector<String> out) {
                        // 1. 按线路分组，收集车号（去重）
                        Map<String, Set<String>> lineCars = new HashMap<>();
                        for (BusGPSRecord r : elements) {
                            String line = r.getLine();
                            if (line == null || line.equals("未知")) continue;
                            lineCars.computeIfAbsent(line, k -> new HashSet<>()).add(r.getChehao());
                        }
                        // 2. 计算每条线路的车辆数，降序排列，取前5
                        List<Map.Entry<String, Integer>> sorted = lineCars.entrySet().stream()
                                .map(e -> Map.entry(e.getKey(), e.getValue().size()))
                                .sorted((a, b) -> b.getValue().compareTo(a.getValue()))
                                .limit(5)
                                .collect(Collectors.toList());
                        // 3. 构建 JSON 数组
                        StringBuilder json = new StringBuilder("[");
                        for (int i = 0; i < sorted.size(); i++) {
                            if (i > 0) json.append(",");
                            json.append(String.format("{\"line\":\"%s\",\"count\":%d}",
                                    sorted.get(i).getKey(), sorted.get(i).getValue()));
                        }
                        json.append("]");
                        out.collect(json.toString());
                    }
                })
                .addSink(new RedisSink("bus:indicators:top5_lines")); // 写入 Redis

        // ==================== M-03：全城平均速度与路况 ====================
        // 先过滤出行驶中车辆（速度>0），再使用6秒滚动窗口
        stream.filter(r -> r.getSpeed() > 0)
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(6)))
                .aggregate(new AvgSpeedAggregator())   // 增量计算平均速度
                .map(avg -> avg + "|" + (avg > 35 ? "通畅" : avg >= 25 ? "缓行" : "拥堵")) // 添加路况标签
                .addSink(new RedisSink("bus:indicators:avg_speed")); // 写入 Redis

        // ==================== M-04：车辆实时状态分布 ====================
        // 6秒滚动窗口，统计窗口内行驶、停车、离线车辆数
        stream.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(6)))
                .process(new StatusDistFunction())       // 自定义窗口函数，输出 JSON
                .addSink(new RedisSink("bus:indicators:status_dist")); // 写入 Redis

        // ==================== M-05：超速/低速实时告警 ====================
        // 逐条检测，无窗口
        DataStream<String> alertStream = stream.process(new AlertProcessFunction());

        // 双通道输出：Redis 用于前端滚动列表，Kafka 用于下游持久化
        // 1) 写入 Redis 告警列表（LPUSH 保留最近50条）
        alertStream.addSink(new RedisSink("bus:alerts:list"));
        // 2) 写入 Kafka Topic "bus_alerts"，供独立消费者（AlertConsumer）写入 MySQL
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("192.168.13.129:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("bus_alerts")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
        alertStream.sinkTo(kafkaSink);

        // ==================== M-06：地图车辆点位 ====================
        // 按车号分组，使用 KeyedProcessFunction + ValueState 维护最新位置
        stream.keyBy(BusGPSRecord::getChehao)
                .process(new VehicleStateFunction())      // 内部更新 ValueState，输出 车号|JSON
                .addSink(new RedisSink("bus:latest"));    // Redis 中存储为 bus:latest:{车号}

        // ==================== 辅助：独立时间戳 ====================
        // 提取每条数据的 GPS 时间，写入 Redis
        stream.map(r -> r.getGpsTime().toString())
                .addSink(new RedisSink("bus:latest_time"));

        // 启动作业
        env.execute("Ningjin Bus GPS Monitor - Final (No MySQL)");
    }
}