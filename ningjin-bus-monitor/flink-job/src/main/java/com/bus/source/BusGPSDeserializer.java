package com.bus.source;

import com.bus.model.BusGPSRecord;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

public class BusGPSDeserializer implements KafkaRecordDeserializationSchema<BusGPSRecord> {
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<BusGPSRecord> out) throws IOException {
        System.out.println("DESERIALIZED, len: " + record.value().length);  // 调试输出

        String json = new String(record.value(), "UTF-8");
        var node = mapper.readTree(json);

        BusGPSRecord busRecord = new BusGPSRecord();
        busRecord.setChehao(node.get("chehao").asText());

        try {
            busRecord.setGpsTime(new Timestamp(sdf.parse(node.get("gpsTime").asText()).getTime()));
        } catch (Exception e) {
            System.err.println("PARSE FAILED: " + node.get("gpsTime").asText());
            busRecord.setGpsTime(new Timestamp(System.currentTimeMillis()));
        }

        busRecord.setLat(node.get("lat").asDouble());
        busRecord.setLng(node.get("lng").asDouble());
        busRecord.setSpeed(node.get("speed").asDouble());
        busRecord.setLine(node.get("line").asText());
        busRecord.setStation(node.has("station") ? node.get("station").asText() : "未知");

        out.collect(busRecord);
    }

    @Override
    public TypeInformation<BusGPSRecord> getProducedType() {
        return Types.POJO(BusGPSRecord.class);
    }
}