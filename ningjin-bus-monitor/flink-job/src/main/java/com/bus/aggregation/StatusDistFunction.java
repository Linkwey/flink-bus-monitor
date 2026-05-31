package com.bus.aggregation;

import com.bus.model.BusGPSRecord;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.util.*;

public class StatusDistFunction
        extends ProcessAllWindowFunction<BusGPSRecord, String, TimeWindow> {
    @Override
    public void process(Context ctx, Iterable<BusGPSRecord> records, Collector<String> out) {
        // 按车号取窗口内最后一条状态
        Map<String, Boolean> carMoving = new HashMap<>(); // true=行驶
        for (BusGPSRecord r : records) {
            carMoving.put(r.getChehao(), r.getSpeed() > 0);
        }
        int moving = 0, parking = 0;
        for (Boolean b : carMoving.values()) {
            if (b) moving++; else parking++;
        }
        int online = carMoving.size();
        int offline = 90 - online;
        String json = String.format("{\"moving\":%d,\"parking\":%d,\"offline\":%d}", moving, parking, offline);
        out.collect(json);
    }
}