package com.bus.process;

import com.bus.model.BusGPSRecord;
import com.bus.util.CoordinateConverter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.json.JSONObject;
import java.sql.Timestamp;

public class AlertProcessFunction extends ProcessFunction<BusGPSRecord, String> {
    @Override
    public void processElement(BusGPSRecord r, Context ctx, Collector<String> out) {
        if (r.getSpeed() > 60 || (r.getSpeed() > 0 && r.getSpeed() < 20)) {
            double[] bd = CoordinateConverter.wgs84ToBd09(r.getLng(), r.getLat());
            JSONObject alert = new JSONObject();
            alert.put("time", r.getGpsTime().toString());
            alert.put("type", r.getSpeed() > 60 ? "超速" : "低速");
            alert.put("plate", r.getChehao());
            alert.put("speed", r.getSpeed());
            alert.put("line", r.getLine());
            alert.put("lng", bd[0]);
            alert.put("lat", bd[1]);
            out.collect(alert.toString());
        }
    }
}