package com.bus.process;

import com.bus.model.BusGPSRecord;
import com.bus.util.CoordinateConverter;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.json.JSONObject;

public class VehicleStateFunction extends KeyedProcessFunction<String, BusGPSRecord, String> {
    private ValueState<String> vehicleState;

    @Override
    public void open(Configuration conf) {
        vehicleState = getRuntimeContext().getState(new ValueStateDescriptor<>("vehicle", String.class));
    }

    @Override
    public void processElement(BusGPSRecord r, Context ctx, Collector<String> out) throws Exception {
        double[] bd = CoordinateConverter.wgs84ToBd09(r.getLng(), r.getLat());
        JSONObject obj = new JSONObject();
        obj.put("chehao", r.getChehao());
        obj.put("lat", bd[1]);
        obj.put("lng", bd[0]);
        obj.put("speed", r.getSpeed());
        obj.put("line", r.getLine());
        obj.put("status", r.getSpeed() > 0 ? "moving" : "parking");

        String jsonStr = obj.toString();
        vehicleState.update(jsonStr);
        out.collect(r.getChehao() + "|" + jsonStr);
    }
}