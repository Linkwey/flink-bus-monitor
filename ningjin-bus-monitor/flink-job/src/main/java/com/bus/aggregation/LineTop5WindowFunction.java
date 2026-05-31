package com.bus.aggregation;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.util.*;
import java.util.stream.Collectors;

public class LineTop5WindowFunction
        extends ProcessWindowFunction<Integer, String, String, TimeWindow> {
    @Override
    public void process(String line, Context ctx, Iterable<Integer> counts, Collector<String> out) {
        int count = 0;
        for (Integer c : counts) {
            count = c;
        }
        // 输出单条线路的计数，格式：线路,车辆数
        out.collect(line + "|" + count);
    }
}