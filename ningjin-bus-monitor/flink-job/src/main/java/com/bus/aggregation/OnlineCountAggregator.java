package com.bus.aggregation;

import com.bus.model.BusGPSRecord;
import org.apache.flink.api.common.functions.AggregateFunction;
import java.util.HashSet;

public class OnlineCountAggregator implements AggregateFunction<BusGPSRecord, HashSet<String>, Integer> {

    @Override
    public HashSet<String> createAccumulator() {
        return new HashSet<>();
    }

    @Override
    public HashSet<String> add(BusGPSRecord value, HashSet<String> accumulator) {
        accumulator.add(value.getChehao());
        System.out.println(">>> ADD: vehicle=" + value.getChehao() + ", set size=" + accumulator.size());
        return accumulator;
    }

    @Override
    public Integer getResult(HashSet<String> accumulator) {
        int size = accumulator.size();
        System.out.println(">>> WINDOW TRIGGERED, online count = " + size);
        return size;
    }

    @Override
    public HashSet<String> merge(HashSet<String> a, HashSet<String> b) {
        a.addAll(b);
        return a;
    }
}