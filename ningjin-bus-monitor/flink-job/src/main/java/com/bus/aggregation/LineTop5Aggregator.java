package com.bus.aggregation;

import com.bus.model.BusGPSRecord;
import org.apache.flink.api.common.functions.AggregateFunction;
import java.util.HashSet;

public class LineTop5Aggregator implements AggregateFunction<BusGPSRecord, HashSet<String>, Integer> {
    @Override
    public HashSet<String> createAccumulator() { return new HashSet<>(); }

    @Override
    public HashSet<String> add(BusGPSRecord r, HashSet<String> acc) {
        acc.add(r.getChehao());
        return acc;
    }

    @Override
    public Integer getResult(HashSet<String> acc) { return acc.size(); }

    @Override
    public HashSet<String> merge(HashSet<String> a, HashSet<String> b) {
        a.addAll(b);
        return a;
    }
}