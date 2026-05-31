package com.bus.aggregation;

import com.bus.model.BusGPSRecord;
import org.apache.flink.api.common.functions.AggregateFunction;

public class AvgSpeedAggregator implements AggregateFunction<BusGPSRecord, double[], Double> {
    @Override
    public double[] createAccumulator() { return new double[]{0, 0}; } // sum, count

    @Override
    public double[] add(BusGPSRecord r, double[] acc) {
        acc[0] += r.getSpeed();
        acc[1] += 1;
        return acc;
    }

    @Override
    public Double getResult(double[] acc) {
        return acc[1] > 0 ? acc[0] / acc[1] : 0;
    }

    @Override
    public double[] merge(double[] a, double[] b) {
        return new double[]{a[0] + b[0], a[1] + b[1]};
    }
}