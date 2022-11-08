package io.trino.plugin.base.metrics;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.trino.spi.metrics.ComplexMetric;
import io.trino.spi.metrics.Metric;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class MetricMap
        implements ComplexMetric<MetricMap>
{

    private final Map<String, Metric<?>> metrics;

    public static MetricMap counters(Map<String, Long> counters)
    {
        return new MetricMap(Maps.transformValues(counters, LongCount::new));
    }

    @JsonCreator
    public MetricMap(Map<String, Metric<?>> metrics)
    {
        this.metrics = ImmutableMap.copyOf(metrics);
    }

    @Override
    public MetricMap mergeWith(MetricMap other)
    {
        Map<String, Metric<?>> result = new HashMap<>(metrics);
        for (Map.Entry<String, Metric<?>> otherMetric : other.getChildMetrics()) {
            result.compute(otherMetric.getKey(), (key, existingMetric) -> existingMetric != null ? (Metric) ((Metric) existingMetric).mergeWith(otherMetric.getValue()) : otherMetric.getValue());
        }
        return new MetricMap(result);
    }

    @Override
    public Collection<Map.Entry<String, Metric<?>>> getChildMetrics()
    {
        return metrics.entrySet();
    }

    @JsonProperty("metrics")
    public Map<String, Metric<?>> getMetrics()
    {
        return metrics;
    }

    @Override
    public String toString()
    {
        return metrics.toString();
    }
}
