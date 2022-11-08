package io.trino.spi.metrics;

import java.util.Collection;
import java.util.Map;

public interface ComplexMetric<T>
        extends Metric<T>
{
    Collection<Map.Entry<String, Metric<?>>> getChildMetrics();
}
