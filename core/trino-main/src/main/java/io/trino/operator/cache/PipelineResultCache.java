package io.trino.operator.cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;
import com.google.common.primitives.Ints;
import io.trino.metadata.Split;
import io.trino.operator.cache.serde.PagesSerdeFactory;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class PipelineResultCache<T>
{
    private final Cache<PipelineResultCacheKey, T> underlying;
    private final ValueCodec<T> valueCodec;

    private PipelineResultCache(ValueCodec<T> valueCodec)
    {
        this.valueCodec = valueCodec;
        underlying = CacheBuilder.newBuilder()
                .recordStats()
                .softValues()
                .weigher(valueCodec)
                .maximumWeight(200L * 1024 * 1024 * 1024)
                .build();
    }



    public static PipelineResultCache<?> uncompressed()
    {
        return new PipelineResultCache<>(new DirectValueCodec());
    }

    public static PipelineResultCache<?> compressed(PagesSerdeFactory pagesSerdeFactory)
    {
        return new PipelineResultCache<>(new SerdeValueCodec(pagesSerdeFactory));
    }

    public Optional<List<Page>> get(PlanNodeSignature planSignature, Split split)
    {
        return get(new PipelineResultCacheKey(planSignature, split));
    }

    public Optional<List<Page>> get(PipelineResultCacheKey key)
    {
        Optional<T> value = Optional.ofNullable(underlying.getIfPresent(key));
        return value.map(valueCodec::deserialize);
    }

    public void put(PlanNodeSignature planSignature, Split split, List<Page> value)
    {
        put(new PipelineResultCacheKey(planSignature, split), value);
    }

    public void put(PipelineResultCacheKey key, List<Page> value)
    {
        T serialized = valueCodec.serialize(value);
        underlying.put(key, serialized);
    }

    private static Object signature(Split split)
    {
        ConnectorSplit connectorSplit = split.getConnectorSplit();
        Object signature = connectorSplit.getSignature();
        return signature;
    }

    public static class PipelineResultCacheKey
    {
        private final PlanNodeSignature planSignature;

        private final Split split;

        public PipelineResultCacheKey(PlanNodeSignature planSignature, Split split)
        {
            this.planSignature = requireNonNull(planSignature, "planSignature is null");
            this.split = requireNonNull(split, "split is null");
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PipelineResultCacheKey that = (PipelineResultCacheKey) o;
            return planSignature.equals(that.planSignature) && signature(split).equals(signature(that.split));
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(
                    planSignature,
                    signature(split));
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("planSignature", planSignature)
                    .add("split", signature(split))
                    .toString();
        }
    }

    interface ValueCodec<T>
            extends Weigher<PipelineResultCacheKey, T>
    {
        T serialize(List<Page> value);

        List<Page> deserialize(T value);
    }

    private static class DirectValueCodec
            implements ValueCodec<List<Page>>
    {
        @Override
        public List<Page> serialize(List<Page> value)
        {
            return value;
        }

        @Override
        public List<Page> deserialize(List<Page> value)
        {
            return value;
        }

        @Override
        public int weigh(PipelineResultCacheKey key, List<Page> value)
        {
            return Ints.checkedCast(value.stream().mapToLong(Page::getRetainedSizeInBytes).sum());
        }
    }
}
