package io.trino.operator.cache;

import com.google.common.base.MoreObjects;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;
import com.google.common.primitives.Ints;
import io.trino.metadata.Split;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class PipelineResultCache
{
    private final Cache<PipelineResultCacheKey, List<Page>> underlying;

    public PipelineResultCache()
    {
        underlying = CacheBuilder.newBuilder()
                .recordStats()
                .softValues()
                .weigher((Weigher<PipelineResultCacheKey, List<Page>>) (key, value) -> Ints.checkedCast(value.stream().mapToLong(Page::getRetainedSizeInBytes).sum()))
                .maximumWeight(128L * 1024 * 1024 * 1024)
                .build();
    }

    public Optional<List<Page>> get(PlanNodeSignature planSignature, Split split)
    {
        return get(new PipelineResultCacheKey(planSignature, split));
    }

    public Optional<List<Page>> get(PipelineResultCacheKey key)
    {
        return Optional.ofNullable(underlying.getIfPresent(key));
    }

    public void put(PlanNodeSignature planSignature, Split split, List<Page> value)
    {
        put(new PipelineResultCacheKey(planSignature, split), value);
    }

    public void put(PipelineResultCacheKey key, List<Page> value)
    {
        underlying.put(key, value);
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
            int planSignatureHash = planSignature.hashCode();
            int signatureHash = signature(split).hashCode();
            int hash = Objects.hash(
                    planSignatureHash,
                    signatureHash
            );
            return hash;
        }

        @Override
        public String toString()
        {
            return MoreObjects.toStringHelper(this)
                    .add("planSignature", planSignature)
                    .add("split", signature(split))
                    .toString();
        }
    }
}
