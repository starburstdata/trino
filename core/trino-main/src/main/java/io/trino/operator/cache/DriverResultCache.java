package io.trino.operator.cache;

import com.google.common.base.MoreObjects;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.Weigher;
import com.google.common.primitives.Ints;
import io.trino.metadata.Split;
import io.trino.spi.Page;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class DriverResultCache
{
    private final Cache<DriverResultCacheKey, List<Page>> underlying;

    public DriverResultCache()
    {
        underlying = CacheBuilder.newBuilder()
                .recordStats()
                .removalListener((RemovalListener<DriverResultCacheKey, List<Page>>) notification -> {
                    System.out.println("removed " + notification);
                })
                .weigher((Weigher<DriverResultCacheKey, List<Page>>) (key, value) -> Ints.checkedCast(value.stream().mapToLong(Page::getRetainedSizeInBytes).sum()))
//                .maximumSize(1024 * 1024)
                .maximumWeight(32L * 1024 * 1024 * 1024)
                .build();
    }

    public Optional<List<Page>> get(PlanSignatureNode planSignature, Split split)
    {
        return get(new DriverResultCacheKey(planSignature, split));
    }

    public Optional<List<Page>> get(DriverResultCacheKey key)
    {
        return Optional.ofNullable(underlying.getIfPresent(key));
    }

    public void put(PlanSignatureNode planSignature, Split split, List<Page> value)
    {
        put(new DriverResultCacheKey(planSignature, split), value);
    }

    public void put(DriverResultCacheKey key, List<Page> value)
    {
        underlying.put(key, value);
        System.out.println("put: " + key.split.getConnectorSplit() + " current size: " + underlying.size() + " instance: " + this);
    }

    public static class DriverResultCacheKey
    {
        private final PlanSignatureNode planSignature;
        private final Split split;

        public DriverResultCacheKey(PlanSignatureNode planSignature, Split split)
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
            DriverResultCacheKey that = (DriverResultCacheKey) o;
            return planSignature.equals(that.planSignature) && split.getConnectorSplit().getInfo().equals(that.split.getConnectorSplit().getInfo());
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(planSignature, split.getConnectorSplit().getInfo());
        }

        @Override
        public String toString()
        {
            return MoreObjects.toStringHelper(this)
                    .add("planSignature", planSignature)
                    .add("split", split.getConnectorSplit().getInfo())
                    .toString();
        }
    }
}
