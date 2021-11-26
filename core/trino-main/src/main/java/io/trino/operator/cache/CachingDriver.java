package io.trino.operator.cache;

import com.google.common.collect.ImmutableList;
import io.trino.metadata.Split;
import io.trino.operator.Driver;
import io.trino.operator.DriverContext;
import io.trino.operator.OperationTimer;
import io.trino.operator.Operator;
import io.trino.operator.SourceOperator;
import io.trino.spi.Page;

import javax.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;

import static java.util.Objects.requireNonNull;

public class CachingDriver
        extends Driver
{
    private final PlanNodeSignature planSignature;
    private final PipelineResultCache resultCache;
    private final Queue<Page> results = new ArrayDeque<>();

    @Nullable
    private List<Page> onlySplitResult;
    @Nullable
    private Split onlySplit;

    public CachingDriver(DriverContext driverContext, PlanNodeSignature planSignature, List<Operator> operators)
    {
        super(driverContext, operators);
        this.planSignature = requireNonNull(planSignature, "planSignature is null");
        this.resultCache = requireNonNull(driverContext.getPipelineContext().getTaskContext().getQueryContext().getDriverResultCache(), "resultCache is null");
    }

    @Override
    protected void addSplit(SourceOperator sourceOperator, Split split)
    {
        Optional<List<Page>> cachedResult = resultCache.get(planSignature, split);
        if (cachedResult.isPresent()) {
            // cache hit, add cached result to the queue to be processed next time #processInternal is invoked
            results.addAll(cachedResult.get());
            // TODO lysy: do we have to handle deleteOperator, updateOperator?
        }
        else {
            setupOutputCache(split);
            super.addSplit(sourceOperator, split);
        }
    }

    protected void newPipelineResult(Page page)
    {
        if (onlySplitResult != null) {
            // onlySplitResult != null means we should cache the result
            onlySplitResult.add(page);
        }
    }

    // we can cache the driver results only if there is a single split being processed
    // because once split is added to the source operator there is no way of matching output page to the input split
    private void setupOutputCache(Split split)
    {
        if (onlySplit == null) {
            // if this is the first split and the caching can be done,
            // create output cache for this split assuming it will be the only one
            onlySplit = split;
            onlySplitResult = new ArrayList<>();
        }
        else {
            // we have more than one split, disable caching the results since we cannot match result page with input split
            onlySplitResult = null;
        }
    }

    @Override
    protected void driverIsFinished()
    {
        updateResultCache();
    }

    private void updateResultCache()
    {
        if (onlySplitResult != null) {
            // if Driver is finished updated result cache
            resultCache.put(planSignature, onlySplit, ImmutableList.copyOf(onlySplitResult));
            onlySplitResult = null;
            onlySplit = null;
        }
    }

    @Override
    protected void beforeProcess(OperationTimer operationTimer)
    {
        processCachedResults(operationTimer);
    }

    private void processCachedResults(OperationTimer operationTimer)
    {
        for (Page page : results) {
            addOutput(operationTimer, page);
        }
        results.clear();
    }
}
