package io.trino.operator.aggregation.partial;

import com.google.common.util.concurrent.ListenableFuture;
import io.trino.operator.DriverContext;
import io.trino.operator.Operator;
import io.trino.operator.OperatorContext;
import io.trino.operator.OperatorFactory;
import io.trino.operator.WorkProcessor;
import io.trino.operator.aggregation.builder.InMemoryHashAggregationBuilder;
import io.trino.spi.Page;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class FlushPartialAggregationOperator
        implements Operator
{
    private final OperatorContext operatorContext;

    private final InMemoryHashAggregationBuilderPool inMemoryHashAggregationBuilderPool;
    private final ListenableFuture<Void> blockedFuture;
    private boolean finished;
    private WorkProcessor<Page> outputPages;
    private InMemoryHashAggregationBuilder aggregationBuilder;

    public FlushPartialAggregationOperator(
            OperatorContext operatorContext,
            InMemoryHashAggregationBuilderPool inMemoryHashAggregationBuilderPool,
            ListenableFuture<Void> blockedFuture)
    {
        this.operatorContext = operatorContext;
        this.inMemoryHashAggregationBuilderPool = inMemoryHashAggregationBuilderPool;
        this.blockedFuture = blockedFuture;
    }

    public static class FlushPartialAggregationOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final InMemoryHashAggregationBuilderPool inMemoryHashAggregationBuilderPool;
        private final ListenableFuture<Void> blockedFuture;

        public FlushPartialAggregationOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                InMemoryHashAggregationBuilderPool inMemoryHashAggregationBuilderPool,
                ListenableFuture<Void> blockedFuture)
        {
            this.operatorId = operatorId;
            this.planNodeId = planNodeId;
            this.inMemoryHashAggregationBuilderPool = requireNonNull(inMemoryHashAggregationBuilderPool, "inMemoryHashAggregationBuilderPool is null");
            this.blockedFuture = blockedFuture;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, FlushPartialAggregationOperator.class.getSimpleName());
            return new FlushPartialAggregationOperator(operatorContext, inMemoryHashAggregationBuilderPool, blockedFuture);
        }

        @Override
        public void noMoreOperators()
        {
        }

        @Override
        public OperatorFactory duplicate()
        {
            throw new UnsupportedOperationException("Source operator factories cannot be duplicated");
        }
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public boolean needsInput()
    {
        return false;
    }

    @Override
    public void addInput(Page page)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Page getOutput()
    {
        if (finished) {
            return null;
        }

        if (outputPages == null) {
            Optional<InMemoryHashAggregationBuilder> pooledBuilder = inMemoryHashAggregationBuilderPool.poll();
            if (pooledBuilder.isEmpty()) {
                // the pool is empty, the work here is done
                finished = true;
                return null;
            }
            aggregationBuilder = pooledBuilder.get();
            outputPages = aggregationBuilder.buildResult();
        }

        if (!outputPages.process()) {
            return null;
        }

        if (outputPages.isFinished()) {
            outputPages = null;
            aggregationBuilder.close();
            aggregationBuilder = null;
            // TODO lysy: release memory, can be here or when the pool is empty maybe
            return null;
        }

        return outputPages.getResult();
    }

    @Override
    public void finish()
    {
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public void close()
    {
    }

    @Override
    public ListenableFuture<Void> isBlocked()
    {
        return blockedFuture;
    }
}
