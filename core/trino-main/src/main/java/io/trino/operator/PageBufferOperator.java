package io.trino.operator;

import io.trino.spi.Page;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.ArrayDeque;
import java.util.Queue;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class PageBufferOperator
        implements Operator
{
    public static class PageBufferOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final int maxBufferSize;

        public PageBufferOperatorFactory(int operatorId, PlanNodeId planNodeId, int maxBufferSize)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            checkArgument(maxBufferSize > 0, "maxBufferSize must be positive but got %s", maxBufferSize);
            this.maxBufferSize = maxBufferSize;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            return new PageBufferOperator(
                    driverContext.addOperatorContext(operatorId, planNodeId, PageBufferOperator.class.getSimpleName()),
                    maxBufferSize);
        }

        @Override
        public void noMoreOperators()
        {
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new PageBufferOperatorFactory(operatorId, planNodeId, maxBufferSize);
        }
    }

    private final OperatorContext operatorContext;
    private final Queue<Page> buffer;
    private final int maxBufferSize;
    private boolean finishing;

    public PageBufferOperator(OperatorContext operatorContext, int maxBufferSize)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.maxBufferSize = maxBufferSize;
        this.buffer = new ArrayDeque<>(maxBufferSize);
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public boolean needsInput()
    {
        return !finishing && buffer.size() < maxBufferSize;
    }

    @Override
    public void addInput(Page page)
    {
        buffer.add(page);
    }

    @Override
    public Page getOutput()
    {
        return buffer.poll();
    }

    @Override
    public void finish()
    {
        finishing = true;
    }

    @Override
    public boolean isFinished()
    {
        return finishing && buffer.isEmpty();
    }
}
