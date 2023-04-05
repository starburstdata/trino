package io.trino.operator.aggregation.partial;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import static com.google.common.base.Preconditions.checkState;

/**
 * Allows listening using {@link #allOperatorsFinished()} for all operators in a single pipeline to finish.
 */
public class OperatorExecutionMonitor
{
    private int pending;
    private boolean noMoreOperators;
    private final SettableFuture<Void> allOperatorsFinished = SettableFuture.create();

    public synchronized void operatorCreated()
    {
        checkState(!noMoreOperators);
        checkState(pending >= 0);
        pending++;
    }

    public synchronized void operatorFinished()
    {
        checkState(pending > 0);
        pending--;
        if (pending == 0 && noMoreOperators) {
            allOperatorsFinished.set(null);
        }
    }

    public synchronized void noMoreOperators()
    {
        noMoreOperators = true;
        if (pending == 0) {
            allOperatorsFinished.set(null);
        }
    }

    public ListenableFuture<Void> allOperatorsFinished()
    {
        return allOperatorsFinished;
    }
}
