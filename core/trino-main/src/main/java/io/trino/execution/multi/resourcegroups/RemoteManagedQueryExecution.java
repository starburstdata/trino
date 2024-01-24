package io.trino.execution.multi.resourcegroups;

import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.execution.ManagedQueryExecution;
import io.trino.execution.QueryInfo;
import io.trino.execution.QueryState;
import io.trino.execution.StateMachine;
import io.trino.metadata.InternalNode;
import io.trino.server.BasicQueryInfo;
import io.trino.spi.ErrorCode;
import io.trino.spi.QueryId;
import org.joda.time.DateTime;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class RemoteManagedQueryExecution
        implements ManagedQueryExecution
{
    private final ResourceGroupEvaluationSecondaryClient resourceGroupEvaluationSecondaryClient;
    private final InternalNode coordinator;
    private final QueryId queryId;
    private final int queryPriority;

    private volatile RemoteQueryState remoteQueryState;
    private final List<Runnable> doneCallbacks = new ArrayList<>();
    private final Object stateLock = new Object();

    public RemoteManagedQueryExecution(
            ResourceGroupEvaluationSecondaryClient resourceGroupEvaluationSecondaryClient,
            InternalNode coordinator,
            QueryId queryId,
            int queryPriority)
    {
        this.resourceGroupEvaluationSecondaryClient = requireNonNull(resourceGroupEvaluationSecondaryClient, "resourceGroupEvaluationSecondaryClient is null");
        this.coordinator = requireNonNull(coordinator, "coordinator is null");
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.queryPriority = queryPriority;
        this.remoteQueryState = new RemoteQueryState(QueryState.DISPATCHING, Duration.ZERO, DataSize.ofBytes(0), DateTime.now());
    }

    @Override
    public void startWaitingForResources()
    {
        resourceGroupEvaluationSecondaryClient.startWaitingForResources(coordinator, queryId);
    }

    @Override
    public void fail(Throwable cause)
    {
        resourceGroupEvaluationSecondaryClient.fail(coordinator, queryId, cause);
    }

    public QueryId getQueryId()
    {
        return queryId;
    }

    public InternalNode getCoordinator()
    {
        return coordinator;
    }

    @Override
    public int getQueryPriority()
    {
        return queryPriority;
    }

    @Override
    public DataSize getTotalMemoryReservation()
    {
        return remoteQueryState.totalMemoryReservation;
    }

    @Override
    public Duration getTotalCpuTime()
    {
        return remoteQueryState.totalCpuTime;
    }

    @Override
    public QueryState getState()
    {
        return remoteQueryState.state;
    }

    @Override
    public boolean isDone()
    {
        return getState().isDone();
    }

    public void setRemoteQueryState(RemoteQueryState newState)
    {
        synchronized (stateLock) {
            remoteQueryState = newState;
            if (remoteQueryState.state.isDone()) {
                doneCallbacks.forEach(Runnable::run);
                doneCallbacks.clear();
            }
        }
    }

    @Override
    public void addDoneCallback(Runnable callback)
    {
        synchronized (stateLock) {
            if (remoteQueryState.state.isDone()) {
                callback.run();
            }
            doneCallbacks.add(callback);
        }
    }

    @Override
    public void addStateChangeListener(StateMachine.StateChangeListener<QueryState> stateChangeListener)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public BasicQueryInfo getBasicQueryInfo()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryInfo getFullQueryInfo()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<ErrorCode> getErrorCode()
    {
        throw new UnsupportedOperationException();
    }

    public record RemoteQueryState(QueryState state, Duration totalCpuTime, DataSize totalMemoryReservation, DateTime updatedAt)
    {
        public RemoteQueryState
        {
            requireNonNull(state, "state is null");
            requireNonNull(totalCpuTime, "totalCpuTime is null");
            requireNonNull(totalMemoryReservation, "totalMemoryReservation is null");
            requireNonNull(updatedAt, "updatedAt is null");
        }
    }
}
