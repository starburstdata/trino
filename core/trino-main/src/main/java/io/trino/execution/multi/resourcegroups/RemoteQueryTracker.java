package io.trino.execution.multi.resourcegroups;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.execution.multi.resourcegroups.RemoteManagedQueryExecution.RemoteQueryState;
import io.trino.metadata.InternalNode;
import io.trino.spi.QueryId;
import jakarta.annotation.PreDestroy;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.execution.QueryState.RUNNING;
import static java.util.Objects.requireNonNull;

public class RemoteQueryTracker
        implements Closeable
{
    private static final Logger log = Logger.get(RemoteQueryTracker.class);
    private final Map<QueryId, RemoteManagedQueryExecution> queries;
    private final ResourceGroupEvaluationSecondaryClient resourceGroupEvaluationSecondaryClient;
    private final ScheduledExecutorService refreshExecutor;

    @Inject
    public RemoteQueryTracker(ResourceGroupEvaluationSecondaryClient resourceGroupEvaluationSecondaryClient)
    {
        this.resourceGroupEvaluationSecondaryClient = requireNonNull(resourceGroupEvaluationSecondaryClient, "resourceGroupEvaluationSecondaryClient is null");
        this.queries = new ConcurrentHashMap<>();
        this.refreshExecutor = Executors.newScheduledThreadPool(2, daemonThreadsNamed("remote-query-tracker-refresh"));
        this.refreshExecutor.scheduleAtFixedRate(this::refreshState, 1000, 100, TimeUnit.MILLISECONDS);
    }

    public void add(RemoteManagedQueryExecution remoteManagedQueryExecution)
    {
        queries.put(remoteManagedQueryExecution.getQueryId(), remoteManagedQueryExecution);
    }

    public RemoteManagedQueryExecution get(QueryId queryId)
    {
        return requireNonNull(queries.get(queryId), "query not found " + queryId);
    }

    private void refreshState()
    {
        Multimap<InternalNode, QueryId> runningQueriesByCoordinator = ArrayListMultimap.create();
        for (RemoteManagedQueryExecution query : queries.values()) {
            if (query.getState().equals(RUNNING)) {
                // only running queries use cpu and memory
                runningQueriesByCoordinator.put(query.getCoordinator(), query.getQueryId());
            }
        }

        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (Map.Entry<InternalNode, Collection<QueryId>> coordinatorQueries : runningQueriesByCoordinator.asMap().entrySet()) {
            if (coordinatorQueries.getValue().isEmpty()) {
                continue;
            }
            CompletableFuture<Map<QueryId, RemoteQueryState>> future = resourceGroupEvaluationSecondaryClient.getQueryState(coordinatorQueries.getKey(), coordinatorQueries.getValue());
            futures.add(future.handleAsync(
                    (result, exception) ->
                    {
                        if (exception != null) {
                            log.warn(exception, "refresh state failed for %s", coordinatorQueries.getKey());
                        }
                        else {
                            result.forEach((queryId, remoteState) -> get(queryId).setRemoteQueryState(remoteState));
                        }
                        return null;
                    },
                    refreshExecutor));
        }
        // wait for the results to be processed so that refresh requests will not overlap
        futures.forEach(future -> {
            try {
                future.get();
            }
            catch (InterruptedException | ExecutionException e) {
                // should not happen as we handle exceptions in handleAsync above
                throw new RuntimeException(e);
            }
        });
    }

    @PreDestroy
    public void close()
    {
        refreshExecutor.shutdownNow();
    }
}
