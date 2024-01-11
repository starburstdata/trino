package io.trino.execution.multi;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import io.trino.dispatcher.DispatchManager;
import io.trino.execution.QueryInfo;
import io.trino.metadata.InternalNodeManager;
import io.trino.server.BasicQueryInfo;
import io.trino.spi.Node;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;
import jakarta.inject.Inject;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Future;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class CurrentQueryProvider
{
    private final DispatchManager dispatchManager;

    private final InternalNodeManager internalNodeManager;

    private final CoordinatorClient coordinatorClient;

    @Inject
    public CurrentQueryProvider(
            DispatchManager dispatchManager,
            InternalNodeManager internalNodeManager,
            CoordinatorClient coordinatorClient)
    {
        this.dispatchManager = requireNonNull(dispatchManager, "dispatchManager is null");
        this.internalNodeManager = requireNonNull(internalNodeManager, "internalNodeManager is null");
        this.coordinatorClient = requireNonNull(coordinatorClient, "coordinatorClient is null");
    }

    public List<BasicQueryInfo> getQueries()
    {
        Node currentNode = internalNodeManager.getCurrentNode();
        List<BasicQueryInfo> queries = new ArrayList<>(dispatchManager.getQueries());
        List<Future<List<BasicQueryInfo>>> futures = internalNodeManager.getCoordinators()
                .stream()
                .filter(node -> !node.equals(currentNode))
                .map(coordinatorClient::getAllLocalQueryInfo)
                .collect(toImmutableList());

        futures.forEach(future -> queries.addAll(Futures.getUnchecked(future)));
        return queries
                .stream()
                .sorted(Comparator.comparing((BasicQueryInfo info) -> info.getQueryStats().getCreateTime()).reversed())
                .collect(toImmutableList());
    }

    public Optional<QueryInfo> getFullQueryInfo(QueryId queryId)
    {
        // TODO lysy: support remote queries
        return dispatchManager.getFullQueryInfo(queryId);
    }

    public BasicQueryInfo getQueryInfo(QueryId queryId)
    {
        // TODO lysy: support remote queries
        return dispatchManager.getQueryInfo(queryId);
    }

    public void cancelQuery(QueryId queryId)
    {
        // TODO lysy: support remote queries
        dispatchManager.cancelQuery(queryId);
    }

    public void failQuery(QueryId queryId, TrinoException queryException)
    {
        // TODO lysy: support remote queries
        dispatchManager.failQuery(queryId, queryException);
    }
}
