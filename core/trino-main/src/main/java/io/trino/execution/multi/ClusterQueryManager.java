package io.trino.execution.multi;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.util.concurrent.Futures;
import io.trino.cache.NonEvictableLoadingCache;
import io.trino.dispatcher.DispatchManager;
import io.trino.execution.QueryInfo;
import io.trino.metadata.InternalNode;
import io.trino.metadata.InternalNodeManager;
import io.trino.server.BasicQueryInfo;
import io.trino.spi.Node;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;
import jakarta.inject.Inject;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.cache.SafeCaches.buildNonEvictableCache;
import static java.util.Objects.requireNonNull;

public class ClusterQueryManager
{
    private final DispatchManager dispatchManager;

    private final InternalNodeManager internalNodeManager;

    private final InternalCoordinatorClient coordinatorClient;
    private final NonEvictableLoadingCache<QueryId, Optional<InternalNode>> queryCoordinatorCache;

    @Inject
    public ClusterQueryManager(
            DispatchManager dispatchManager,
            InternalNodeManager internalNodeManager,
            InternalCoordinatorClient coordinatorClient)
    {
        this.dispatchManager = requireNonNull(dispatchManager, "dispatchManager is null");
        this.internalNodeManager = requireNonNull(internalNodeManager, "internalNodeManager is null");
        this.coordinatorClient = requireNonNull(coordinatorClient, "coordinatorClient is null");
        this.queryCoordinatorCache = buildNonEvictableCache(CacheBuilder.newBuilder()
                .maximumSize(10000), CacheLoader.from(this::getCoordinator));
    }

    private Optional<InternalNode> getCoordinator(QueryId queryId)
    {
        Node currentNode = internalNodeManager.getCurrentNode();
        List<Future<Optional<InternalNode>>> futures = internalNodeManager.getCoordinators()
                .stream()
                .filter(node -> !node.equals(currentNode))
                .map(coordinator -> Futures.lazyTransform(
                        coordinatorClient.isQueryRegistered(coordinator, queryId),
                        isQueryRegistered -> isQueryRegistered ? Optional.of(coordinator) : Optional.<InternalNode>empty()))
                .collect(toImmutableList());

        return futures.stream().map(Futures::getUnchecked).filter(Optional::isPresent).map(Optional::get).findFirst();
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
        if (dispatchManager.isQueryRegistered(queryId)) {
            return dispatchManager.getFullQueryInfo(queryId);
        }
        return getQueryCoordinator(queryId).flatMap(coordinator -> coordinatorClient.getFullQueryInfo(coordinator, queryId));
    }

    public BasicQueryInfo getQueryInfo(QueryId queryId)
    {
        if (dispatchManager.isQueryRegistered(queryId)) {
            return dispatchManager.getQueryInfo(queryId);
        }
        return getQueryCoordinator(queryId).map(coordinator -> coordinatorClient.getQueryInfo(coordinator, queryId))
                .orElseThrow(() -> new NoSuchElementException(queryId.toString()));
    }

    public void cancelQuery(QueryId queryId)
    {
        if (dispatchManager.isQueryRegistered(queryId)) {
            dispatchManager.cancelQuery(queryId);
        }
        else {
            getQueryCoordinator(queryId).ifPresentOrElse(
                    coordinator -> coordinatorClient.cancelQuery(coordinator, queryId),
                    () -> {
                        throw new NoSuchElementException(queryId.toString());
                    });
        }
    }

    public void failQuery(QueryId queryId, TrinoException queryException)
    {
        if (dispatchManager.isQueryRegistered(queryId)) {
            dispatchManager.failQuery(queryId, queryException);
        }
        else {
            getQueryCoordinator(queryId).ifPresentOrElse(
                    coordinator -> coordinatorClient.failQuery(coordinator, queryId, queryException),
                    () -> {
                        throw new NoSuchElementException(queryId.toString());
                    });
        }
    }

    private Optional<InternalNode> getQueryCoordinator(QueryId queryId)
    {
        try {
            return queryCoordinatorCache.get(queryId);
        }
        catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
