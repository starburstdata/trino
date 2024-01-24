package io.trino.execution.multi.resourcegroups;

import com.google.inject.Inject;
import io.trino.execution.ManagedQueryExecution;
import io.trino.execution.resourcegroups.InternalResourceGroupManager;
import io.trino.execution.resourcegroups.ResourceGroupManager;
import io.trino.server.ResourceGroupInfo;
import io.trino.spi.resourcegroups.ResourceGroupConfigurationManagerFactory;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.spi.resourcegroups.SelectionContext;
import io.trino.spi.resourcegroups.SelectionCriteria;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;

import static java.util.Objects.requireNonNull;

public class RemoteResourceGroupManager<T>
        implements ResourceGroupManager<T>
{
    private final ResourceGroupManager<T> localResourceGroupManager;
    private final ResourceGroupEvaluationPrimaryClient resourceGroupEvaluationPrimaryClient;

    @Inject
    public RemoteResourceGroupManager(InternalResourceGroupManager localResourceGroupManager, ResourceGroupEvaluationPrimaryClient resourceGroupEvaluationPrimaryClient)
    {
        this.localResourceGroupManager = requireNonNull(localResourceGroupManager, "localResourceGroupManager is null");
        this.resourceGroupEvaluationPrimaryClient = requireNonNull(resourceGroupEvaluationPrimaryClient, "resourceGroupClient is null");
    }

    @Override
    public void submit(ManagedQueryExecution queryExecution, SelectionCriteria criteria, SelectionContext<T> selectionContext, Executor executor)
    {
        resourceGroupEvaluationPrimaryClient.submit(queryExecution.getBasicQueryInfo().getQueryId(), queryExecution.getQueryPriority(), criteria);
        queryExecution.addStateChangeListener(newState -> {
            resourceGroupEvaluationPrimaryClient.queryStateChanged(queryExecution);
        });
    }

    @Override
    public SelectionContext<T> selectGroup(SelectionCriteria criteria)
    {
        return localResourceGroupManager.selectGroup(criteria);
    }

    @Override
    public Optional<ResourceGroupInfo> tryGetResourceGroupInfo(ResourceGroupId id)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<List<ResourceGroupInfo>> tryGetPathToRoot(ResourceGroupId id)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addConfigurationManagerFactory(ResourceGroupConfigurationManagerFactory factory)
    {
        localResourceGroupManager.addConfigurationManagerFactory(factory);
    }

    @Override
    public void loadConfigurationManager()
            throws Exception
    {
        localResourceGroupManager.loadConfigurationManager();
    }
}
