package io.trino.execution.multi.resourcegroups;

import com.google.inject.Binder;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.execution.multi.resourcegroups.ResourceGroupEvaluationPrimaryResource.QueryResourceGroupState;
import io.trino.execution.multi.resourcegroups.ResourceGroupEvaluationPrimaryResource.SubmitQueryRequest;
import io.trino.execution.resourcegroups.ResourceGroupManager;

import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static io.trino.server.InternalCommunicationHttpClientModule.internalHttpClientModule;

public class RemoteResourceGroupSecondaryModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        jsonCodecBinder(binder).bindJsonCodec(SubmitQueryRequest.class);
        jsonCodecBinder(binder).bindJsonCodec(QueryResourceGroupState.class);
        install(internalHttpClientModule("remote-resource-group-client", ForRemoteResourceGroupClient.class)
                .withTracing()
                .build());
        binder.bind(ResourceGroupEvaluationPrimaryClient.class).in(Singleton.class);
        binder.bind(ResourceGroupManager.class).to(RemoteResourceGroupManager.class);
        jaxrsBinder(binder).bind(ResourceGroupEvaluationSecondaryResource.class);
    }
}
