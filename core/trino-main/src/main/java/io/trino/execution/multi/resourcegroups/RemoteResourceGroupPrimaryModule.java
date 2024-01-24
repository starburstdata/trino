package io.trino.execution.multi.resourcegroups;

import com.google.inject.Binder;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.execution.multi.resourcegroups.ResourceGroupEvaluationSecondaryClient.FailQueryRequest;
import io.trino.execution.multi.resourcegroups.ResourceGroupEvaluationSecondaryClient.QueryStateRequest;
import io.trino.execution.multi.resourcegroups.ResourceGroupEvaluationSecondaryClient.QueryStateResponse;

import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static io.trino.server.InternalCommunicationHttpClientModule.internalHttpClientModule;

public class RemoteResourceGroupPrimaryModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        jsonCodecBinder(binder).bindJsonCodec(FailQueryRequest.class);
        jsonCodecBinder(binder).bindJsonCodec(QueryStateRequest.class);
        jsonCodecBinder(binder).bindJsonCodec(QueryStateResponse.class);

        install(internalHttpClientModule("remote-resource-group-client", ForRemoteResourceGroupClient.class)
                .withTracing()
                .build());
        binder.bind(ResourceGroupEvaluationSecondaryClient.class).in(Singleton.class);
        binder.bind(RemoteQueryTracker.class).in(Singleton.class);
        jaxrsBinder(binder).bind(ResourceGroupEvaluationPrimaryResource.class);
    }
}
