package io.trino.execution.multi;

import com.google.inject.Binder;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.execution.QueryInfo;
import io.trino.execution.multi.resourcegroups.ResourceGroupEvaluationSecondaryClient;
import io.trino.server.BasicQueryInfo;

import java.util.Optional;

import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static io.trino.server.InternalCommunicationHttpClientModule.internalHttpClientModule;

public class ClusterQueryManagerModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        jsonCodecBinder(binder).bindListJsonCodec(BasicQueryInfo.class);
        jsonCodecBinder(binder).bindJsonCodec(BasicQueryInfo.class);
        jsonCodecBinder(binder).bindJsonCodec(new TypeLiteral<Optional<QueryInfo>>() {});
        jsonCodecBinder(binder).bindJsonCodec(InternalCoordinatorClient.IsQueryRegisteredResponse.class);
        jsonCodecBinder(binder).bindJsonCodec(ResourceGroupEvaluationSecondaryClient.FailQueryRequest.class);
        install(internalHttpClientModule("coordinator-client", ForCoordinatorClient.class)
                .withTracing()
                .build());
        binder.bind(InternalCoordinatorClient.class).in(Singleton.class);
        binder.bind(ClusterQueryManager.class).in(Singleton.class);
        jaxrsBinder(binder).bind(InternalCoordinatorResource.class);
    }
}
