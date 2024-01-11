package io.trino.execution.multi;

import com.google.inject.Binder;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.server.BasicQueryInfo;

import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static io.trino.server.InternalCommunicationHttpClientModule.internalHttpClientModule;

public class CurrentQueryProviderModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        jsonCodecBinder(binder).bindListJsonCodec(BasicQueryInfo.class);
        install(internalHttpClientModule("coordinator-client", ForCoordinatorClient.class)
                .withTracing()
                .build());
        binder.bind(CoordinatorClient.class).in(Singleton.class);
        binder.bind(CurrentQueryProvider.class).in(Singleton.class);
    }
}
