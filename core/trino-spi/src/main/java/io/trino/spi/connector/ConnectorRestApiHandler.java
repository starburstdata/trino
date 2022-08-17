package io.trino.spi.connector;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public interface ConnectorRestApiHandler
{
    //this will require the plugin to add the JaxrsModule and pass this to provided ServletContainer
    void service(final HttpServletRequest req, final HttpServletResponse res);
}
