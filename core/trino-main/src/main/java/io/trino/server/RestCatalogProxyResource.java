/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.server;

import io.trino.connector.CatalogServiceProvider;
import io.trino.metadata.Catalog;
import io.trino.metadata.CatalogManager;
import io.trino.server.security.ResourceSecurity;
import io.trino.spi.connector.ConnectorRestApiHandler;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletResponseWrapper;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.Optional;

import static io.trino.server.security.ResourceSecurity.AccessType.AUTHENTICATED_USER;
import static java.util.Objects.requireNonNull;

@Path("/v1/catalog-proxy")
public class RestCatalogProxyResource
{
    private final CatalogManager catalogManager;
    private final CatalogServiceProvider<Optional<ConnectorRestApiHandler>> catalogServiceProvider;

    @Inject
    public RestCatalogProxyResource(CatalogManager catalogManager,
            CatalogServiceProvider<Optional<ConnectorRestApiHandler>> catalogServiceProvider)
    {
        this.catalogManager = requireNonNull(catalogManager, "catalogManager is null");
        this.catalogServiceProvider = requireNonNull(catalogServiceProvider, "catalogServiceProvider is null");
    }

    @ResourceSecurity(AUTHENTICATED_USER)
    @GET
    @Path("{catalog}")
    public Response proxyGet(
            @PathParam("catalog") String catalogName,
            @Context HttpServletRequest servletRequest,
            @Context HttpServletResponse servletResponse)
    {
        return proxyJsonResponse(catalogName, servletRequest, servletResponse);
    }

    @ResourceSecurity(AUTHENTICATED_USER)
    @POST
    @Path("{catalog}")
    public Response proxyPost(
            @PathParam("catalog") String catalogName,
            @Context HttpServletRequest servletRequest,
            @Context HttpServletResponse servletResponse)
    {
        return proxyJsonResponse(catalogName, servletRequest, servletResponse);
    }

    @ResourceSecurity(AUTHENTICATED_USER)
    @DELETE
    @Path("{catalog}")
    public Response proxyDelete(
            @PathParam("catalog") String catalogName,
            @Context HttpServletRequest servletRequest,
            @Context HttpServletResponse servletResponse)
    {
        return proxyJsonResponse(catalogName, servletRequest, servletResponse);
    }

    private Response proxyJsonResponse(String catalogName,
            HttpServletRequest servletRequest,
            HttpServletResponse servletResponse)
    {
        Catalog catalog = catalogManager.getCatalog(catalogName)
                .orElseThrow(() -> new IllegalArgumentException("No catalog registered for : " + catalogName));

        catalogServiceProvider.getService(catalog.getCatalogHandle())
                .orElseThrow(() -> new IllegalArgumentException("No rest-api registered for : " + catalogName))
                .service(servletRequest, servletResponse);
        try {
            final CopyPrintWriter writer = new CopyPrintWriter(servletResponse.getWriter());
            HttpServletResponseWrapper httpServletResponseWrapper = new HttpServletResponseWrapper(servletResponse)
            {
                @Override
                public PrintWriter getWriter()
                {
                    return writer;
                }
            };
            return Response.status(servletResponse.getStatus())
                    .entity(((CopyPrintWriter) httpServletResponseWrapper.getWriter()).getCopy())
                    .build();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static class CopyPrintWriter
            extends PrintWriter
    {

        private final StringBuilder copy = new StringBuilder();

        public CopyPrintWriter(Writer writer)
        {
            super(writer);
        }

        @Override
        public void write(int c)
        {
            copy.append((char) c); // It is actually a char, not an int.
            super.write(c);
        }

        @Override
        public void write(char[] chars, int offset, int length)
        {
            copy.append(chars, offset, length);
            super.write(chars, offset, length);
        }

        @Override
        public void write(String string, int offset, int length)
        {
            copy.append(string, offset, length);
            super.write(string, offset, length);
        }

        public String getCopy()
        {
            return copy.toString();
        }
    }
}
