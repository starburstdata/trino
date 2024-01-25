package io.trino.execution.multi.resourcegroups;

import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;

import static io.airlift.http.client.ResponseHandlerUtils.propagate;

public class StatusCodeCheckResponseHandler
        implements ResponseHandler<Void, RuntimeException>
{

    public static StatusCodeCheckResponseHandler checkResponseStatusCode()
    {
        return new StatusCodeCheckResponseHandler();
    }

    @Override
    public Void handleException(Request request, Exception exception)
            throws RuntimeException
    {
        throw propagate(request, exception);
    }

    @Override
    public Void handle(Request request, Response response)
            throws RuntimeException
    {
        if (!isOk(response)) {
            throw new TrinoException(
                    StandardErrorCode.GENERIC_INTERNAL_ERROR,
                    "request failed with http status code: %s, request %s, response: %s".formatted(response.getStatusCode(), request, response));
        }
        return null;
    }

    private static boolean isOk(Response response)
    {
        return (response.getStatusCode() == HttpStatus.OK.code()) ||
                (response.getStatusCode() == HttpStatus.NO_CONTENT.code());
    }
}
