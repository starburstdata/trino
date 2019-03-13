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
package io.prestosql;

import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.StatementRewriteContext;
import io.prestosql.spi.security.Identity;

import java.util.Map;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class FullStatementRewriteContext
        implements StatementRewriteContext
{
    private final Session session;

    public FullStatementRewriteContext(Session session)
    {
        this.session = requireNonNull(session, "session is null");
    }

    public Session getSession()
    {
        return session;
    }

    @Override
    public String getQueryId()
    {
        return session.getQueryId().toString();
    }

    @Override
    public Identity getIdentity()
    {
        return session.getIdentity();
    }

    @Override
    public Optional<String> getSource()
    {
        return session.getSource();
    }

    @Override
    public Optional<String> getCatalog()
    {
        return session.getCatalog();
    }

    @Override
    public Optional<String> getSchema()
    {
        return session.getSchema();
    }

    @Override
    public Map<String, String> getPreparedStatements()
    {
        return ImmutableMap.copyOf(session.getPreparedStatements());
    }

    @Override
    public Optional<String> getRemoteUserAddress()
    {
        return session.getRemoteUserAddress();
    }

    @Override
    public Optional<String> getUserAgent()
    {
        return session.getUserAgent();
    }

    @Override
    public Optional<String> getClientInfo()
    {
        return session.getClientInfo();
    }

    @Override
    public Optional<String> getTraceToken()
    {
        return session.getTraceToken();
    }

    @Override
    public long getStartTime()
    {
        return session.getStartTime();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("queryId", getQueryId())
                .add("principal", getIdentity().getPrincipal())
                .add("user", getIdentity().getUser())
                .add("source", getSource().orElse(null))
                .add("catalog", getCatalog())
                .add("schema", getSchema())
                .add("startTime", getStartTime())
                .add("traceToken", getTraceToken().orElse(null))
                .add("clientInfo", getClientInfo().orElse(null))
                .add("remoteUserAddress", getRemoteUserAddress().orElse(null))
                .omitNullValues()
                .toString();
    }
}
