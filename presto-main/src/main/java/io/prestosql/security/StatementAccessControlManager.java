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
package io.prestosql.security;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.prestosql.spi.StatementRewriteContext;
import io.prestosql.spi.security.AccessDeniedException;
import io.prestosql.spi.security.StatementAccessControl;
import io.prestosql.spi.security.StatementAccessControlFactory;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.prestosql.util.PropertiesUtil.loadProperties;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class StatementAccessControlManager
{
    private static final Logger log = Logger.get(io.prestosql.security.StatementAccessControlManager.class);
    private static final File STATEMENT_ACCESS_CONTROL_CONFIGURATION = new File("etc/statement-access-control.properties");
    private static final String STATEMENT_ACCESS_CONTROL_PROPERTY_NAME = "statement-access-control.name";
    private final Map<String, StatementAccessControlFactory> statementAccessControlFactories = new ConcurrentHashMap<>();
    private final AtomicReference<Optional<StatementAccessControl>> configuredStatementAccessControl = new AtomicReference<>(Optional.empty());

    public void addStatementAccessControlFactory(StatementAccessControlFactory statementAccessControlFactory)
    {
        requireNonNull(statementAccessControlFactory, "statementAccessControlFactory is null");

        if (statementAccessControlFactories.putIfAbsent(statementAccessControlFactory.getName(), statementAccessControlFactory) != null) {
            throw new IllegalArgumentException(format("Statement access control '%s' is already registered", statementAccessControlFactory.getName()));
        }
    }

    public void loadConfiguredStatementAccessControl()
            throws Exception
    {
        if (STATEMENT_ACCESS_CONTROL_CONFIGURATION.exists()) {
            Map<String, String> properties = new HashMap<>(loadProperties(STATEMENT_ACCESS_CONTROL_CONFIGURATION));

            String statementAccessControlName = properties.remove(STATEMENT_ACCESS_CONTROL_PROPERTY_NAME);
            checkArgument(!isNullOrEmpty(statementAccessControlName),
                    "Access control configuration %s does not contain %s", STATEMENT_ACCESS_CONTROL_CONFIGURATION.getAbsoluteFile(), STATEMENT_ACCESS_CONTROL_PROPERTY_NAME);

            setConfiguredStatementAccessControl(statementAccessControlName, properties);
        }
    }

    @VisibleForTesting
    protected void setConfiguredStatementAccessControl(String name, Map<String, String> properties)
    {
        requireNonNull(name, "name is null");
        requireNonNull(properties, "properties is null");

        log.info("-- Loading statement access control --");

        StatementAccessControlFactory statementAccessControlFactory = statementAccessControlFactories.get(name);
        checkState(statementAccessControlFactory != null, "Statement access control %s is not registered", name);

        StatementAccessControl statementAccessControl = statementAccessControlFactory.create(ImmutableMap.copyOf(properties));
        configuredStatementAccessControl.set(Optional.of(statementAccessControl));

        log.info("-- Loaded statement access control %s --", name);
    }

    public String getModifiedQuery(StatementRewriteContext statementRewriteContext, String query) throws AccessDeniedException
    {
        requireNonNull(statementRewriteContext, "statementRewriteContext is null");
        requireNonNull(query, "query is null");

        if (configuredStatementAccessControl.get().isPresent()) {
            return configuredStatementAccessControl.get().get().getModifiedQuery(statementRewriteContext, query);
        }
        return query;
    }
}
