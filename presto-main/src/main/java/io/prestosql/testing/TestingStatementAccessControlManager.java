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
package io.prestosql.testing;

import com.google.common.collect.ImmutableMap;
import io.prestosql.security.StatementAccessControlManager;
import io.prestosql.spi.StatementRewriteContext;
import io.prestosql.spi.security.AccessDeniedException;
import io.prestosql.spi.security.StatementAccessControl;
import io.prestosql.spi.security.StatementAccessControlFactory;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

public class TestingStatementAccessControlManager
        extends StatementAccessControlManager
{
    private final AtomicReference<Optional<StatementAccessControl>> configuredStatementAccessControl = new AtomicReference<>(Optional.empty());

    @Override
    public void addStatementAccessControlFactory(StatementAccessControlFactory statementAccessControlFactory)
    {
        configuredStatementAccessControl.set(Optional.of(statementAccessControlFactory.create(ImmutableMap.of())));
    }

    @Override
    public String getModifiedQuery(StatementRewriteContext statementRewriteContext, String query) throws AccessDeniedException
    {
        if (configuredStatementAccessControl.get().isPresent()) {
            return configuredStatementAccessControl.get().get().getModifiedQuery(statementRewriteContext, query);
        }
        return query;
    }
}
