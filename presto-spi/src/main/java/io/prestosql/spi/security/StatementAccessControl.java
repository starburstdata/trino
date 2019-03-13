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
package io.prestosql.spi.security;

import io.prestosql.spi.Plugin;
import io.prestosql.spi.StatementRewriteContext;

/**
 * A <code>StatementAccessControl</code> modifies the input SQL query as a
 * first step during query processing in Presto co-ordinator.
 * This can be leveraged by any third party data access control provider to
 * return a modified SQL query as per their data masking and filtering needs.
 * <p>
 * A third party provider has to implement a new Presto plugin and override {@link Plugin#getStatementAccessControlFactories()}
 */
public interface StatementAccessControl
{
    /**
     * Modifies the SQL query
     * @param statementRewriteContext contains {@link Identity} information,
     *        the default catalog and schema etc. useful for ACL processing
     * @param query to be processed
     * @return modified query
     * @throws AccessDeniedException if the Identity does not have sufficient ACLs to
     *         execute any of DDL or DML queries.
     */
    String getModifiedQuery(StatementRewriteContext statementRewriteContext, String query) throws AccessDeniedException;
}
