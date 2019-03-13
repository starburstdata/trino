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
package io.prestosql.execution;

import io.prestosql.Session;
import io.prestosql.execution.QueryPreparer.PreparedQuery;
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.security.StatementAccessControlManager;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.StatementRewriteContext;
import io.prestosql.spi.security.StatementAccessControl;
import io.prestosql.spi.security.StatementAccessControlFactory;
import io.prestosql.sql.ParsingUtil;
import io.prestosql.sql.analyzer.SemanticException;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.tree.AllColumns;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.testing.TestingStatementAccessControlManager;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;

import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.spi.StandardErrorCode.NOT_FOUND;
import static io.prestosql.sql.QueryUtil.selectList;
import static io.prestosql.sql.QueryUtil.simpleQuery;
import static io.prestosql.sql.QueryUtil.table;
import static io.prestosql.sql.analyzer.SemanticErrorCode.INVALID_PARAMETER_USAGE;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestQueryPreparer
{
    private static final SqlParser SQL_PARSER = new SqlParser();
    private static final QueryPreparer QUERY_PREPARER = new QueryPreparer(SQL_PARSER, Optional.empty());

    @Test
    public void testSelectStatement()
    {
        PreparedQuery preparedQuery = QUERY_PREPARER.prepareQuery(TEST_SESSION, "SELECT * FROM foo");
        assertEquals(preparedQuery.getStatement(),
                simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("foo"))));
    }

    @Test
    public void testExecuteStatement()
    {
        Session session = testSessionBuilder()
                .addPreparedStatement("my_query", "SELECT * FROM foo")
                .build();
        PreparedQuery preparedQuery = QUERY_PREPARER.prepareQuery(session, "EXECUTE my_query");
        assertEquals(preparedQuery.getStatement(),
                simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("foo"))));
    }

    @Test
    public void testModifiedStatement()
    {
        StatementAccessControlManager statementAccessControlManager = new TestingStatementAccessControlManager();
        statementAccessControlManager.addStatementAccessControlFactory(new StatementAccessControlFactory() {
            public String getName()
            {
                return "TestStatementAccessControl";
            }

            public StatementAccessControl create(Map<String, String> config)
            {
                return (StatementRewriteContext statementRewriteContext, String query) ->
                {
                    QualifiedObjectName name = new QualifiedObjectName(statementRewriteContext.getCatalog().orElse(""),
                            statementRewriteContext.getSchema().orElse(""), "foo");
                    return "SELECT AGE FROM " + name + " WHERE age > 10";
                };
            }
        });

        QueryPreparer queryPreparer = new QueryPreparer(SQL_PARSER, Optional.of(statementAccessControlManager));

        Session session = testSessionBuilder()
                .addPreparedStatement("my_query", "SELECT * FROM foo")
                .build();
        QualifiedObjectName qualifiedTableName = new QualifiedObjectName(session.getCatalog().orElse(""),
                session.getSchema().orElse(""), "foo");
        PreparedQuery preparedQuery = queryPreparer.prepareQuery(session, "EXECUTE my_query");
        assertEquals(preparedQuery.getStatement(),
                SQL_PARSER.createStatement("SELECT AGE FROM " + qualifiedTableName + " WHERE age > 10", ParsingUtil.createParsingOptions(session)));
    }

    @Test
    public void testExecuteStatementDoesNotExist()
    {
        try {
            QUERY_PREPARER.prepareQuery(TEST_SESSION, "execute my_query");
            fail("expected exception");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), NOT_FOUND.toErrorCode());
        }
    }

    @Test
    public void testTooManyParameters()
    {
        try {
            Session session = testSessionBuilder()
                    .addPreparedStatement("my_query", "SELECT * FROM foo where col1 = ?")
                    .build();
            QUERY_PREPARER.prepareQuery(session, "EXECUTE my_query USING 1,2");
            fail("expected exception");
        }
        catch (SemanticException e) {
            assertEquals(e.getCode(), INVALID_PARAMETER_USAGE);
        }
    }

    @Test
    public void testTooFewParameters()
    {
        try {
            Session session = testSessionBuilder()
                    .addPreparedStatement("my_query", "SELECT ? FROM foo where col1 = ?")
                    .build();
            QUERY_PREPARER.prepareQuery(session, "EXECUTE my_query USING 1");
            fail("expected exception");
        }
        catch (SemanticException e) {
            assertEquals(e.getCode(), INVALID_PARAMETER_USAGE);
        }
    }
}
