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
package io.trino.sql.planner;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.SymbolReference;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class Symbol
        implements Comparable<Symbol> {
    private final String tableId;
    private final String columnId;
    private final String name;

    public static Symbol from(Expression expression) {
        checkArgument(expression instanceof SymbolReference, "Unexpected expression: %s", expression);
        SymbolReference symbolReference = (SymbolReference) expression;
        return new Symbol(symbolReference.getTableId(), symbolReference.getColumnId(), symbolReference.getName());
    }

    @JsonCreator
    public Symbol(
            @JsonProperty("tableId") String tableId,
            @JsonProperty("columnId") String columnId,
            @JsonProperty("name") String name)
    {
        requireNonNull(name, "name is null");
        this.name = name;
        this.columnId = columnId != null ? columnId : name;
        this.tableId = tableId;
    }

    public Symbol(String name) {
        this.name = name;
        this.columnId = name;
        this.tableId = null;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    public SymbolReference toSymbolReference()
    {
        return new SymbolReference(tableId, columnId, name);
    }

    @Override
    @JsonValue
    public String toString()
    {
        return new StringBuilder().append(tableId).append(",").append(columnId).append(",").append(name).toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Symbol symbol = (Symbol) o;

        return name.equals(symbol.name);
    }

    @Override
    public int hashCode()
    {
        return name.hashCode();
    }

    @Override
    public int compareTo(Symbol o)
    {
        return name.compareTo(o.name);
    }

    @JsonProperty
    public String getTableId()
    {
        return tableId;
    }

    @JsonProperty
    public String getColumnId()
    {
        return columnId;
    }
}
