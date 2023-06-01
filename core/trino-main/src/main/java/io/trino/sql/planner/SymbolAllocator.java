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

import com.google.common.primitives.Ints;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.Type;
import io.trino.sql.analyzer.Field;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.GroupingOperation;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.SymbolReference;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class SymbolAllocator
{
    private final Map<Symbol, Type> symbols;
    private int nextId;

    public SymbolAllocator()
    {
        symbols = new HashMap<>();
    }

    public SymbolAllocator(Map<Symbol, Type> initial)
    {
        symbols = new HashMap<>(initial);
    }

    public Symbol newSymbol(Symbol symbolHint)
    {
        return newSymbol(symbolHint.getTableId(), symbolHint.getColumnId(), symbolHint, null);
    }

    public Symbol newSymbol(String tableId, String columnId, Symbol symbolHint, String suffix)
    {
        checkArgument(symbols.containsKey(symbolHint), "symbolHint not in symbols map");
        return newSymbol(tableId, columnId, symbolHint.getName(), symbols.get(symbolHint), suffix);
    }

    public Symbol newSymbol(String tableId, String columnId, String nameHint, Type type)
    {
        if (columnId == null) {
            columnId = nameHint;
        }
        return newSymbol(tableId, columnId, nameHint, type, null);
    }

    public Symbol newSymbol(String nameHint, Type type) {
        return newSymbol(null, null, nameHint, type, null);    }

    public Symbol newHashSymbol()
    {
        return newSymbol(null, null, "$hashValue", BigintType.BIGINT);
    }

    public Symbol newSymbol(String tableId, String nameHint, Type type, @Nullable String suffix) {
        return newSymbol(tableId, nameHint, nameHint, type, suffix);
    }

    public Symbol newSymbol(String tableId, String columnId, String nameHint, Type type, @Nullable String suffix)
    {
        requireNonNull(nameHint, "nameHint is null");
        requireNonNull(type, "type is null");
        // TODO: workaround for the fact that QualifiedName lowercases parts
        nameHint = nameHint.toLowerCase(ENGLISH);

        // don't strip the tail if the only _ is the first character
        int index = nameHint.lastIndexOf("_");
        if (index > 0) {
            String tail = nameHint.substring(index + 1);

            // only strip if tail is numeric or _ is the last character
            if (Ints.tryParse(tail) != null || index == nameHint.length() - 1) {
                nameHint = nameHint.substring(0, index);
            }
        }

        String unique = nameHint;

        if (suffix != null) {
            unique = unique + "$" + suffix;
        }

        Symbol symbol = new Symbol(tableId, columnId, unique);
        while (symbols.putIfAbsent(symbol, type) != null) {
            symbol = new Symbol(tableId, columnId, unique + "_" + nextId());
        }

        return symbol;
    }

    public Symbol newSymbol(Expression expression, Type type)
    {
        return newSymbol(expression, type, null);
    }

    public Symbol newSymbol(Expression expression, Type type, String suffix)
    {
        String nameHint = "expr";
        String tableId = null;
        String columnId = nameHint;
        if (expression instanceof Identifier identifier) {
            nameHint = identifier.getValue();
            columnId = nameHint;
        }
        else if (expression instanceof FunctionCall functionCall) {
            nameHint = ResolvedFunction.extractFunctionName(functionCall.getName());
            columnId = nameHint;
        }
        else if (expression instanceof SymbolReference symbolReference) {
            nameHint = symbolReference.getName();
            tableId = symbolReference.getTableId();
            columnId = symbolReference.getColumnId();
        }
        else if (expression instanceof GroupingOperation) {
            nameHint = "grouping";
            columnId = nameHint;
        }

        return newSymbol(tableId, columnId, nameHint, type, suffix);
    }

    public Symbol newSymbol(Field field)
    {
        String nameHint = field.getName().orElse("field");
        return newSymbol(null, null, nameHint, field.getType());
    }

    public Symbol newSymbol(String tableId, Field field)
    {
        String nameHint = field.getName().orElse("field");
        return newSymbol(tableId, null, nameHint, field.getType());
    }

    public TypeProvider getTypes()
    {
        return TypeProvider.viewOf(symbols);
    }

    private int nextId()
    {
        return nextId++;
    }
}
