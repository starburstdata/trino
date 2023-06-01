package io.trino.server.protocol;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.*;
import io.trino.sql.planner.Symbol;

import java.io.IOException;

public class SymbolSerialization {
    private static final String SEPARATOR = ",";

    private SymbolSerialization() {
    }

    public static class SymbolDeserializer
            extends JsonDeserializer<Symbol> {
        public SymbolDeserializer() {
        }

        @Override
        public Symbol deserialize(JsonParser p, DeserializationContext ctxt) throws IOException{
            String[] parts = p.getValueAsString().split(SEPARATOR);
            return new Symbol(parts[0], parts[1], parts[2]);
        }
    }

    public static class SymbolKeyDeserializer
            extends KeyDeserializer {
        @Override
        public Symbol deserializeKey(String key, DeserializationContext deserializationContext)
        {
            String[] parts = key.split(SEPARATOR);
            return new Symbol(parts[0], parts[1], parts[2]);
        }
    }
}
