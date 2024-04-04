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
package io.trino.hive.formats.avro.model;

import org.apache.avro.Schema;

import static java.util.Objects.requireNonNull;

public final class DefaultValueFieldRecordFieldReadAction
        extends RecordFieldReadAction
{
    private final Schema fieldSchema;
    //avro bytes rep of the default value
    private final byte[] defaultBytes;
    private final int outputChannel;

    DefaultValueFieldRecordFieldReadAction(Schema fieldSchema, byte[] defaultBytes, int outputChannel)
    {
        this.fieldSchema = requireNonNull(fieldSchema, "fieldSchema is null");
        this.defaultBytes = requireNonNull(defaultBytes, "defaultBytes is null");
        this.outputChannel = outputChannel;
    }

    public Schema getFieldSchema()
    {
        return fieldSchema;
    }

    public byte[] getDefaultBytes()
    {
        return defaultBytes;
    }

    public int getOutputChannel()
    {
        return outputChannel;
    }
}
