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

public final class RecordReadAction
        extends AvroReadAction
{
    private final Schema readSchema;
    private final Schema writeSchema;
    private final RecordFieldReadAction[] fieldReadActions;

    public RecordReadAction(Schema readSchema, Schema writeSchema, RecordFieldReadAction[] fieldReadActions)
    {
        this.readSchema = requireNonNull(readSchema, "readSchema is null");
        this.writeSchema = requireNonNull(writeSchema, "writeSchema is null");
        this.fieldReadActions = requireNonNull(fieldReadActions, "fieldReadActions is null");
    }

    @Override
    public Schema readSchema()
    {
        return readSchema;
    }

    @Override
    public Schema writeSchema()
    {
        return writeSchema;
    }

    public RecordFieldReadAction[] getFieldReadActions()
    {
        return fieldReadActions;
    }
}
