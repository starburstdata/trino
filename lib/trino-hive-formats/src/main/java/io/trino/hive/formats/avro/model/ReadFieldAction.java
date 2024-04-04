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

import static java.util.Objects.requireNonNull;

public final class ReadFieldAction
        extends RecordFieldReadAction
{
    private final AvroReadAction readAction;
    private final int outputChannel;

    public ReadFieldAction(AvroReadAction readAction, int outputChannel)
    {
        this.readAction = requireNonNull(readAction, "readAction is null");
        this.outputChannel = outputChannel;
    }

    public AvroReadAction getReadAction()
    {
        return readAction;
    }

    public int getOutputChannel()
    {
        return outputChannel;
    }
}
