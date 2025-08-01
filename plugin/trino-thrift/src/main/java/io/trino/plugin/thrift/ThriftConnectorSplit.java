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
package io.trino.plugin.thrift;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.plugin.thrift.api.TrinoThriftId;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;

import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;

public record ThriftConnectorSplit(TrinoThriftId splitId, List<HostAddress> addresses)
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = instanceSize(ThriftConnectorSplit.class);

    public ThriftConnectorSplit
    {
        requireNonNull(splitId, "splitId is null");
        addresses = ImmutableList.copyOf(addresses);
    }

    @Override
    @JsonProperty
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + splitId.getRetainedSizeInBytes()
                + estimatedSizeOf(addresses, HostAddress::getRetainedSizeInBytes);
    }
}
