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
package io.trino.plugin.hive.cache.serde;

import io.trino.spi.block.ArrayBlockEncoding;
import io.trino.spi.block.BlockEncoding;
import io.trino.spi.block.ByteArrayBlockEncoding;
import io.trino.spi.block.DictionaryBlockEncoding;
import io.trino.spi.block.Int128ArrayBlockEncoding;
import io.trino.spi.block.Int96ArrayBlockEncoding;
import io.trino.spi.block.IntArrayBlockEncoding;
import io.trino.spi.block.LazyBlockEncoding;
import io.trino.spi.block.LongArrayBlockEncoding;
import io.trino.spi.block.MapBlockEncoding;
import io.trino.spi.block.RowBlockEncoding;
import io.trino.spi.block.RunLengthBlockEncoding;
import io.trino.spi.block.ShortArrayBlockEncoding;
import io.trino.spi.block.SingleMapBlockEncoding;
import io.trino.spi.block.SingleRowBlockEncoding;
import io.trino.spi.block.VariableWidthBlockEncoding;
import io.trino.spi.type.TypeManager;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class CachePagesSerdeFactory
        extends PagesSerdeFactory
{
    private static final ConcurrentMap<String, BlockEncoding> BLOCK_ENCODINGS = new ConcurrentHashMap<>();

    static {
        // add the built-in BlockEncodings
        addBlockEncoding(new VariableWidthBlockEncoding());
        addBlockEncoding(new ByteArrayBlockEncoding());
        addBlockEncoding(new ShortArrayBlockEncoding());
        addBlockEncoding(new IntArrayBlockEncoding());
        addBlockEncoding(new LongArrayBlockEncoding());
        addBlockEncoding(new Int96ArrayBlockEncoding());
        addBlockEncoding(new Int128ArrayBlockEncoding());
        addBlockEncoding(new DictionaryBlockEncoding());
        addBlockEncoding(new ArrayBlockEncoding());
        addBlockEncoding(new MapBlockEncoding());
        addBlockEncoding(new SingleMapBlockEncoding());
        addBlockEncoding(new RowBlockEncoding());
        addBlockEncoding(new SingleRowBlockEncoding());
        addBlockEncoding(new RunLengthBlockEncoding());
        addBlockEncoding(new LazyBlockEncoding());
    }

    public CachePagesSerdeFactory(TypeManager typeManager)
    {
        // compression should be enabled in as many tests as possible
        super(new InternalBlockEncodingSerde(CachePagesSerdeFactory::getBlockEncoding, typeManager::getType));
    }

    private static void addBlockEncoding(BlockEncoding blockEncoding)
    {
        requireNonNull(blockEncoding, "blockEncoding is null");
        BlockEncoding existingEntry = BLOCK_ENCODINGS.putIfAbsent(blockEncoding.getName(), blockEncoding);
        checkArgument(existingEntry == null, "Encoding already registered: %s", blockEncoding.getName());
    }

    private static BlockEncoding getBlockEncoding(String encodingName)
    {
        BlockEncoding blockEncoding = BLOCK_ENCODINGS.get(encodingName);
        checkArgument(blockEncoding != null, "Unknown block encoding: %s", encodingName);
        return blockEncoding;
    }
}
