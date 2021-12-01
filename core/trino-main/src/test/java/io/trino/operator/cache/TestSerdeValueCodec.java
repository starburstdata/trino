package io.trino.operator.cache;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.block.BlockAssertions;
import io.trino.operator.cache.serde.CachePagesSerdeFactory;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.type.TestingTypeManager;
import org.testng.annotations.Test;

import java.util.List;

import static io.trino.operator.PageAssertions.assertPageEquals;
import static io.trino.spi.type.BigintType.BIGINT;

public class TestSerdeValueCodec
{
    @Test
    public void testSerde()
    {
        CachePagesSerdeFactory cachePagesSerdeFactory = new CachePagesSerdeFactory(new TestingTypeManager()::getType);
        SerdeValueCodec codec = new SerdeValueCodec(cachePagesSerdeFactory);

        Block block = BlockAssertions.createLongsBlock(1, 2, 3);
        Page page = new Page(block);
        Slice serialized = codec.serialize(List.of(page));

        List<Page> deserialized = codec.deserialize(serialized);
        assertPageEquals(ImmutableList.of(BIGINT), deserialized.get(0), page);
    }
}
