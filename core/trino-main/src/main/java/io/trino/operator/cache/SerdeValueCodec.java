package io.trino.operator.cache;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.trino.operator.cache.PipelineResultCache.PipelineResultCacheKey;
import io.trino.operator.cache.serde.PagesSerde;
import io.trino.operator.cache.serde.PagesSerdeFactory;
import io.trino.operator.cache.serde.PagesSerdeUtil;
import io.trino.spi.Page;

import java.util.Iterator;
import java.util.List;

import static io.trino.operator.cache.serde.PagesSerdeUtil.writePages;

public class SerdeValueCodec
        implements PipelineResultCache.ValueCodec<Slice>
{
    private static final Logger log = Logger.get(SerdeValueCodec.class);

    private final PagesSerdeFactory serdeFactory;

    public SerdeValueCodec(PagesSerdeFactory serdeFactory)
    {
        this.serdeFactory = serdeFactory;
    }

    @Override
    public Slice serialize(List<Page> value)
    {
        SliceOutput out = new DynamicSliceOutput(8);
        writePages(getPagesSerde(), out, value.iterator());
        Slice slice = out.slice();
        return slice;
    }

    @Override
    public List<Page> deserialize(Slice value)
    {
        Iterator<Page> pages = PagesSerdeUtil.readPages(getPagesSerde(), value.getInput());
        return ImmutableList.copyOf(pages);
    }

    private PagesSerde getPagesSerde()
    {
        return serdeFactory.createPagesSerde(true);
    }

    @Override
    public int weigh(PipelineResultCacheKey key, Slice value)
    {
        return value.length();
    }
}
