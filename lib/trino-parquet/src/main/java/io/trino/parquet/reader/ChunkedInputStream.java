package io.trino.parquet.reader;

import com.google.common.collect.Iterators;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.Slices;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

import static com.google.common.base.Preconditions.checkArgument;

public class ChunkedInputStream
        extends InputStream
{
    private final Iterator<SliceInput> chunks;
    private SliceInput current;

    public static ChunkedInputStream from(Iterator<Slice> slices)
    {
        return new ChunkedInputStream(Iterators.transform(slices, Slice::getInput));
    }

    public ChunkedInputStream(Iterator<SliceInput> chunks)
    {
        this.chunks = chunks;
        this.current = chunks.next();
    }

    public Slice getSlice(int length)
    {
        if (length == 0) {
            return Slices.EMPTY_SLICE;
        }
        if (current.available() == 0) {
            checkArgument(chunks.hasNext(), "requested %s bytes but 0 was available", length);
            current = chunks.next();
        }
        if (current.available() >= length) {
            return current.readSlice(length);
        }
        // requested length crosses the slice boundary
        byte[] bytes = new byte[length];
        int offset = 0;
        int leftToRead = length;
        while (leftToRead > 0) {
            int read = current.read(bytes, offset, leftToRead);
            if (read == -1) {
                checkArgument(chunks.hasNext(), "requested %s bytes but only %s was available", length, length - leftToRead);
                current = chunks.next();
            }
            else {
                leftToRead -= read;
                offset += read;
            }
        }
        return Slices.wrappedBuffer(bytes);
    }

    @Override
    public int read()
            throws IOException
    {
        while (current.available() == 0) {
            checkArgument(chunks.hasNext(), "requested 1 byte but 0 was available");
            current = chunks.next();
        }
        return current.read();
    }
}
