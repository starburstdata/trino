package io.trino.operator.join;

import io.trino.spi.block.Block;
import io.trino.spi.type.Type;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class HashChannels
{
    private final List<Type> types;
    private final List<List<Block>> channels;
    private final List<Integer> hashChannels;

    public HashChannels(List<Type> types, List<List<Block>> channels, List<Integer> hashChannels)
    {
        this.types = requireNonNull(types, "types is null");
        this.channels = requireNonNull(channels, "channels is null");
        this.hashChannels = requireNonNull(hashChannels, "hashChannels is null");
    }

    public Block getHashChannel(int blockIndex, int hashChannelIndex)
    {
        int hashChannel = hashChannels.get(hashChannelIndex);
        return channels.get(hashChannel).get(blockIndex);
    }

    public boolean isSingleHashOfType(Type type)
    {
        return hashChannels.size() == 1 && types.get(hashChannels.get(0)).equals(type);
    }
}
