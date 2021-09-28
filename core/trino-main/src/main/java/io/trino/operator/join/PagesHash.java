package io.trino.operator.join;

import io.trino.spi.Page;
import io.trino.spi.PageBuilder;

public interface PagesHash
{
    int getChannelCount();

    int getPositionCount();

    long getInMemorySizeInBytes();

    long getHashCollisions();

    double getExpectedHashCollisions();

    int getAddressIndex(int position, Page hashChannelsPage);

    int getAddressIndex(int rightPosition, Page hashChannelsPage, long rawHash);

    void appendTo(long position, PageBuilder pageBuilder, int outputChannelOffset);
}
