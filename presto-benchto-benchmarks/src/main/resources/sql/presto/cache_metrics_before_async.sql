SELECT format('ASYNC_BEFORE (%s)', sum(async_downloaded_mb.count)) FROM jmx.current."metrics:name=rubix.bookkeeper.count.async_downloaded_mb" async_downloaded_mb;
