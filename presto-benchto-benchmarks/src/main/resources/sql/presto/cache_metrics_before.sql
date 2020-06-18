SELECT format('REMOTE_BEFORE (%f), NON_LOCAL_BEFORE (%f)', sum(fs.readfromremote), sum(fs.nonlocaldataread)) FROM jmx.current."rubix:catalog=hive,name=stats" fs;
