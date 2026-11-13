// One vocabulary for the storage I/O state, shared by the dashboard, IO Stats, Storage I/O page and analyzer.
// Customer facing text uses the plain words; support pages show the technical name next to them.

type StorageDeviceStatus = Raven.Server.Storage.StorageDeviceStatus;
type SyncReason = Sparrow.Server.Meters.IoMetrics.SyncReason;
type DeviceClass = Voron.Impl.Journal.DeviceWriteBudget.DeviceClass;

export const storageStatusLabel: Record<StorageDeviceStatus, string> = {
    Ok: "OK",
    Busy: "Busy",
    Saturated: "Saturated",
};

// bootstrap color variants
export const storageStatusColor: Record<StorageDeviceStatus, "success" | "warning" | "danger"> = {
    Ok: "success",
    Busy: "warning",
    Saturated: "danger",
};

export const storageStatusHelp: Record<StorageDeviceStatus, string> = {
    Ok: "The disk keeps up with the writes.",
    Busy: "The disk was overloaded in the last 10 seconds: syncs were paced, or journal writes took longer than the limit.",
    Saturated: "The disk has been overloaded long enough to slow down writes and indexing. It clears after an hour without overload.",
};

export const syncReasonLabel: Record<SyncReason, string> = {
    None: "-",
    Explicit: "Requested",
    Idle: "Idle",
    JournalCount: "Journal count",
    SoftThreshold: "Soft limit",
    Mandatory: "Mandatory",
};

export const syncReasonHelp: Record<SyncReason, string> = {
    None: "",
    Explicit: "Requested by the server, for example for an index, a backup or an admin action.",
    Idle: "No writes for a few seconds, so the server synced the pending changes.",
    JournalCount: "Many journal files were waiting for this sync before they could be reused.",
    SoftThreshold: "The data waiting to be synced passed the soft limit (Storage.MaxUnsyncedSizeBeforeSyncInMb).",
    Mandatory: "The data waiting to be synced passed the hard limit (Storage.MaxUnsyncedSizeBeforeMandatorySyncInMb), so the sync ran even though the disk was busy.",
};

export const deviceClassHelp: Record<DeviceClass, string> = {
    Unknown: "Not known yet: there were no journal writes, or they were too small to tell a slow disk from a fast one. Journal compression set to Auto uses LZ4.",
    Fast: "A fast disk, for example local NVMe. Journal compression set to Auto uses LZ4 and compresses only transactions of 512 KB or more. New journal files are prepared in the background.",
    Budgeted: "A disk with a limit on requests or bytes per second, for example AWS gp3. Journal compression set to Auto uses Zstd, so fewer bytes are written.",
};

export const storageTermHelp = {
    drain: "Paced: the disk is overloaded, so data file syncs write their data in blocks and let journal writes go first. Users wait for journal writes, so they get the disk first and syncs take longer. The server log calls this drain.",
    trickle: "Normal: the disk keeps up. On Linux, changes are written to disk in the background as they come, so a sync has little left to do. The server log calls this trickle.",
    drainShare: "How much of the last hour the disk spent in paced mode.",
    journalWriteLatency:
        "Average time of a journal write on this disk. Users wait for these writes when they save data. It covers the server and every database and index whose data file is on this disk.",
    syncCost:
        "Average time of a data file sync on this disk. A sync makes the changes in a data file safe on disk. Above the limit, the disk switches to paced mode.",
    queueDepth:
        "How many requests wait at the disk on average. Above the limit, the disk switches to paced mode and read-ahead slows down.",
    readAhead:
        "While more than 10% of memory is free, the server reads data files into memory ahead of time, so later reads are faster. It uses only spare disk capacity: its budget halves every second the disk queue is above the limit. It stops when free memory drops below 5%.",
    diskStats:
        "Requests sent to this disk per second and their average size, as the operating system counts them. Cloud disks limit both requests per second (IOPS) and bytes per second (throughput). Many small requests hit the IOPS limit first, large ones the throughput limit. Compare with the limits of your volume.",
    pipelining: "Pipelined: the next journal write starts before the previous one finishes. Used when writes are small and the disk is slow to answer.",
    consolidation: "Consolidating: the server waits a moment to combine several small journal writes into one larger write.",
    flusherYields: "How many times the server postponed writing to this data file, to leave the disk free for journal writes.",
    sharedJournal: "The indexes of a database write to one shared journal instead of one journal each.",
};

// the sync cost average expires after a minute without a data file sync, so 0 means no recent sync, not a fast one
type WriteMode = Sparrow.Server.Platform.Pal.RvnWriteMode;

export const writeModeLabel: Record<WriteMode, string> = {
    Auto: "Auto",
    IoRing: "I/O ring",
    VectoredFileIo: "Vectored file I/O",
    FileIo: "File I/O",
};

export const writeModeHelp: Record<WriteMode, string> = {
    Auto: "Auto: I/O ring if it can be set up at startup, otherwise vectored file I/O on Linux and file I/O on Windows.",
    IoRing: "I/O ring (io_uring on Linux, IoRing on Windows): data file writes are queued and sent to the disk in batches.",
    VectoredFileIo: "Vectored file I/O: one call writes several page ranges at once.",
    FileIo: "File I/O: one call per page range.",
};

export const writeModeTermHelp =
    "How this server writes its data files. Chosen once at startup for the whole server (Storage.WriteMode). Journal writes are not affected.";

export const journalCompressionLabel: Record<Voron.JournalCompressionAlgorithm, string> = {
    Auto: "Auto",
    Lz4: "LZ4",
    Zstd: "Zstd",
};

export const ioStatsHelp = {
    journalCompression:
        "How the latest compressed journal write in this view was compressed. With Auto, Zstd is used on disks with a limit on requests or bytes per second (Budgeted) and LZ4 on the others.",
};

export const meterTypeLabel: Record<Sparrow.Server.Meters.IoMetrics.MeterType, string> = {
    JournalWrite: "Journal write",
    DataFlush: "Data flush",
    DataSync: "Data sync",
    Compression: "Compression",
};
