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
    Busy: "The disk is congested right now, or journal writes are slower than the congestion threshold.",
    Saturated: "The disk was congested for long enough to slow down writes and indexing. Clears after an hour back to normal.",
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
    Explicit: "Requested by the server (index, backup, admin action).",
    Idle: "No writes for a few seconds, the pending data was synced.",
    JournalCount: "Too many journals were waiting for the sync before they could be reused.",
    SoftThreshold: "Unsynced data passed Storage.MaxUnsyncedSizeBeforeSyncInMb.",
    Mandatory: "Unsynced data passed Storage.MaxUnsyncedSizeBeforeMandatorySyncInMb: the sync runs whatever the disk state.",
};

export const deviceClassHelp: Record<DeviceClass, string> = {
    Unknown: "Not classified: no journal writes yet, or writes too small to tell a slow disk from a fast one (small writes are quick on any disk). Auto journal compression uses LZ4.",
    Fast: "Fast disk (NVMe-like): limits so high they are not hit. Auto journal compression uses LZ4, only transactions of at least 512 KB are compressed, and new journals are pre-zeroed in the background.",
    Budgeted: "Bandwidth / IOPS limited disk (e.g. gp3): Auto journal compression uses Zstd, because bytes are what the disk limits.",
};

export const storageTermHelp = {
    drain: "Paced (drain in the server log): the disk is congested. Flushes stop pushing data to the disk in the background, and each data file sync writes its data in blocks first, then fsyncs. Syncs take longer so that journal writes, which users wait for, get through.",
    trickle: "Normal (trickle in the server log): the disk keeps up. On Linux each flush pushes its data to the disk in the background, so a sync mostly just fsyncs.",
    drainShare: "Share of the last hour the disk was in paced mode.",
    journalWriteLatency:
        "Moving average of the journal write time of every storage environment whose data file is on this disk: the server, each database, its shared index journal and indexes writing their own journal. Users wait for these writes. Journals moved to another disk (e.g. a symlink) still count for the disk of their data file.",
    syncCost: "Average time of a data file sync on this disk, including the paced writeback. Above its threshold the disk switches to paced mode.",
    queueDepth: "Average number of requests queued on the device (iostat aqu-sz). Above its threshold the disk switches to paced mode.",
    diskStats:
        "Requests the OS sends to this disk per second and their average size (the same counters as the IO Stats dashboard widget). Cloud disks limit both requests per second (IOPS) and bytes per second (throughput), reads and writes together. Small requests hit the IOPS limit first, large ones the throughput limit: compare with the limits of the volume.",
    pipelining: "Pipelining: a journal write is submitted while the previous one is still in flight. Used for small, latency bound writes.",
    consolidation: "Batch consolidation: the transaction merger holds a batch open a bit longer so one larger journal write replaces several small ones.",
    flusherYields: "How often the flusher postponed flushing this environment to leave the disk to journal writes.",
    sharedJournal: "Index environments of a database write through one shared journal, hard-linked into each index.",
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
    Auto: "Auto: I/O ring when it can be set up at startup, otherwise vectored file I/O on Linux and file I/O on Windows.",
    IoRing: "I/O ring (io_uring on Linux, IoRing on Windows): data file writes are queued and submitted in batches.",
    VectoredFileIo: "Vectored file I/O (pwritev): one system call writes several page ranges.",
    FileIo: "File I/O: one write call per page range.",
};

export const writeModeTermHelp =
    "How this server writes its data files when flushing. Decided once at startup for the whole server (Storage.WriteMode). Journal writes are not affected.";

export const journalCompressionLabel: Record<Voron.JournalCompressionAlgorithm, string> = {
    Auto: "Auto",
    Lz4: "LZ4",
    Zstd: "Zstd",
};

export const ioStatsHelp = {
    journalCompression:
        "Algorithm of the latest compressed journal write in the data shown. With Auto, Zstd is used on bandwidth / IOPS limited (Budgeted) disks and LZ4 on the others.",
    drainSync:
        "Paced sync: the disk was congested, so the sync wrote its data in blocks first, then fsync. The bar covers both. Slower on purpose, so journal writes get through.",
};

export const meterTypeLabel: Record<Sparrow.Server.Meters.IoMetrics.MeterType, string> = {
    JournalWrite: "Journal write",
    DataFlush: "Data flush",
    DataSync: "Data sync",
    Compression: "Compression",
};
