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
    Unknown: "Not classified yet: no journal writes, or writes too small to tell a slow disk from a fast one. Safe defaults apply.",
    Fast: "Fast disk (NVMe-like): limits so high they are not hit. Auto journal compression uses LZ4.",
    Budgeted: "Bandwidth / IOPS limited disk (e.g. gp3): Auto journal compression uses Zstd to save disk budget.",
};

export const storageTermHelp = {
    drain: "Drain (paced writeback): the disk is congested, so flushes stop pushing data in the background and each sync writes its data in paced blocks first. Leaves the disk to the journal writes users wait for.",
    trickle: "Trickle: the disk has headroom, every flush starts pushing its data to the disk in the background.",
    drainShare: "Share of the last hour the disk spent in drain mode (paced writeback).",
    journalWriteLatency: "Average time of a journal write on this disk. Users wait for these writes.",
    syncCost: "Average time of a data file sync on this disk, including the paced writeback. Above the drain threshold, writes get paced.",
    queueDepth: "Average number of requests queued on the device (iostat aqu-sz). Above the drain threshold, writes get paced.",
    pipelining: "Pipelining: a journal write is submitted while the previous one is still in flight. Used for small, latency bound writes.",
    consolidation: "Batch consolidation: the transaction merger holds a batch open a bit longer so one larger journal write replaces several small ones.",
    flusherYields: "How often the flusher postponed flushing this environment to leave the disk to journal writes.",
    sharedJournal: "Index environments of a database write through one shared journal, hard-linked into each index.",
};

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
    drainPeriods:
        "Shaded: the disk was in drain mode (paced writeback), sampled every 10 seconds over the last hour. Syncs in these periods write their data in paced blocks before the final fsync, so they take longer.",
    drainSync:
        "Sync that ran with paced writeback (drain): it wrote its data in paced blocks first, then flushed the file to disk (fsync). Its duration covers both.",
};

export const meterTypeLabel: Record<Sparrow.Server.Meters.IoMetrics.MeterType, string> = {
    JournalWrite: "Journal write",
    DataFlush: "Data flush",
    DataSync: "Data sync",
    Compression: "Compression",
};
