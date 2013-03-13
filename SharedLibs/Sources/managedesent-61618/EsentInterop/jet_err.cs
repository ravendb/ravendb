//-----------------------------------------------------------------------
// <copyright file="jet_err.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    using System.Diagnostics.CodeAnalysis;

    /// <summary>
    /// ESENT error codes.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1629:DocumentationTextMustEndWithAPeriod",
        Justification = "Auto-generated comments.")]
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1628:DocumentationTextMustBeginWithACapitalLetter",
        Justification = "Auto-generated comments.")]
    public enum JET_err
    {
        /// <summary>
        /// Successful operation.
        /// </summary>
        Success = 0,

        #region Errors        

        /// <summary>
        /// Resource Failure Simulator failure
        /// </summary>
        RfsFailure = -100,

        /// <summary>
        /// Resource Failure Simulator not initialized
        /// </summary>
        RfsNotArmed = -101,

        /// <summary>
        /// Could not close file
        /// </summary>
        FileClose = -102,

        /// <summary>
        /// Could not start thread
        /// </summary>
        OutOfThreads = -103,

        /// <summary>
        /// System busy due to too many IOs
        /// </summary>
        TooManyIO = -105,

        /// <summary>
        /// A requested async task could not be executed
        /// </summary>
        TaskDropped = -106,

        /// <summary>
        /// Fatal internal error
        /// </summary>
        InternalError = -107,

        /// <summary>
        /// You are running MinESE, that does not have all features compiled in.  This functionality is only supported in a full version of ESE.
        /// </summary>
        DisabledFunctionality = -112,

        /// <summary>
        /// Buffer dependencies improperly set. Recovery failure
        /// </summary>
        DatabaseBufferDependenciesCorrupted = -255,

        /// <summary>
        /// Version already existed. Recovery failure
        /// </summary>
        PreviousVersion = -322,

        /// <summary>
        /// Reached Page Boundary
        /// </summary>
        PageBoundary = -323,

        /// <summary>
        /// Reached Key Boundary
        /// </summary>
        KeyBoundary = -324,

        /// <summary>
        /// Database corrupted
        /// </summary>
        BadPageLink = -327,

        /// <summary>
        /// Bookmark has no corresponding address in database
        /// </summary>
        BadBookmark = -328,

        /// <summary>
        /// A call to the operating system failed
        /// </summary>
        NTSystemCallFailed = -334,

        /// <summary>
        /// Database corrupted
        /// </summary>
        BadParentPageLink = -338,

        /// <summary>
        /// AvailExt cache doesn't match btree
        /// </summary>
        SPAvailExtCacheOutOfSync = -340,

        /// <summary>
        /// AvailExt space tree is corrupt
        /// </summary>
        SPAvailExtCorrupted = -341,

        /// <summary>
        /// Out of memory allocating an AvailExt cache node
        /// </summary>
        SPAvailExtCacheOutOfMemory = -342,

        /// <summary>
        /// OwnExt space tree is corrupt
        /// </summary>
        SPOwnExtCorrupted = -343,

        /// <summary>
        /// Dbtime on current page is greater than global database dbtime
        /// </summary>
        DbTimeCorrupted = -344,

        /// <summary>
        /// key truncated on index that disallows key truncation
        /// </summary>
        KeyTruncated = -346,

        /// <summary>
        /// Some database pages have become unreachable even from the avail tree, only an offline defragmentation can return the lost space.
        /// </summary>
        DatabaseLeakInSpace = -348,

        /// <summary>
        /// Key is too large
        /// </summary>
        KeyTooBig = -408,

        /// <summary>
        /// illegal attempt to separate an LV which must be intrinsic
        /// </summary>
        CannotSeparateIntrinsicLV = -416,

        /// <summary>
        /// Operation not supported on separated long-value
        /// </summary>
        SeparatedLongValue = -421,

        /// <summary>
        /// Logged operation cannot be redone
        /// </summary>
        InvalidLoggedOperation = -500,

        /// <summary>
        /// Log file is corrupt
        /// </summary>
        LogFileCorrupt = -501,

        /// <summary>
        /// No backup directory given
        /// </summary>
        NoBackupDirectory = -503,

        /// <summary>
        /// The backup directory is not emtpy
        /// </summary>
        BackupDirectoryNotEmpty = -504,

        /// <summary>
        /// Backup is active already
        /// </summary>
        BackupInProgress = -505,

        /// <summary>
        /// Restore in progress
        /// </summary>
        RestoreInProgress = -506,

        /// <summary>
        /// Missing the log file for check point
        /// </summary>
        MissingPreviousLogFile = -509,

        /// <summary>
        /// Failure writing to log file
        /// </summary>
        LogWriteFail = -510,

        /// <summary>
        /// Try to log something after recovery faild
        /// </summary>
        LogDisabledDueToRecoveryFailure = -511,

        /// <summary>
        /// Try to log something during recovery redo
        /// </summary>
        CannotLogDuringRecoveryRedo = -512,

        /// <summary>
        /// Name of logfile does not match internal generation number
        /// </summary>
        LogGenerationMismatch = -513,

        /// <summary>
        /// Version of log file is not compatible with Jet version
        /// </summary>
        BadLogVersion = -514,

        /// <summary>
        /// Timestamp in next log does not match expected
        /// </summary>
        InvalidLogSequence = -515,

        /// <summary>
        /// Log is not active
        /// </summary>
        LoggingDisabled = -516,

        /// <summary>
        /// Log buffer is too small for recovery
        /// </summary>
        LogBufferTooSmall = -517,

        /// <summary>
        /// Maximum log file number exceeded
        /// </summary>
        LogSequenceEnd = -519,

        /// <summary>
        /// No backup in progress
        /// </summary>
        NoBackup = -520,

        /// <summary>
        /// Backup call out of sequence
        /// </summary>
        InvalidBackupSequence = -521,

        /// <summary>
        /// Cannot do backup now
        /// </summary>
        BackupNotAllowedYet = -523,

        /// <summary>
        /// Could not delete backup file
        /// </summary>
        DeleteBackupFileFail = -524,

        /// <summary>
        /// Could not make backup temp directory
        /// </summary>
        MakeBackupDirectoryFail = -525,

        /// <summary>
        /// Cannot perform incremental backup when circular logging enabled
        /// </summary>
        InvalidBackup = -526,

        /// <summary>
        /// Restored with errors
        /// </summary>
        RecoveredWithErrors = -527,

        /// <summary>
        /// Current log file missing
        /// </summary>
        MissingLogFile = -528,

        /// <summary>
        /// Log disk full
        /// </summary>
        LogDiskFull = -529,

        /// <summary>
        /// Bad signature for a log file
        /// </summary>
        BadLogSignature = -530,

        /// <summary>
        /// Bad signature for a db file
        /// </summary>
        BadDbSignature = -531,

        /// <summary>
        /// Bad signature for a checkpoint file
        /// </summary>
        BadCheckpointSignature = -532,

        /// <summary>
        /// Checkpoint file not found or corrupt
        /// </summary>
        CheckpointCorrupt = -533,

        /// <summary>
        /// Patch file page not found during recovery
        /// </summary>
        MissingPatchPage = -534,

        /// <summary>
        /// Patch file page is not valid
        /// </summary>
        BadPatchPage = -535,

        /// <summary>
        /// Redo abruptly ended due to sudden failure in reading logs from log file
        /// </summary>
        RedoAbruptEnded = -536,

        /// <summary>
        /// Signature in SLV file does not agree with database
        /// </summary>
        BadSLVSignature = -537,

        /// <summary>
        /// Hard restore detected that patch file is missing from backup set
        /// </summary>
        PatchFileMissing = -538,

        /// <summary>
        /// Database does not belong with the current set of log files
        /// </summary>
        DatabaseLogSetMismatch = -539,

        /// <summary>
        /// Database and streaming file do not match each other
        /// </summary>
        DatabaseStreamingFileMismatch = -540,

        /// <summary>
        /// actual log file size does not match JET_paramLogFileSize
        /// </summary>
        LogFileSizeMismatch = -541,

        /// <summary>
        /// Could not locate checkpoint file
        /// </summary>
        CheckpointFileNotFound = -542,

        /// <summary>
        /// The required log files for recovery is missing.
        /// </summary>
        RequiredLogFilesMissing = -543,

        /// <summary>
        /// Soft recovery is intended on a backup database. Restore should be used instead
        /// </summary>
        SoftRecoveryOnBackupDatabase = -544,

        /// <summary>
        /// databases have been recovered, but the log file size used during recovery does not match JET_paramLogFileSize
        /// </summary>
        LogFileSizeMismatchDatabasesConsistent = -545,

        /// <summary>
        /// the log file sector size does not match the current volume's sector size
        /// </summary>
        LogSectorSizeMismatch = -546,

        /// <summary>
        /// databases have been recovered, but the log file sector size (used during recovery) does not match the current volume's sector size
        /// </summary>
        LogSectorSizeMismatchDatabasesConsistent = -547,

        /// <summary>
        /// databases have been recovered, but all possible log generations in the current sequence are used; delete all log files and the checkpoint file and backup the databases before continuing
        /// </summary>
        LogSequenceEndDatabasesConsistent = -548,

        /// <summary>
        /// Illegal attempt to replay a streaming file operation where the data wasn't logged. Probably caused by an attempt to roll-forward with circular logging enabled
        /// </summary>
        StreamingDataNotLogged = -549,

        /// <summary>
        /// Database was not shutdown cleanly. Recovery must first be run to properly complete database operations for the previous shutdown.
        /// </summary>
        DatabaseDirtyShutdown = -550,

        /// <summary>
        /// Database last consistent time unmatched
        /// </summary>
        ConsistentTimeMismatch = -551,

        /// <summary>
        /// Patch file is not generated from this backup
        /// </summary>
        DatabasePatchFileMismatch = -552,

        /// <summary>
        /// The starting log number too low for the restore
        /// </summary>
        EndingRestoreLogTooLow = -553,

        /// <summary>
        /// The starting log number too high for the restore
        /// </summary>
        StartingRestoreLogTooHigh = -554,

        /// <summary>
        /// Restore log file has bad signature
        /// </summary>
        GivenLogFileHasBadSignature = -555,

        /// <summary>
        /// Restore log file is not contiguous
        /// </summary>
        GivenLogFileIsNotContiguous = -556,

        /// <summary>
        /// Some restore log files are missing
        /// </summary>
        MissingRestoreLogFiles = -557,

        /// <summary>
        /// The database missed a previous full backup before incremental backup
        /// </summary>
        MissingFullBackup = -560,

        /// <summary>
        /// The backup database size is not in 4k
        /// </summary>
        BadBackupDatabaseSize = -561,

        /// <summary>
        /// Attempted to upgrade a database that is already current
        /// </summary>
        DatabaseAlreadyUpgraded = -562,

        /// <summary>
        /// Attempted to use a database which was only partially converted to the current format -- must restore from backup
        /// </summary>
        DatabaseIncompleteUpgrade = -563,

        /// <summary>
        /// Some current log files are missing for continuous restore
        /// </summary>
        MissingCurrentLogFiles = -565,

        /// <summary>
        /// dbtime on page smaller than dbtimeBefore in record
        /// </summary>
        DbTimeTooOld = -566,

        /// <summary>
        /// dbtime on page in advance of the dbtimeBefore in record
        /// </summary>
        DbTimeTooNew = -567,

        /// <summary>
        /// Some log or patch files are missing during backup
        /// </summary>
        MissingFileToBackup = -569,

        /// <summary>
        /// torn-write was detected in a backup set during hard restore
        /// </summary>
        LogTornWriteDuringHardRestore = -570,

        /// <summary>
        /// torn-write was detected during hard recovery (log was not part of a backup set)
        /// </summary>
        LogTornWriteDuringHardRecovery = -571,

        /// <summary>
        /// corruption was detected in a backup set during hard restore
        /// </summary>
        LogCorruptDuringHardRestore = -573,

        /// <summary>
        /// corruption was detected during hard recovery (log was not part of a backup set)
        /// </summary>
        LogCorruptDuringHardRecovery = -574,

        /// <summary>
        /// Cannot have logging enabled while attempting to upgrade db
        /// </summary>
        MustDisableLoggingForDbUpgrade = -575,

        /// <summary>
        /// TargetInstance specified for restore is not found or log files don't match
        /// </summary>
        BadRestoreTargetInstance = -577,

        /// <summary>
        /// Soft recovery successfully replayed all operations, but the Undo phase of recovery was skipped
        /// </summary>
        RecoveredWithoutUndo = -579,

        /// <summary>
        /// Databases to be restored are not from the same shadow copy backup
        /// </summary>
        DatabasesNotFromSameSnapshot = -580,

        /// <summary>
        /// Soft recovery on a database from a shadow copy backup set
        /// </summary>
        SoftRecoveryOnSnapshot = -581,

        /// <summary>
        /// One or more logs that were committed to this database, are missing.  These log files are required to maintain durable ACID semantics, but not required to maintain consistency if the JET_bitReplayIgnoreLostLogs bit is specified during recovery.
        /// </summary>
        CommittedLogFilesMissing = -582,

        /// <summary>
        /// The physical sector size reported by the disk subsystem, is unsupported by ESE for a specific file type.
        /// </summary>
        SectorSizeNotSupported = -583,

        /// <summary>
        /// Soft recovery successfully replayed all operations and intended to skip the Undo phase of recovery, but the Undo phase was not required
        /// </summary>
        RecoveredWithoutUndoDatabasesConsistent = -584,

        /// <summary>
        /// One or more logs were found to be corrupt during recovery.  These log files are required to maintain durable ACID semantics, but not required to maintain consistency if the JET_bitIgnoreLostLogs bit and JET_paramDeleteOutOfRangeLogs is specified during recovery.
        /// </summary>
        CommittedLogFileCorrupt = -586,

        /// <summary>
        /// Unicode translation buffer too small
        /// </summary>
        UnicodeTranslationBufferTooSmall = -601,

        /// <summary>
        /// Unicode normalization failed
        /// </summary>
        UnicodeTranslationFail = -602,

        /// <summary>
        /// OS does not provide support for Unicode normalisation (and no normalisation callback was specified)
        /// </summary>
        UnicodeNormalizationNotSupported = -603,

        /// <summary>
        /// Can not validate the language
        /// </summary>
        UnicodeLanguageValidationFailure = -604,

        /// <summary>
        /// Existing log file has bad signature
        /// </summary>
        ExistingLogFileHasBadSignature = -610,

        /// <summary>
        /// Existing log file is not contiguous
        /// </summary>
        ExistingLogFileIsNotContiguous = -611,

        /// <summary>
        /// Checksum error in log file during backup
        /// </summary>
        LogReadVerifyFailure = -612,

        /// <summary>
        /// Checksum error in SLV file during backup
        /// </summary>
        SLVReadVerifyFailure = -613,

        /// <summary>
        /// too many outstanding generations between checkpoint and current generation
        /// </summary>
        CheckpointDepthTooDeep = -614,

        /// <summary>
        /// hard recovery attempted on a database that wasn't a backup database
        /// </summary>
        RestoreOfNonBackupDatabase = -615,

        /// <summary>
        /// log truncation attempted but not all required logs were copied
        /// </summary>
        LogFileNotCopied = -616,

        /// <summary>
        /// A surrogate backup is in progress.
        /// </summary>
        SurrogateBackupInProgress = -617,

        /// <summary>
        /// Backup was aborted by server by calling JetTerm with JET_bitTermStopBackup or by calling JetStopBackup
        /// </summary>
        BackupAbortByServer = -801,

        /// <summary>
        /// Invalid flags parameter
        /// </summary>
        InvalidGrbit = -900,

        /// <summary>
        /// Termination in progress
        /// </summary>
        TermInProgress = -1000,

        /// <summary>
        /// API not supported
        /// </summary>
        FeatureNotAvailable = -1001,

        /// <summary>
        /// Invalid name
        /// </summary>
        InvalidName = -1002,

        /// <summary>
        /// Invalid API parameter
        /// </summary>
        InvalidParameter = -1003,

        /// <summary>
        /// Tried to attach a read-only database file for read/write operations
        /// </summary>
        DatabaseFileReadOnly = -1008,

        /// <summary>
        /// Invalid database id
        /// </summary>
        InvalidDatabaseId = -1010,

        /// <summary>
        /// Out of Memory
        /// </summary>
        OutOfMemory = -1011,

        /// <summary>
        /// Maximum database size reached
        /// </summary>
        OutOfDatabaseSpace = -1012,

        /// <summary>
        /// Out of table cursors
        /// </summary>
        OutOfCursors = -1013,

        /// <summary>
        /// Out of database page buffers
        /// </summary>
        OutOfBuffers = -1014,

        /// <summary>
        /// Too many indexes
        /// </summary>
        TooManyIndexes = -1015,

        /// <summary>
        /// Too many columns in an index
        /// </summary>
        TooManyKeys = -1016,

        /// <summary>
        /// Record has been deleted
        /// </summary>
        RecordDeleted = -1017,

        /// <summary>
        /// Checksum error on a database page
        /// </summary>
        ReadVerifyFailure = -1018,

        /// <summary>
        /// Blank database page
        /// </summary>
        PageNotInitialized = -1019,

        /// <summary>
        /// Out of file handles
        /// </summary>
        OutOfFileHandles = -1020,

        /// <summary>
        /// The OS returned ERROR_CRC from file IO
        /// </summary>
        DiskReadVerificationFailure = -1021,

        /// <summary>
        /// Disk IO error
        /// </summary>
        DiskIO = -1022,

        /// <summary>
        /// Invalid file path
        /// </summary>
        InvalidPath = -1023,

        /// <summary>
        /// Invalid system path
        /// </summary>
        InvalidSystemPath = -1024,

        /// <summary>
        /// Invalid log directory
        /// </summary>
        InvalidLogDirectory = -1025,

        /// <summary>
        /// Record larger than maximum size
        /// </summary>
        RecordTooBig = -1026,

        /// <summary>
        /// Too many open databases
        /// </summary>
        TooManyOpenDatabases = -1027,

        /// <summary>
        /// Not a database file
        /// </summary>
        InvalidDatabase = -1028,

        /// <summary>
        /// Database engine not initialized
        /// </summary>
        NotInitialized = -1029,

        /// <summary>
        /// Database engine already initialized
        /// </summary>
        AlreadyInitialized = -1030,

        /// <summary>
        /// Database engine is being initialized
        /// </summary>
        InitInProgress = -1031,

        /// <summary>
        /// Cannot access file, the file is locked or in use
        /// </summary>
        FileAccessDenied = -1032,

        /// <summary>
        /// Query support unavailable
        /// </summary>
        QueryNotSupported = -1034,

        /// <summary>
        /// SQL Link support unavailable
        /// </summary>
        SQLLinkNotSupported = -1035,

        /// <summary>
        /// Buffer is too small
        /// </summary>
        BufferTooSmall = -1038,

        /// <summary>
        /// Too many columns defined
        /// </summary>
        TooManyColumns = -1040,

        /// <summary>
        /// Container is not empty
        /// </summary>
        ContainerNotEmpty = -1043,

        /// <summary>
        /// Filename is invalid
        /// </summary>
        InvalidFilename = -1044,

        /// <summary>
        /// Invalid bookmark
        /// </summary>
        InvalidBookmark = -1045,

        /// <summary>
        /// Column used in an index
        /// </summary>
        ColumnInUse = -1046,

        /// <summary>
        /// Data buffer doesn't match column size
        /// </summary>
        InvalidBufferSize = -1047,

        /// <summary>
        /// Cannot set column value
        /// </summary>
        ColumnNotUpdatable = -1048,

        /// <summary>
        /// Index is in use
        /// </summary>
        IndexInUse = -1051,

        /// <summary>
        /// Link support unavailable
        /// </summary>
        LinkNotSupported = -1052,

        /// <summary>
        /// Null keys are disallowed on index
        /// </summary>
        NullKeyDisallowed = -1053,

        /// <summary>
        /// Operation must be within a transaction
        /// </summary>
        NotInTransaction = -1054,

        /// <summary>
        /// Transaction must rollback because failure of unversioned update
        /// </summary>
        MustRollback = -1057,

        /// <summary>
        /// Too many active database users
        /// </summary>
        TooManyActiveUsers = -1059,

        /// <summary>
        /// Invalid or unknown country/region code
        /// </summary>
        InvalidCountry = -1061,

        /// <summary>
        /// Invalid or unknown language id
        /// </summary>
        InvalidLanguageId = -1062,

        /// <summary>
        /// Invalid or unknown code page
        /// </summary>
        InvalidCodePage = -1063,

        /// <summary>
        /// Invalid flags for LCMapString()
        /// </summary>
        InvalidLCMapStringFlags = -1064,

        /// <summary>
        /// Attempted to create a version store entry (RCE) larger than a version bucket
        /// </summary>
        VersionStoreEntryTooBig = -1065,

        /// <summary>
        /// Version store out of memory (and cleanup attempt failed to complete)
        /// </summary>
        VersionStoreOutOfMemoryAndCleanupTimedOut = -1066,

        /// <summary>
        /// Version store out of memory (cleanup already attempted)
        /// </summary>
        VersionStoreOutOfMemory = -1069,

        /// <summary>
        /// UNUSED: lCSRPerfFUCB * g_lCursorsMax exceeded (XJET only)
        /// </summary>
        CurrencyStackOutOfMemory = -1070,

        /// <summary>
        /// Cannot index escrow column or SLV column
        /// </summary>
        CannotIndex = -1071,

        /// <summary>
        /// Record has not been deleted
        /// </summary>
        RecordNotDeleted = -1072,

        /// <summary>
        /// Too many mempool entries requested
        /// </summary>
        TooManyMempoolEntries = -1073,

        /// <summary>
        /// Out of btree ObjectIDs (perform offline defrag to reclaim freed/unused ObjectIds)
        /// </summary>
        OutOfObjectIDs = -1074,

        /// <summary>
        /// Long-value ID counter has reached maximum value. (perform offline defrag to reclaim free/unused LongValueIDs)
        /// </summary>
        OutOfLongValueIDs = -1075,

        /// <summary>
        /// Auto-increment counter has reached maximum value (offline defrag WILL NOT be able to reclaim free/unused Auto-increment values).
        /// </summary>
        OutOfAutoincrementValues = -1076,

        /// <summary>
        /// Dbtime counter has reached maximum value (perform offline defrag to reclaim free/unused Dbtime values)
        /// </summary>
        OutOfDbtimeValues = -1077,

        /// <summary>
        /// Sequential index counter has reached maximum value (perform offline defrag to reclaim free/unused SequentialIndex values)
        /// </summary>
        OutOfSequentialIndexValues = -1078,

        /// <summary>
        /// Multi-instance call with single-instance mode enabled
        /// </summary>
        RunningInOneInstanceMode = -1080,

        /// <summary>
        /// Single-instance call with multi-instance mode enabled
        /// </summary>
        RunningInMultiInstanceMode = -1081,

        /// <summary>
        /// Global system parameters have already been set
        /// </summary>
        SystemParamsAlreadySet = -1082,

        /// <summary>
        /// System path already used by another database instance
        /// </summary>
        SystemPathInUse = -1083,

        /// <summary>
        /// Logfile path already used by another database instance
        /// </summary>
        LogFilePathInUse = -1084,

        /// <summary>
        /// Temp path already used by another database instance
        /// </summary>
        TempPathInUse = -1085,

        /// <summary>
        /// Instance Name already in use
        /// </summary>
        InstanceNameInUse = -1086,

        /// <summary>
        /// This instance cannot be used because it encountered a fatal error
        /// </summary>
        InstanceUnavailable = -1090,

        /// <summary>
        /// This database cannot be used because it encountered a fatal error
        /// </summary>
        DatabaseUnavailable = -1091,

        /// <summary>
        /// This instance cannot be used because it encountered a log-disk-full error performing an operation (likely transaction rollback) that could not tolerate failure
        /// </summary>
        InstanceUnavailableDueToFatalLogDiskFull = -1092,

        /// <summary>
        /// Out of sessions
        /// </summary>
        OutOfSessions = -1101,

        /// <summary>
        /// Write lock failed due to outstanding write lock
        /// </summary>
        WriteConflict = -1102,

        /// <summary>
        /// Transactions nested too deeply
        /// </summary>
        TransTooDeep = -1103,

        /// <summary>
        /// Invalid session handle
        /// </summary>
        InvalidSesid = -1104,

        /// <summary>
        /// Update attempted on uncommitted primary index
        /// </summary>
        WriteConflictPrimaryIndex = -1105,

        /// <summary>
        /// Operation not allowed within a transaction
        /// </summary>
        InTransaction = -1108,

        /// <summary>
        /// Must rollback current transaction -- cannot commit or begin a new one
        /// </summary>
        RollbackRequired = -1109,

        /// <summary>
        /// Read-only transaction tried to modify the database
        /// </summary>
        TransReadOnly = -1110,

        /// <summary>
        /// Attempt to replace the same record by two diffrerent cursors in the same session
        /// </summary>
        SessionWriteConflict = -1111,

        /// <summary>
        /// record would be too big if represented in a database format from a previous version of Jet
        /// </summary>
        RecordTooBigForBackwardCompatibility = -1112,

        /// <summary>
        /// The temp table could not be created due to parameters that conflict with JET_bitTTForwardOnly
        /// </summary>
        CannotMaterializeForwardOnlySort = -1113,

        /// <summary>
        /// This session handle can't be used with this table id
        /// </summary>
        SesidTableIdMismatch = -1114,

        /// <summary>
        /// Invalid instance handle
        /// </summary>
        InvalidInstance = -1115,

        /// <summary>
        /// The instance was shutdown successfully but all the attached databases were left in a dirty state by request via JET_bitTermDirty
        /// </summary>
        DirtyShutdown = -1116,

        /// <summary>
        /// The database page read from disk had the wrong page number.
        /// </summary>
        ReadPgnoVerifyFailure = -1118,

        /// <summary>
        /// The database page read from disk had a previous write not represented on the page.
        /// </summary>
        ReadLostFlushVerifyFailure = -1119,

        /// <summary>
        /// Attempted to PrepareToCommit a distributed transaction to non-zero level
        /// </summary>
        MustCommitDistributedTransactionToLevel0 = -1150,

        /// <summary>
        /// Attempted a write-operation after a distributed transaction has called PrepareToCommit
        /// </summary>
        DistributedTransactionAlreadyPreparedToCommit = -1151,

        /// <summary>
        /// Attempted to PrepareToCommit a non-distributed transaction
        /// </summary>
        NotInDistributedTransaction = -1152,

        /// <summary>
        /// Attempted to commit a distributed transaction, but PrepareToCommit has not yet been called
        /// </summary>
        DistributedTransactionNotYetPreparedToCommit = -1153,

        /// <summary>
        /// Attempted to begin a distributed transaction when not at level 0
        /// </summary>
        CannotNestDistributedTransactions = -1154,

        /// <summary>
        /// Attempted to begin a distributed transaction but no callback for DTC coordination was specified on initialisation
        /// </summary>
        DTCMissingCallback = -1160,

        /// <summary>
        /// Attempted to recover a distributed transaction but no callback for DTC coordination was specified on initialisation
        /// </summary>
        DTCMissingCallbackOnRecovery = -1161,

        /// <summary>
        /// Unexpected error code returned from DTC callback
        /// </summary>
        DTCCallbackUnexpectedError = -1162,

        /// <summary>
        /// Database already exists
        /// </summary>
        DatabaseDuplicate = -1201,

        /// <summary>
        /// Database in use
        /// </summary>
        DatabaseInUse = -1202,

        /// <summary>
        /// No such database
        /// </summary>
        DatabaseNotFound = -1203,

        /// <summary>
        /// Invalid database name
        /// </summary>
        DatabaseInvalidName = -1204,

        /// <summary>
        /// Invalid number of pages
        /// </summary>
        DatabaseInvalidPages = -1205,

        /// <summary>
        /// Non database file or corrupted db
        /// </summary>
        DatabaseCorrupted = -1206,

        /// <summary>
        /// Database exclusively locked
        /// </summary>
        DatabaseLocked = -1207,

        /// <summary>
        /// Cannot disable versioning for this database
        /// </summary>
        CannotDisableVersioning = -1208,

        /// <summary>
        /// Database engine is incompatible with database
        /// </summary>
        InvalidDatabaseVersion = -1209,

        /// <summary>
        /// The database is in an older (200) format
        /// </summary>
        Database200Format = -1210,

        /// <summary>
        /// The database is in an older (400) format
        /// </summary>
        Database400Format = -1211,

        /// <summary>
        /// The database is in an older (500) format
        /// </summary>
        Database500Format = -1212,

        /// <summary>
        /// The database page size does not match the engine
        /// </summary>
        PageSizeMismatch = -1213,

        /// <summary>
        /// Cannot start any more database instances
        /// </summary>
        TooManyInstances = -1214,

        /// <summary>
        /// A different database instance is using this database
        /// </summary>
        DatabaseSharingViolation = -1215,

        /// <summary>
        /// An outstanding database attachment has been detected at the start or end of recovery, but database is missing or does not match attachment info
        /// </summary>
        AttachedDatabaseMismatch = -1216,

        /// <summary>
        /// Specified path to database file is illegal
        /// </summary>
        DatabaseInvalidPath = -1217,

        /// <summary>
        /// A database is being assigned an id already in use
        /// </summary>
        DatabaseIdInUse = -1218,

        /// <summary>
        /// Force Detach allowed only after normal detach errored out
        /// </summary>
        ForceDetachNotAllowed = -1219,

        /// <summary>
        /// Corruption detected in catalog
        /// </summary>
        CatalogCorrupted = -1220,

        /// <summary>
        /// Database is partially attached. Cannot complete attach operation
        /// </summary>
        PartiallyAttachedDB = -1221,

        /// <summary>
        /// Database with same signature in use
        /// </summary>
        DatabaseSignInUse = -1222,

        /// <summary>
        /// Corrupted db but repair not allowed
        /// </summary>
        DatabaseCorruptedNoRepair = -1224,

        /// <summary>
        /// recovery tried to replay a database creation, but the database was originally created with an incompatible (likely older) version of the database engine
        /// </summary>
        InvalidCreateDbVersion = -1225,

        /// <summary>
        /// The database cannot be attached because it is currently being rebuilt as part of an incremental reseed.
        /// </summary>
        DatabaseIncompleteIncrementalReseed = -1226,

        /// <summary>
        /// The database is not a valid state to perform an incremental reseed.
        /// </summary>
        DatabaseInvalidIncrementalReseed = -1227,

        /// <summary>
        /// The incremental reseed being performed on the specified database cannot be completed due to a fatal error.  A full reseed is required to recover this database.
        /// </summary>
        DatabaseFailedIncrementalReseed = -1228,

        /// <summary>
        /// The incremental reseed being performed on the specified database cannot be completed because the min required log contains no attachment info.  A full reseed is required to recover this database.
        /// </summary>
        NoAttachmentsFailedIncrementalReseed = -1229,

        /// <summary>
        /// Table is exclusively locked
        /// </summary>
        TableLocked = -1302,

        /// <summary>
        /// Table already exists
        /// </summary>
        TableDuplicate = -1303,

        /// <summary>
        /// Table is in use, cannot lock
        /// </summary>
        TableInUse = -1304,

        /// <summary>
        /// No such table or object
        /// </summary>
        ObjectNotFound = -1305,

        /// <summary>
        /// Bad file/index density
        /// </summary>
        DensityInvalid = -1307,

        /// <summary>
        /// Table is not empty
        /// </summary>
        TableNotEmpty = -1308,

        /// <summary>
        /// Invalid table id
        /// </summary>
        InvalidTableId = -1310,

        /// <summary>
        /// Cannot open any more tables (cleanup already attempted)
        /// </summary>
        TooManyOpenTables = -1311,

        /// <summary>
        /// Oper. not supported on table
        /// </summary>
        IllegalOperation = -1312,

        /// <summary>
        /// Cannot open any more tables (cleanup attempt failed to complete)
        /// </summary>
        TooManyOpenTablesAndCleanupTimedOut = -1313,

        /// <summary>
        /// Table or object name in use
        /// </summary>
        ObjectDuplicate = -1314,

        /// <summary>
        /// Object is invalid for operation
        /// </summary>
        InvalidObject = -1316,

        /// <summary>
        /// Use CloseTable instead of DeleteTable to delete temp table
        /// </summary>
        CannotDeleteTempTable = -1317,

        /// <summary>
        /// Illegal attempt to delete a system table
        /// </summary>
        CannotDeleteSystemTable = -1318,

        /// <summary>
        /// Illegal attempt to delete a template table
        /// </summary>
        CannotDeleteTemplateTable = -1319,

        /// <summary>
        /// Must have exclusive lock on table.
        /// </summary>
        ExclusiveTableLockRequired = -1322,

        /// <summary>
        /// DDL operations prohibited on this table
        /// </summary>
        FixedDDL = -1323,

        /// <summary>
        /// On a derived table, DDL operations are prohibited on inherited portion of DDL
        /// </summary>
        FixedInheritedDDL = -1324,

        /// <summary>
        /// Nesting of hierarchical DDL is not currently supported.
        /// </summary>
        CannotNestDDL = -1325,

        /// <summary>
        /// Tried to inherit DDL from a table not marked as a template table.
        /// </summary>
        DDLNotInheritable = -1326,

        /// <summary>
        /// System parameters were set improperly
        /// </summary>
        InvalidSettings = -1328,

        /// <summary>
        /// Client has requested stop service
        /// </summary>
        ClientRequestToStopJetService = -1329,

        /// <summary>
        /// Template table was created with NoFixedVarColumnsInDerivedTables
        /// </summary>
        CannotAddFixedVarColumnToDerivedTable = -1330,

        /// <summary>
        /// Index build failed
        /// </summary>
        IndexCantBuild = -1401,

        /// <summary>
        /// Primary index already defined
        /// </summary>
        IndexHasPrimary = -1402,

        /// <summary>
        /// Index is already defined
        /// </summary>
        IndexDuplicate = -1403,

        /// <summary>
        /// No such index
        /// </summary>
        IndexNotFound = -1404,

        /// <summary>
        /// Cannot delete clustered index
        /// </summary>
        IndexMustStay = -1405,

        /// <summary>
        /// Illegal index definition
        /// </summary>
        IndexInvalidDef = -1406,

        /// <summary>
        /// Invalid create index description
        /// </summary>
        InvalidCreateIndex = -1409,

        /// <summary>
        /// Out of index description blocks
        /// </summary>
        TooManyOpenIndexes = -1410,

        /// <summary>
        /// Non-unique inter-record index keys generated for a multivalued index
        /// </summary>
        MultiValuedIndexViolation = -1411,

        /// <summary>
        /// Failed to build a secondary index that properly reflects primary index
        /// </summary>
        IndexBuildCorrupted = -1412,

        /// <summary>
        /// Primary index is corrupt. The database must be defragmented
        /// </summary>
        PrimaryIndexCorrupted = -1413,

        /// <summary>
        /// Secondary index is corrupt. The database must be defragmented
        /// </summary>
        SecondaryIndexCorrupted = -1414,

        /// <summary>
        /// Illegal index id
        /// </summary>
        InvalidIndexId = -1416,

        /// <summary>
        /// tuple index can only be on a secondary index
        /// </summary>
        IndexTuplesSecondaryIndexOnly = -1430,

        /// <summary>
        /// tuple index may only have eleven columns in the index
        /// </summary>
        IndexTuplesTooManyColumns = -1431,

        /// <summary>
        /// tuple index must be a non-unique index
        /// </summary>
        IndexTuplesNonUniqueOnly = -1432,

        /// <summary>
        /// tuple index must be on a text/binary column
        /// </summary>
        IndexTuplesTextBinaryColumnsOnly = -1433,

        /// <summary>
        /// tuple index does not allow setting cbVarSegMac
        /// </summary>
        IndexTuplesVarSegMacNotAllowed = -1434,

        /// <summary>
        /// invalid min/max tuple length or max characters to index specified
        /// </summary>
        IndexTuplesInvalidLimits = -1435,

        /// <summary>
        /// cannot call RetrieveColumn() with RetrieveFromIndex on a tuple index
        /// </summary>
        IndexTuplesCannotRetrieveFromIndex = -1436,

        /// <summary>
        /// specified key does not meet minimum tuple length
        /// </summary>
        IndexTuplesKeyTooSmall = -1437,

        /// <summary>
        /// Column value is long
        /// </summary>
        ColumnLong = -1501,

        /// <summary>
        /// No such chunk in long value
        /// </summary>
        ColumnNoChunk = -1502,

        /// <summary>
        /// Field will not fit in record
        /// </summary>
        ColumnDoesNotFit = -1503,

        /// <summary>
        /// Null not valid
        /// </summary>
        NullInvalid = -1504,

        /// <summary>
        /// Column indexed, cannot delete
        /// </summary>
        ColumnIndexed = -1505,

        /// <summary>
        /// Field length is greater than maximum
        /// </summary>
        ColumnTooBig = -1506,

        /// <summary>
        /// No such column
        /// </summary>
        ColumnNotFound = -1507,

        /// <summary>
        /// Field is already defined
        /// </summary>
        ColumnDuplicate = -1508,

        /// <summary>
        /// Attempted to create a multi-valued column, but column was not Tagged
        /// </summary>
        MultiValuedColumnMustBeTagged = -1509,

        /// <summary>
        /// Second autoincrement or version column
        /// </summary>
        ColumnRedundant = -1510,

        /// <summary>
        /// Invalid column data type
        /// </summary>
        InvalidColumnType = -1511,

        /// <summary>
        /// No non-NULL tagged columns
        /// </summary>
        TaggedNotNULL = -1514,

        /// <summary>
        /// Invalid w/o a current index
        /// </summary>
        NoCurrentIndex = -1515,

        /// <summary>
        /// The key is completely made
        /// </summary>
        KeyIsMade = -1516,

        /// <summary>
        /// Column Id Incorrect
        /// </summary>
        BadColumnId = -1517,

        /// <summary>
        /// Bad itagSequence for tagged column
        /// </summary>
        BadItagSequence = -1518,

        /// <summary>
        /// Cannot delete, column participates in relationship
        /// </summary>
        ColumnInRelationship = -1519,

        /// <summary>
        /// AutoIncrement and Version cannot be tagged
        /// </summary>
        CannotBeTagged = -1521,

        /// <summary>
        /// Default value exceeds maximum size
        /// </summary>
        DefaultValueTooBig = -1524,

        /// <summary>
        /// Duplicate detected on a unique multi-valued column
        /// </summary>
        MultiValuedDuplicate = -1525,

        /// <summary>
        /// Corruption encountered in long-value tree
        /// </summary>
        LVCorrupted = -1526,

        /// <summary>
        /// Duplicate detected on a unique multi-valued column after data was normalized, and normalizing truncated the data before comparison
        /// </summary>
        MultiValuedDuplicateAfterTruncation = -1528,

        /// <summary>
        /// Invalid column in derived table
        /// </summary>
        DerivedColumnCorruption = -1529,

        /// <summary>
        /// Tried to convert column to a primary index placeholder, but column doesn't meet necessary criteria
        /// </summary>
        InvalidPlaceholderColumn = -1530,

        /// <summary>
        /// Only JET_coltypLongText and JET_coltypLongBinary columns can be compressed
        /// </summary>
        ColumnCannotBeCompressed = -1538,

        /// <summary>
        /// The key was not found
        /// </summary>
        RecordNotFound = -1601,

        /// <summary>
        /// No working buffer
        /// </summary>
        RecordNoCopy = -1602,

        /// <summary>
        /// Currency not on a record
        /// </summary>
        NoCurrentRecord = -1603,

        /// <summary>
        /// Primary key may not change
        /// </summary>
        RecordPrimaryChanged = -1604,

        /// <summary>
        /// Illegal duplicate key
        /// </summary>
        KeyDuplicate = -1605,

        /// <summary>
        /// Attempted to update record when record update was already in progress
        /// </summary>
        AlreadyPrepared = -1607,

        /// <summary>
        /// No call to JetMakeKey
        /// </summary>
        KeyNotMade = -1608,

        /// <summary>
        /// No call to JetPrepareUpdate
        /// </summary>
        UpdateNotPrepared = -1609,

        /// <summary>
        /// Data has changed, operation aborted
        /// </summary>
        DataHasChanged = -1611,

        /// <summary>
        /// Windows installation does not support language
        /// </summary>
        LanguageNotSupported = -1619,

        /// <summary>
        /// Internal error: data could not be decompressed
        /// </summary>
        DecompressionFailed = -1620,

        /// <summary>
        /// No version updates only for uncommitted tables
        /// </summary>
        UpdateMustVersion = -1621,

        /// <summary>
        /// Too many sort processes
        /// </summary>
        TooManySorts = -1701,

        /// <summary>
        /// Invalid operation on Sort
        /// </summary>
        InvalidOnSort = -1702,

        /// <summary>
        /// Temp file could not be opened
        /// </summary>
        TempFileOpenError = -1803,

        /// <summary>
        /// Too many open databases
        /// </summary>
        TooManyAttachedDatabases = -1805,

        /// <summary>
        /// No space left on disk
        /// </summary>
        DiskFull = -1808,

        /// <summary>
        /// Permission denied
        /// </summary>
        PermissionDenied = -1809,

        /// <summary>
        /// File not found
        /// </summary>
        FileNotFound = -1811,

        /// <summary>
        /// Invalid file type
        /// </summary>
        FileInvalidType = -1812,

        /// <summary>
        /// Cannot Restore after init.
        /// </summary>
        AfterInitialization = -1850,

        /// <summary>
        /// Logs could not be interpreted
        /// </summary>
        LogCorrupted = -1852,

        /// <summary>
        /// Invalid operation
        /// </summary>
        InvalidOperation = -1906,

        /// <summary>
        /// Access denied
        /// </summary>
        AccessDenied = -1907,

        /// <summary>
        /// Infinite split
        /// </summary>
        TooManySplits = -1909,

        /// <summary>
        /// Multiple threads are using the same session
        /// </summary>
        SessionSharingViolation = -1910,

        /// <summary>
        /// An entry point in a DLL we require could not be found
        /// </summary>
        EntryPointNotFound = -1911,

        /// <summary>
        /// Specified session already has a session context set
        /// </summary>
        SessionContextAlreadySet = -1912,

        /// <summary>
        /// Tried to reset session context, but current thread did not orignally set the session context
        /// </summary>
        SessionContextNotSetByThisThread = -1913,

        /// <summary>
        /// Tried to terminate session in use
        /// </summary>
        SessionInUse = -1914,

        /// <summary>
        /// Internal error during dynamic record format conversion
        /// </summary>
        RecordFormatConversionFailed = -1915,

        /// <summary>
        /// Just one open user database per session is allowed (JET_paramOneDatabasePerSession)
        /// </summary>
        OneDatabasePerSession = -1916,

        /// <summary>
        /// error during rollback
        /// </summary>
        RollbackError = -1917,

        /// <summary>
        /// The operation did not complete successfully because the database is already running maintenance on specified database
        /// </summary>
        DatabaseAlreadyRunningMaintenance = -2004,

        /// <summary>
        /// A callback failed
        /// </summary>
        CallbackFailed = -2101,

        /// <summary>
        /// A callback function could not be found
        /// </summary>
        CallbackNotResolved = -2102,

        /// <summary>
        /// An element of the JET space hints structure was not correct or actionable.
        /// </summary>
        SpaceHintsInvalid = -2103,

        /// <summary>
        /// Corruption encountered in space manager of streaming file
        /// </summary>
        SLVSpaceCorrupted = -2201,

        /// <summary>
        /// Corruption encountered in streaming file
        /// </summary>
        SLVCorrupted = -2202,

        /// <summary>
        /// SLV columns cannot have a default value
        /// </summary>
        SLVColumnDefaultValueNotAllowed = -2203,

        /// <summary>
        /// Cannot find streaming file associated with this database
        /// </summary>
        SLVStreamingFileMissing = -2204,

        /// <summary>
        /// Streaming file exists, but database to which it belongs is missing
        /// </summary>
        SLVDatabaseMissing = -2205,

        /// <summary>
        /// Tried to create a streaming file when one already exists or is already recorded in the catalog
        /// </summary>
        SLVStreamingFileAlreadyExists = -2206,

        /// <summary>
        /// Specified path to a streaming file is invalid
        /// </summary>
        SLVInvalidPath = -2207,

        /// <summary>
        /// Tried to perform an SLV operation but streaming file was never created
        /// </summary>
        SLVStreamingFileNotCreated = -2208,

        /// <summary>
        /// Attach a readonly streaming file for read/write operations
        /// </summary>
        SLVStreamingFileReadOnly = -2209,

        /// <summary>
        /// SLV file header failed checksum verification
        /// </summary>
        SLVHeaderBadChecksum = -2210,

        /// <summary>
        /// SLV file header contains invalid information
        /// </summary>
        SLVHeaderCorrupted = -2211,

        /// <summary>
        /// Tried to move pages from the Free state when they were not in that state
        /// </summary>
        SLVPagesNotFree = -2213,

        /// <summary>
        /// Tried to move pages from the Reserved state when they were not in that state
        /// </summary>
        SLVPagesNotReserved = -2214,

        /// <summary>
        /// Tried to move pages from the Committed state when they were not in that state
        /// </summary>
        SLVPagesNotCommitted = -2215,

        /// <summary>
        /// Tried to move pages from the Deleted state when they were not in that state
        /// </summary>
        SLVPagesNotDeleted = -2216,

        /// <summary>
        /// Unexpected conflict detected trying to write-latch SLV space pages
        /// </summary>
        SLVSpaceWriteConflict = -2217,

        /// <summary>
        /// The database can not be created/attached because its corresponding SLV Root is still open by another process.
        /// </summary>
        SLVRootStillOpen = -2218,

        /// <summary>
        /// The database can not be created/attached because the SLV Provider has not been loaded.
        /// </summary>
        SLVProviderNotLoaded = -2219,

        /// <summary>
        /// The specified SLV EA List is corrupted.
        /// </summary>
        SLVEAListCorrupt = -2220,

        /// <summary>
        /// The database cannot be created/attached because the SLV Root Name was omitted
        /// </summary>
        SLVRootNotSpecified = -2221,

        /// <summary>
        /// The specified SLV Root path was invalid.
        /// </summary>
        SLVRootPathInvalid = -2222,

        /// <summary>
        /// The specified SLV EA List has no allocated space.
        /// </summary>
        SLVEAListZeroAllocation = -2223,

        /// <summary>
        /// Deletion of SLV columns is not currently supported.
        /// </summary>
        SLVColumnCannotDelete = -2224,

        /// <summary>
        /// Tried to create a new catalog entry for SLV Ownership Map when one already exists
        /// </summary>
        SLVOwnerMapAlreadyExists = -2225,

        /// <summary>
        /// Corruption encountered in SLV Ownership Map
        /// </summary>
        SLVOwnerMapCorrupted = -2226,

        /// <summary>
        /// Corruption encountered in SLV Ownership Map
        /// </summary>
        SLVOwnerMapPageNotFound = -2227,

        /// <summary>
        /// The specified SLV File handle belongs to a SLV Root that no longer exists.
        /// </summary>
        SLVFileStale = -2229,

        /// <summary>
        /// The specified SLV File is currently in use
        /// </summary>
        SLVFileInUse = -2230,

        /// <summary>
        /// The specified streaming file is currently in use
        /// </summary>
        SLVStreamingFileInUse = -2231,

        /// <summary>
        /// An I/O error occurred while accessing an SLV File (general read / write failure)
        /// </summary>
        SLVFileIO = -2232,

        /// <summary>
        /// No space left in the streaming file
        /// </summary>
        SLVStreamingFileFull = -2233,

        /// <summary>
        /// Specified path to a SLV File was invalid
        /// </summary>
        SLVFileInvalidPath = -2234,

        /// <summary>
        /// Cannot access SLV File, the SLV File is locked or is in use
        /// </summary>
        SLVFileAccessDenied = -2235,

        /// <summary>
        /// The specified SLV File was not found
        /// </summary>
        SLVFileNotFound = -2236,

        /// <summary>
        /// An unknown error occurred while accessing an SLV File
        /// </summary>
        SLVFileUnknown = -2237,

        /// <summary>
        /// The specified SLV EA List could not be returned because it is too large to fit in the standard EA format.  Retrieve the SLV File as a file handle instead.
        /// </summary>
        SLVEAListTooBig = -2238,

        /// <summary>
        /// The loaded SLV Provider's version does not match the database engine's version.
        /// </summary>
        SLVProviderVersionMismatch = -2239,

        /// <summary>
        /// Buffer allocated for SLV data or meta-data was too small
        /// </summary>
        SLVBufferTooSmall = -2243,

        /// <summary>
        /// OS Shadow copy API used in an invalid sequence
        /// </summary>
        OSSnapshotInvalidSequence = -2401,

        /// <summary>
        /// OS Shadow copy ended with time-out
        /// </summary>
        OSSnapshotTimeOut = -2402,

        /// <summary>
        /// OS Shadow copy not allowed (backup or recovery in progress)
        /// </summary>
        OSSnapshotNotAllowed = -2403,

        /// <summary>
        /// invalid JET_OSSNAPID
        /// </summary>
        OSSnapshotInvalidSnapId = -2404,

        /// <summary>
        /// Internal test injection limit hit
        /// </summary>
        TooManyTestInjections = -2501,

        /// <summary>
        /// Test injection not supported
        /// </summary>
        TestInjectionNotSupported = -2502,

        /// <summary>
        /// Some how the log data provided got out of sequence with the current state of the instance
        /// </summary>
        InvalidLogDataSequence = -2601,

        /// <summary>
        /// Attempted to use Local Storage without a callback function being specified
        /// </summary>
        LSCallbackNotSpecified = -3000,

        /// <summary>
        /// Attempted to set Local Storage for an object which already had it set
        /// </summary>
        LSAlreadySet = -3001,

        /// <summary>
        /// Attempted to retrieve Local Storage from an object which didn't have it set
        /// </summary>
        LSNotSet = -3002,

        /// <summary>
        /// an I/O was issued to a location that was sparse
        /// </summary>
        FileIOSparse = -4000,

        /// <summary>
        /// a read was issued to a location beyond EOF (writes will expand the file)
        /// </summary>
        FileIOBeyondEOF = -4001,

        /// <summary>
        /// instructs the JET_ABORTRETRYFAILCALLBACK caller to abort the specified I/O
        /// </summary>
        FileIOAbort = -4002,

        /// <summary>
        /// instructs the JET_ABORTRETRYFAILCALLBACK caller to retry the specified I/O
        /// </summary>
        FileIORetry = -4003,

        /// <summary>
        /// instructs the JET_ABORTRETRYFAILCALLBACK caller to fail the specified I/O
        /// </summary>
        FileIOFail = -4004,

        /// <summary>
        /// read/write access is not supported on compressed files
        /// </summary>
        FileCompressed = -4005,

        #endregion
    }
}
