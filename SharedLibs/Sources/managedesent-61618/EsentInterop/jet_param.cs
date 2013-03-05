//-----------------------------------------------------------------------
// <copyright file="jet_param.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    /// <summary>
    /// ESENT system parameters.
    /// </summary>
    public enum JET_param
    {
        /// <summary>
        /// This parameter indicates the relative or absolute file system path of the
        /// folder that will contain the checkpoint file for the instance. The path
        /// must be terminated with a backslash character, which indicates that the
        /// target path is a folder. 
        /// </summary>
        SystemPath = 0,

        /// <summary>
        /// This parameter indicates the relative or absolute file system path of
        /// the folder or file that will contain the temporary database for the instance.
        /// If the path is to a folder that will contain the temporary database then it
        /// must be terminated with a backslash character.
        /// </summary>
        TempPath = 1,

        /// <summary>
        /// This parameter indicates the relative or absolute file system path of the
        /// folder that will contain the transaction logs for the instance. The path must
        /// be terminated with a backslash character, which indicates that the target path
        /// is a folder.
        /// </summary>
        LogFilePath = 2,

        /// <summary>
        /// This parameter sets the three letter prefix used for many of the files used by
        /// the database engine. For example, the checkpoint file is called EDB.CHK by
        /// default because EDB is the default base name.
        /// </summary>
        BaseName = 3,

        /// <summary>
        /// This parameter supplies an application specific string that will be added to
        /// any event log messages that are emitted by the database engine. This allows
        /// easy correlation of event log messages with the source application. By default
        /// the host application executable name will be used.
        /// </summary>
        EventSource = 4,

        /// <summary>
        /// This parameter reserves the requested number of session resources for use by an
        /// instance. A session resource directly corresponds to a JET_SESID data type.
        /// This setting will affect how many sessions can be used at the same time.
        /// </summary>
        MaxSessions = 5,

        /// <summary>
        /// This parameter reserves the requested number of B+ Tree resources for use by
        /// an instance. This setting will affect how many tables can be used at the same time.
        /// </summary>
        MaxOpenTables = 6,

        // PreferredMaxOpenTables(7) is obsolete

        /// <summary>
        /// This parameter reserves the requested number of cursor resources for use by an
        /// instance. A cursor resource directly corresponds to a JET_TABLEID data type.
        /// This setting will affect how many cursors can be used at the same time. A cursor
        /// resource cannot be shared by different sessions so this parameter must be set to
        /// a large enough value so that each session can use as many cursors as are required.
        /// </summary>
        MaxCursors = 8,

        /// <summary>
        /// This parameter reserves the requested number of version store pages for use by an instance.
        /// </summary>
        MaxVerPages = 9,

        /// <summary>
        /// This parameter reserves the requested number of temporary table resources for use
        /// by an instance. This setting will affect how many temporary tables can be used at
        /// the same time. If this system parameter is set to zero then no temporary database
        /// will be created and any activity that requires use of the temporary database will
        /// fail. This setting can be useful to avoid the I/O required to create the temporary
        /// database if it is known that it will not be used.
        /// </summary>
        /// <remarks>
        /// The use of a temporary table also requires a cursor resource.
        /// </remarks>
        MaxTemporaryTables = 10,

        /// <summary>
        /// This parameter will configure the size of the transaction log files. Each
        /// transaction log file is a fixed size. The size is equal to the setting of
        /// this system parameter in units of 1024 bytes.
        /// </summary>
        LogFileSize = 11,

        /// <summary>
        /// This parameter will configure the amount of memory used to cache log records
        /// before they are written to the transaction log file. The unit for this
        /// parameter is the sector size of the volume that holds the transaction log files.
        /// The sector size is almost always 512 bytes, so it is safe to assume that size
        /// for the unit. This parameter has an impact on performance. When the database
        /// engine is under heavy update load, this buffer can become full very rapidly.
        /// A larger cache size for the transaction log file is critical for good update
        /// performance under such a high load condition. The default is known to be too small
        /// for this case.
        /// Do not set this parameter to a number of buffers that is larger (in bytes) than
        /// half the size of a transaction log file.
        /// </summary>
        LogBuffers = 12,

        /// <summary>
        /// This parameter configures how transaction log files are managed by the database
        /// engine. When circular logging is off, all transaction log files that are generated
        /// are retained on disk until they are no longer needed because a full backup of the
        /// database has been performed. When circular logging is on, only transaction log files
        /// that are younger than the current checkpoint are retained on disk. The benefit of
        /// this mode is that backups are not required to retire old transaction log files. 
        /// </summary>
        CircularLog = 17,

        /// <summary>
        /// This parameter controls the amount of space that is added to a database file each
        /// time it needs to grow to accommodate more data. The size is in database pages.
        /// </summary>
        DbExtensionSize = 18,

        /// <summary>
        /// This parameter controls the initial size of the temporary database. The size is in
        /// database pages. A size of zero indicates that the default size of an ordinary
        /// database should be used. It is often desirable for small applications to configure
        /// the temporary database to be as small as possible. Setting this parameter to
        /// SystemParameters.PageTempDBSmallest will achieve the smallest temporary database possible.
        /// </summary>
        PageTempDBMin = 19,

        /// <summary>
        /// This parameter configures the maximum size of the database page cache. The size
        /// is in database pages. If this parameter is left to its default value, then the
        /// maximum size of the cache will be set to the size of physical memory when JetInit
        /// is called.
        /// </summary>
        CacheSizeMax = 23,

        /// <summary>
        /// This parameter controls how aggressively database pages are flushed from the
        /// database page cache to minimize the amount of time it will take to recover from a
        /// crash. The parameter is a threshold in bytes for about how many transaction log
        /// files will need to be replayed after a crash. If circular logging is enabled using
        /// JET_param.CircularLog then this parameter will also control the approximate amount
        /// of transaction log files that will be retained on disk.
        /// </summary>
        CheckpointDepthMax = 24,

        /// <summary>
        /// This parameter controls when the database page cache begins evicting pages from the
        /// cache to make room for pages that are not cached. When the number of page buffers in the cache
        /// drops below this threshold then a background process will be started to replenish that pool
        /// of available buffers. This threshold is always relative to the maximum cache size as set by
        /// JET_paramCacheSizeMax. This threshold must also always be less than the stop threshold as
        /// set by JET_paramStopFlushThreshold.
        /// <para>
        /// The distance height of the start threshold will determine the response time that the database
        ///  page cache must have to produce available buffers before the application needs them. A high
        /// start threshold will give the background process more time to react. However, a high start
        /// threshold implies a higher stop threshold and that will reduce the effective size of the
        /// database page cache for modified pages (Windows 2000) or for all pages (Windows XP and later).
        /// </para>
        /// </summary>
        StartFlushThreshold = 31,

        /// <summary>
        /// This parameter controls when the database page cache ends evicting pages from the cache to make
        /// room for pages that are not cached. When the number of page buffers in the cache rises above
        /// this threshold then the background process that was started to replenish that pool of available
        /// buffers is stopped. This threshold is always relative to the maximum cache size as set by
        /// JET_paramCacheSizeMax. This threshold must also always be greater than the start threshold
        /// as set by JET_paramStartFlushThreshold.
        /// <para>
        /// The distance between the start threshold and the stop threshold affects the efficiency with
        /// which database pages are flushed by the background process. A larger gap will make it
        /// more likely that writes to neighboring pages may be combined. However, a high stop
        /// threshold will reduce the effective size of the database page cache for modified
        /// pages (Windows 2000) or for all pages (Windows XP and later).
        /// </para>
        /// </summary>
        StopFlushThreshold = 32,

        /// <summary>
        /// This parameter is the master switch that controls crash recovery for an instance.
        /// If this parameter is set to "On" then ARIES style recovery will be used to bring all
        /// databases in the instance to a consistent state in the event of a process or machine
        /// crash. If this parameter is set to "Off" then all databases in the instance will be
        /// managed without the benefit of crash recovery. That is to say, that if the instance
        /// is not shut down cleanly using JetTerm prior to the process exiting or machine shutdown
        /// then the contents of all databases in that instance will be corrupted.
        /// </summary>
        Recovery = 34,

        /// <summary>
        /// This parameter can be used to control the size of the database page cache at run time.
        /// Ordinarily, the cache will automatically tune its size as a function of database and
        /// machine activity levels. If the application sets this parameter to zero, then the cache
        /// will tune its own size in this manner. However, if the application sets this parameter
        /// to a non-zero value then the cache will adjust itself to that target size.
        /// </summary>
        CacheSize = 41,

        /// <summary>
        /// When this parameter is true, every database is checked at JetAttachDatabase time for
        /// indexes over Unicode key columns that were built using an older version of the NLS
        /// library in the operating system. This must be done because the database engine persists
        /// the sort keys generated by LCMapStringW and the value of these sort keys change from release to release.
        /// If a primary index is detected to be in this state then JetAttachDatabase will always fail with
        /// JET_err.PrimaryIndexCorrupted.
        /// If any secondary indexes are detected to be in this state then there are two possible outcomes.
        /// If AttachDatabaseGrbit.DeleteCorruptIndexes was passed to JetAttachDatabase then these indexes
        /// will be deleted and JET_wrnCorruptIndexDeleted will be returned from JetAttachDatabase. These
        /// indexes will need to be recreated by your application. If AttachDatabaseGrbit.DeleteCorruptIndexes
        /// was not passed to JetAttachDatabase then the call will fail with JET_errSecondaryIndexCorrupted.
        /// </summary>
        EnableIndexChecking = 45,

        /// <summary>
        /// This parameter can be used to control which event log the database engine uses for its event log
        /// messages. By default, all event log messages will go to the Application event log. If the registry
        /// key name for another event log is configured then the event log messages will go there instead.
        /// </summary>        
        EventSourceKey = 49,

        /// <summary>
        /// When this parameter is true, informational event log messages that would ordinarily be generated by
        /// the database engine will be suppressed.
        /// </summary>
        NoInformationEvent = 50,

        /// <summary>
        /// Configures the detail level of eventlog messages that are emitted
        /// to the eventlog by the database engine. Higher numbers will result
        /// in more detailed eventlog messages.
        /// </summary>
        EventLoggingLevel = 51,

        /// <summary>
        /// This parameter configures the minimum size of the database page cache. The size is in database pages.
        /// </summary>
        CacheSizeMin = 60,

        /// <summary>
        /// This parameter represents a threshold relative to <see cref="JET_param.MaxVerPages"/> that controls
        /// the discretionary use of version pages by the database engine. If the size of the version store exceeds
        /// this threshold then any information that is only used for optional background tasks, such as reclaiming
        /// deleted space in the database, is instead sacrificed to preserve room for transactional information.
        /// </summary>
        PreferredVerPages = 63,

        /// <summary>
        /// This parameter configures the page size for the database. The page
        /// size is the smallest unit of space allocation possible for a database
        /// file. The database page size is also very important because it sets
        /// the upper limit on the size of an individual record in the database. 
        /// </summary>
        /// <remarks>
        /// Only one database page size is supported per process at this time.
        /// This means that if you are in a single process that contains different
        /// applications that use the database engine then they must all agree on
        /// a database page size.
        /// </remarks>
        DatabasePageSize = 64,

        /// <summary>
        /// This parameter can be used to convert a JET_ERR into a string.
        /// This should only be used with JetGetSystemParameter.
        /// </summary>
        ErrorToString = 70,

        /// <summary>
        /// Configures the engine with a <see cref="JET_CALLBACK"/> delegate.
        /// This callback may be called for the following reasons:
        /// <see cref="JET_cbtyp.FreeCursorLS"/>, <see cref="JET_cbtyp.FreeTableLS"/>
        /// or <see cref="JET_cbtyp.Null"/>. See <see cref="Api.JetSetLS"/>
        /// for more information. This parameter cannot currently be retrieved.
        /// </summary>
        RuntimeCallback = 73,

        /// <summary>
        /// This parameter controls the outcome of JetInit when the database
        /// engine is configured to start using transaction log files on disk
        /// that are of a different size than what is configured. Normally,
        /// <see cref="Api.JetInit"/> will successfully recover the databases
        /// but will fail with <see cref="JET_err.LogFileSizeMismatchDatabasesConsistent"/>
        /// to indicate that the log file size is misconfigured. However, when
        /// this parameter is set to true then the database engine will silently
        /// delete all the old log files, start a new set of transaction log files
        /// using the configured log file size. This parameter is useful when the
        /// application wishes to transparently change its transaction log file
        /// size yet still work transparently in upgrade and restore scenarios.
        /// </summary>
        CleanupMismatchedLogFiles = 77,

        /// <summary>
        /// When this parameter is set to true then any folder that is missing in a file system path in use by
        /// the database engine will be silently created. Otherwise, the operation that uses the missing file system
        /// path will fail with JET_err.InvalidPath.
        /// </summary>
        CreatePathIfNotExist = 100,

        /// <summary>
        /// When this parameter is true then only one database is allowed to
        /// be opened using JetOpenDatabase by a given session at one time.
        /// The temporary database is excluded from this restriction. 
        /// </summary>
        OneDatabasePerSession = 102,

        /// <summary>
        /// This parameter controls the maximum number of instances that can be created in a single process.
        /// </summary>
        MaxInstances = 104,

        /// <summary>
        /// This parameter controls the number of background cleanup work items that
        /// can be queued to the database engine thread pool at any one time.
        /// </summary>
        VersionStoreTaskQueueMax = 105,
    }
}