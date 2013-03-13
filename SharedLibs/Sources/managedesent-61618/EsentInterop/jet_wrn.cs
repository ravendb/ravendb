//-----------------------------------------------------------------------
// <copyright file="jet_wrn.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    using System.Diagnostics.CodeAnalysis;

    /// <summary>
    /// ESENT warning codes.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1629:DocumentationTextMustEndWithAPeriod",
        Justification = "Auto-generated comments.")]
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1628:DocumentationTextMustBeginWithACapitalLetter",
        Justification = "Auto-generated comments.")]
    public enum JET_wrn
    {
        /// <summary>
        /// Successful operation.
        /// </summary>
        Success = 0,

        #region Warnings        

        /// <summary>
        /// The version store is still active
        /// </summary>
        RemainingVersions = 321,

        /// <summary>
        /// seek on non-unique index yielded a unique key
        /// </summary>
        UniqueKey = 345,

        /// <summary>
        /// Column is a separated long-value
        /// </summary>
        SeparateLongValue = 406,

        /// <summary>
        /// Existing log file has bad signature
        /// </summary>
        ExistingLogFileHasBadSignature = 558,

        /// <summary>
        /// Existing log file is not contiguous
        /// </summary>
        ExistingLogFileIsNotContiguous = 559,

        /// <summary>
        /// INTERNAL ERROR
        /// </summary>
        SkipThisRecord = 564,

        /// <summary>
        /// TargetInstance specified for restore is running
        /// </summary>
        TargetInstanceRunning = 578,

        /// <summary>
        /// One or more logs that were committed to this database, were not recovered.  The database is still clean/consistent, as though the lost log's transactions were committed lazily (and lost).
        /// </summary>
        CommittedLogFilesLost = 585,

        /// <summary>
        /// One or more logs that were committed to this database, were no recovered.  The database is still clean/consistent, as though the corrupted log's transactions were committed lazily (and lost).
        /// </summary>
        CommittedLogFilesRemoved = 587,

        /// <summary>
        /// Signal used by clients to indicate JetInit() finished with undo
        /// </summary>
        FinishWithUndo = 588,

        /// <summary>
        /// Database corruption has been repaired
        /// </summary>
        DatabaseRepaired = 595,

        /// <summary>
        /// Column is NULL-valued
        /// </summary>
        ColumnNull = 1004,

        /// <summary>
        /// Buffer too small for data
        /// </summary>
        BufferTruncated = 1006,

        /// <summary>
        /// Database is already attached
        /// </summary>
        DatabaseAttached = 1007,

        /// <summary>
        /// Sort does not fit in memory
        /// </summary>
        SortOverflow = 1009,

        /// <summary>
        /// Exact match not found during seek
        /// </summary>
        SeekNotEqual = 1039,

        /// <summary>
        /// No extended error information
        /// </summary>
        NoErrorInfo = 1055,

        /// <summary>
        /// No idle activity occured
        /// </summary>
        NoIdleActivity = 1058,

        /// <summary>
        /// No write lock at transaction level 0
        /// </summary>
        NoWriteLock = 1067,

        /// <summary>
        /// Column set to NULL-value
        /// </summary>
        ColumnSetNull = 1068,

        /// <summary>
        /// Warning code DTC callback should return if the specified transaction is to be committed
        /// </summary>
        DTCCommitTransaction = 1163,

        /// <summary>
        /// Warning code DTC callback should return if the specified transaction is to be rolled back
        /// </summary>
        DTCRollbackTransaction = 1164,

        /// <summary>
        /// Opened an empty table
        /// </summary>
        TableEmpty = 1301,

        /// <summary>
        /// System cleanup has a cursor open on the table
        /// </summary>
        TableInUseBySystem = 1327,

        /// <summary>
        /// Out of date index removed
        /// </summary>
        CorruptIndexDeleted = 1415,

        /// <summary>
        /// Max length too big, truncated
        /// </summary>
        ColumnMaxTruncated = 1512,

        /// <summary>
        /// Single instance column bursted
        /// </summary>
        CopyLongValue = 1520,

        /// <summary>
        /// RetrieveTaggedColumnList ran out of copy buffer before retrieving all tagged columns
        /// </summary>
        TaggedColumnsRemaining = 1523,

        /// <summary>
        /// Column value(s) not returned because the corresponding column id or itagSequence requested for enumeration was null
        /// </summary>
        ColumnSkipped = 1531,

        /// <summary>
        /// Column value(s) not returned because they could not be reconstructed from the data at hand
        /// </summary>
        ColumnNotLocal = 1532,

        /// <summary>
        /// Column values exist that were not requested for enumeration
        /// </summary>
        ColumnMoreTags = 1533,

        /// <summary>
        /// Column value truncated at the requested size limit during enumeration
        /// </summary>
        ColumnTruncated = 1534,

        /// <summary>
        /// Column values exist but were not returned by request
        /// </summary>
        ColumnPresent = 1535,

        /// <summary>
        /// Column value returned in JET_COLUMNENUM as a result of JET_bitEnumerateCompressOutput
        /// </summary>
        ColumnSingleValue = 1536,

        /// <summary>
        /// Column value(s) not returned because they were set to their default value(s) and JET_bitEnumerateIgnoreDefault was specified
        /// </summary>
        ColumnDefault = 1537,

        /// <summary>
        /// Column value(s) not returned because they could not be reconstructed from the data in the record
        /// </summary>
        ColumnNotInRecord = 1539,

        /// <summary>
        /// Data has changed
        /// </summary>
        DataHasChanged = 1610,

        /// <summary>
        /// Moved to new key
        /// </summary>
        KeyChanged = 1618,

        /// <summary>
        /// Database file is read only
        /// </summary>
        FileOpenReadOnly = 1813,

        /// <summary>
        /// Idle registry full
        /// </summary>
        IdleFull = 1908,

        /// <summary>
        /// Online defrag already running on specified database
        /// </summary>
        DefragAlreadyRunning = 2000,

        /// <summary>
        /// Online defrag not running on specified database
        /// </summary>
        DefragNotRunning = 2001,

        /// <summary>
        /// JetDatabaseScan already running on specified database
        /// </summary>
        DatabaseScanAlreadyRunning = 2002,

        /// <summary>
        /// JetDatabaseScan not running on specified database
        /// </summary>
        DatabaseScanNotRunning = 2003,

        /// <summary>
        /// Unregistered a non-existant callback function
        /// </summary>
        CallbackNotRegistered = 2100,

        /// <summary>
        /// The log data provided jumped to the next log suddenly, we have deleted the incomplete log file as a precautionary measure
        /// </summary>
        PreviousLogFileIncomplete = 2602,

        #endregion
    }
}
