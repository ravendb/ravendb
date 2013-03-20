//-----------------------------------------------------------------------
// <copyright file="VistaGrbits.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop.Vista
{
    /// <summary>
    /// Options for <see cref="VistaApi.JetOSSnapshotEnd"/>.
    /// </summary>
    public enum SnapshotEndGrbit
    {
        /// <summary>
        /// Default options.
        /// </summary>
        None = 0,
    
        /// <summary>
        /// The snapshot session aborted.
        /// </summary>
        AbortSnapshot = 0x1,
    }

    /// <summary>
    /// Options for <see cref="VistaApi.JetOSSnapshotPrepareInstance"/>.
    /// </summary>
    public enum SnapshotPrepareInstanceGrbit
    {
        /// <summary>
        /// Default options.
        /// </summary>
        None = 0,
    }

    /// <summary>
    /// Options for <see cref="VistaApi.JetOSSnapshotTruncateLog"/>
    /// and <see cref="VistaApi.JetOSSnapshotTruncateLogInstance"/>.
    /// </summary>
    public enum SnapshotTruncateLogGrbit
    {
        /// <summary>
        /// No truncation will occur.
        /// </summary>
        None = 0,

        /// <summary>
        /// All the databases are attached so the storage engine can compute
        /// and do the log truncation.
        /// </summary>
        AllDatabasesSnapshot = 0x1,
    }

    /// <summary>
    /// Options for <see cref="VistaApi.JetOSSnapshotGetFreezeInfo"/>.
    /// </summary>
    public enum SnapshotGetFreezeInfoGrbit
    {
        /// <summary>
        /// Default options.
        /// </summary>
        None = 0,
    }

    /// <summary>
    /// Information levels for <see cref="VistaApi.JetGetInstanceMiscInfo"/>.
    /// </summary>
    public enum JET_InstanceMiscInfo
    {
        /// <summary>
        /// Get the signature of the transaction log associated with this sequence.
        /// </summary>
        LogSignature = 0,
    }

    /// <summary>
    /// Grbits that have been added to the Vista version of ESENT.
    /// </summary>
    public static class VistaGrbits
    {
        /// <summary>
        /// Specifying this flag for an index that has more than one key column
        /// that is a multi-valued column will result in an index entry being
        /// created for each result of a cross product of all the values in
        /// those key columns. Otherwise, the index would only have one entry
        /// for each multi-value in the most significant key column that is a
        /// multi-valued column and each of those index entries would use the
        /// first multi-value from any other key columns that are multi-valued columns.
        /// <para>
        /// For example, if you specified this flag for an index over column
        /// A that has the values "red" and "blue" and over column B that has
        /// the values "1" and "2" then the following index entries would be
        /// created: "red", "1"; "red", "2"; "blue", "1"; "blue", "2". Otherwise,
        /// the following index entries would be created: "red", "1"; "blue", "1".
        /// </para>
        /// </summary>
        public const CreateIndexGrbit IndexCrossProduct = (CreateIndexGrbit)0x4000;

        /// <summary>
        /// Specifying this flag will cause any update to the index that would
        /// result in a truncated key to fail with <see cref="JET_err.KeyTruncated"/>.
        /// Otherwise, keys will be silently truncated.
        /// </summary>
        public const CreateIndexGrbit IndexDisallowTruncation = (CreateIndexGrbit)0x10000;

        /// <summary>
        /// Index over multiple multi-valued columns but only with values of same itagSequence.
        /// </summary>
        public const CreateIndexGrbit IndexNestedTable = (CreateIndexGrbit)0x20000;

        /// <summary>
        /// The engine can mark the database headers as appropriate (for example,
        /// a full backup completed), even though the call to truncate was not completed.
        /// </summary>
        public const EndExternalBackupGrbit TruncateDone = (EndExternalBackupGrbit)0x100;

        /// <summary>
        /// Perform recovery, but halt at the Undo phase. Allows whatever logs are present to
        /// be replayed, then later additional logs can be copied and replayed.
        /// </summary>
        public const InitGrbit RecoveryWithoutUndo = (InitGrbit)0x4;

        /// <summary>
        /// On successful soft recovery, truncate log files.
        /// </summary>
        public const InitGrbit TruncateLogsAfterRecovery = (InitGrbit)0x00000010;

        /// <summary>
        /// Missing database map entry default to same location.
        /// </summary>
        public const InitGrbit ReplayMissingMapEntryDB = (InitGrbit)0x00000020;

        /// <summary>
        /// Transaction logs must exist in the log file directory
        /// (i.e. can't auto-start a new stream).
        /// </summary>
        public const InitGrbit LogStreamMustExist = (InitGrbit)0x40;

        /// <summary>
        /// The snapshot session continues after JetOSSnapshotThaw and will
        /// require a JetOSSnapshotEnd function call.
        /// </summary>
        public const SnapshotPrepareGrbit ContinueAfterThaw = (SnapshotPrepareGrbit)0x4;

        /// <summary>
        /// Specifying this flag will cause the index to use the maximum key size
        /// specified in the cbKeyMost field in the structure. Otherwise, the
        /// index will use JET_cbKeyMost (255) as its maximum key size.
        /// </summary>
        /// <remarks>
        /// Set internally when the NATIVE_INDEXCREATE structure is generated.
        /// </remarks>
        internal const CreateIndexGrbit IndexKeyMost = (CreateIndexGrbit)0x8000;

        /// <summary>
        /// LCID field of JET_INDEXCREATE actually points to a JET_UNICODEINDEX
        /// struct to allow user-defined LCMapString() flags.
        /// </summary>
        internal const CreateIndexGrbit IndexUnicode = (CreateIndexGrbit)0x00000800;
    }
}