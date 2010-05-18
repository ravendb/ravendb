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