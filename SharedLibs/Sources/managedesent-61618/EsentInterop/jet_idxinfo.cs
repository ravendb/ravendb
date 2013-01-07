//-----------------------------------------------------------------------
// <copyright file="jet_idxinfo.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    using System;

    /// <summary>
    /// Info levels for retrieve index information with JetGetIndexInfo
    /// and JetGetTableIndexInfo.
    /// </summary>
    public enum JET_IdxInfo
    {
        /// <summary>
        /// Returns a <see cref="JET_INDEXLIST"/> structure with information about the index.
        /// </summary>
        Default = 0,

        /// <summary>
        /// Returns a <see cref="JET_INDEXLIST"/> structure with information about the index.
        /// </summary>
        List = 1,

        /// <summary>
        /// SysTabCursor is obsolete.
        /// </summary>
        [Obsolete("This value is not used, and is provided for completeness to match the published header in the SDK.")]
        SysTabCursor = 2,

        /// <summary>
        /// OLC is obsolete.
        /// </summary>
        [Obsolete("This value is not used, and is provided for completeness to match the published header in the SDK.")]
        OLC = 3,

        /// <summary>
        /// Reset OLC is obsolete.
        /// </summary>
        [Obsolete("This value is not used, and is provided for completeness to match the published header in the SDK.")]
        ResetOLC = 4,

        /// <summary>
        /// Returns an integer with the space usage of the index.
        /// </summary>
        SpaceAlloc = 5,

        /// <summary>
        /// Returns an integer with the LCID of the index.
        /// </summary>
        LCID = 6,

        /// <summary>
        /// Langid is obsolete. Use <see cref="LCID"/> instead.
        /// </summary>
        [Obsolete("Use JET_IdxInfo.LCID")]
        Langid = 6,

        /// <summary>
        /// Returns an integer with the count of indexes in the table.
        /// </summary>
        Count = 7,

        /// <summary>
        /// Returns a ushort with the value of cbVarSegMac the index was created with.
        /// </summary>
        VarSegMac = 8,

        /// <summary>
        /// Returns a <see cref="JET_INDEXID"/> identifying the index.
        /// </summary>
        IndexId = 9,

        /// <summary>
        /// Introduced in Windows Vista. Returns a ushort with the value of cbKeyMost the 
        /// index was created with.
        /// </summary>
        KeyMost = 10,

        /// <summary>
        /// Introduced in Windows 7. Returns a JET_INDEXCREATE structure suitable
        /// for use by JetCreateIndex2().
        /// </summary>
        InfoCreateIndex = 11,

        /// <summary>
        /// Introduced in Windows 7. Returns a JET_INDEXCREATE2 structure suitable
        /// for use by JetCreateIndex2().
        /// </summary>
        InfoCreateIndex2 = 11,
    }
}
