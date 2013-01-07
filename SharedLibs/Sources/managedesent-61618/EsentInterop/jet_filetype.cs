//-----------------------------------------------------------------------
// <copyright file="jet_filetype.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    /// <summary>
    /// Esent file types.
    /// </summary>
    public enum JET_filetype
    {
        /// <summary>
        /// Unknown file.
        /// </summary>
        Unknown = 0,

        /// <summary>
        /// Database file.
        /// </summary>
        Database = 1,

        /// <summary>
        /// Transaction log.
        /// </summary>
        Log = 3,

        /// <summary>
        /// Checkpoint file.
        /// </summary>
        Checkpoint = 4,

        /// <summary>
        /// Temporary database.
        /// </summary>
        TempDatabase = 5,
    }
}