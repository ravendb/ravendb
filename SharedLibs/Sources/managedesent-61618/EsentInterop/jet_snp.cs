//-----------------------------------------------------------------------
// <copyright file="jet_snp.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    /// <summary>
    /// The type of operation that progress is being reported for.
    /// </summary>
    public enum JET_SNP
    {
        /// <summary>
        /// Callback is for a repair option.
        /// </summary>
        Repair = 2,

        /// <summary>
        /// Callback is for database defragmentation.
        /// </summary>
        Compact = 4,
        
        /// <summary>
        /// Callback is for a restore options.
        /// </summary>
        Restore = 8,

        /// <summary>
        /// Callback is for a backup options.
        /// </summary>
        Backup = 9,

        /// <summary>
        /// Callback is for database zeroing.
        /// </summary>
        Scrub = 11,

        /// <summary>
        /// Callback is for the process of upgrading the record format of
        /// all database pages.
        /// </summary>
        UpgradeRecordFormat = 12,
    }
}
