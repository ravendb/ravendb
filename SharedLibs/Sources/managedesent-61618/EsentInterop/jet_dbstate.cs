//-----------------------------------------------------------------------
// <copyright file="jet_dbstate.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    /// <summary>
    /// Database states (used in <see cref="JET_DBINFOMISC"/>).
    /// </summary>
    public enum JET_dbstate
    {
        /// <summary>
        /// The database was just created.
        /// </summary>
        JustCreated = 1,

        /// <summary>
        /// Dirty shutdown (inconsistent) database.
        /// </summary>
        DirtyShutdown = 2,

        /// <summary>
        /// Clean shutdown (consistent) database.
        /// </summary>
        CleanShutdown = 3,

        /// <summary>
        /// Database is being converted.
        /// </summary>
        BeingConverted = 4,

        /// <summary>
        /// Database was force-detached.
        /// </summary>
        ForceDetach = 5,
    }
}