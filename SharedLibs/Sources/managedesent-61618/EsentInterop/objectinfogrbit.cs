//-----------------------------------------------------------------------
// <copyright file="objectinfogrbit.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    using System;

    /// <summary>
    /// Table options, used in <see cref="JET_OBJECTINFO"/>.
    /// </summary>
    [Flags]
    public enum ObjectInfoGrbit
    {
        /// <summary>
        /// The table can have bookmarks.
        /// </summary>
        Bookmark = 0x1,

        /// <summary>
        /// The table can be rolled back.
        /// </summary>
        Rollback = 0x2,

        /// <summary>
        /// The table can be updated.
        /// </summary>
        Updatable = 0x4,
    }
}