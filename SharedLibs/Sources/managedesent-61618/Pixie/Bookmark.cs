//-----------------------------------------------------------------------
// <copyright file="Bookmark.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent
{
    /// <summary>
    /// A bookmark represents a cursor location.
    /// </summary>
    internal struct Bookmark
    {
        /// <summary>
        /// Gets or sets the ESE bookmark of the record.
        /// </summary>
        internal byte[] BookmarkData { get; set; }

        /// <summary>
        /// Gets or sets the length of the ESE bookmark.
        /// </summary>
        internal int BookmarkLength { get; set; }
    }
}