//-----------------------------------------------------------------------
// <copyright file="Caches.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    using System.Diagnostics;

    /// <summary>
    /// Static class containing MemoryCaches for different ESENT buffers.
    /// Use these to avoid memory allocations when the memory will be
    /// used for a brief time.
    /// </summary>
    internal static class Caches
    {
        /// <summary>
        /// The maximum key size that any version of ESENT can have for
        /// any page size. This is also the maximum bookmark size.
        /// </summary>
        private const int KeyMostMost = 2000;

        /// <summary>
        /// The maximum number of buffers we want in a cache.
        /// </summary>
        private const int MaxBuffers = 16;

        /// <summary>
        /// Cached buffers for columns.
        /// </summary>
        private static readonly MemoryCache columnCache = new MemoryCache(128 * 1024, MaxBuffers);

        /// <summary>
        /// Cached buffers for keys and bookmarks.
        /// </summary>
        private static readonly MemoryCache bookmarkCache = new MemoryCache(KeyMostMost, MaxBuffers);

        /// <summary>
        /// Gets the cached buffers for columns.
        /// </summary>
        public static MemoryCache ColumnCache
        {
            [DebuggerStepThrough]
            get { return columnCache; }
        }

        /// <summary>
        /// Gets the cached buffers for keys and bookmarks.
        /// </summary>
        public static MemoryCache BookmarkCache
        {
            [DebuggerStepThrough]
            get { return bookmarkCache; }
        }
    }
}