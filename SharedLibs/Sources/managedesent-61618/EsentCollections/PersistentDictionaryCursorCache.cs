// --------------------------------------------------------------------------------------------------------------------
// <copyright file="PersistentDictionaryCursorCache.cs" company="Microsoft Corporation">
//   Copyright (c) Microsoft Corporation.
// </copyright>
// <summary>
//   We don't want to open and close dictionary cursors too frequently. This code provides
//   a cache of unused cursors. 
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Collections.Generic
{
    using System;
    using System.Diagnostics;
    using Microsoft.Isam.Esent.Interop;

    /// <summary>
    /// A cache of <see cref="PersistentDictionaryCursor{TKey,TValue}"/>
    /// objects.
    /// </summary>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <typeparam name="TValue">The type of the value.</typeparam>
    internal sealed class PersistentDictionaryCursorCache<TKey, TValue> : IDisposable
        where TKey : IComparable<TKey>
    {
        /// <summary>
        /// The maximum number of cursors that can be cached.
        /// </summary>
        private const int MaxCachedCursors = 64;

        /// <summary>
        /// The underlying ESENT instance.
        /// </summary>
        private readonly Instance instance;

        /// <summary>
        /// Data converters for the cursors.
        /// </summary>
        private readonly PersistentDictionaryConverters<TKey, TValue> converters;

        /// <summary>
        /// Configuration for the cursors.
        /// </summary>
        private readonly IPersistentDictionaryConfig config;

        /// <summary>
        /// The name of the database to attach.
        /// </summary>
        private readonly string database;

        /// <summary>
        /// The cached cursors.
        /// </summary>
        private readonly PersistentDictionaryCursor<TKey, TValue>[] cursors;

        /// <summary>
        /// Lock objects used to serialize access to the cursors.
        /// </summary>
        private readonly object lockObject;

        /// <summary>
        /// Initializes a new instance of the PersistentDictionaryCursorCache{TKey,TValue}
        /// class.
        /// </summary>
        /// <param name="instance">The ESENT instance to use when opening a cursor.</param>
        /// <param name="database">The database to open the cursors on.</param>
        /// <param name="converters">The converters the cursors should use.</param>
        /// <param name="config">The configuration for the cursors.</param>
        public PersistentDictionaryCursorCache(
            Instance instance,
            string database,
            PersistentDictionaryConverters<TKey, TValue> converters,
            IPersistentDictionaryConfig config)
        {
            this.instance = instance;
            this.converters = converters;
            this.config = config;
            this.database = database;
            this.cursors = new PersistentDictionaryCursor<TKey, TValue>[MaxCachedCursors];
            this.lockObject = new object();
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            for (int i = 0; i < this.cursors.Length; ++i)
            {
                if (null != this.cursors[i])
                {
                    this.cursors[i].Dispose();
                    this.cursors[i] = null;
                }
            }

            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Gets a new cursor. This will return a cached cursor if available,
        /// or create a new one.
        /// </summary>
        /// <returns>A new cursor.</returns>
        public PersistentDictionaryCursor<TKey, TValue> GetCursor()
        {
            lock (this.lockObject)
            {
                for (int i = 0; i < this.cursors.Length; ++i)
                {
                    if (null != this.cursors[i])
                    {
                        var cursor = this.cursors[i];
                        this.cursors[i] = null;
                        return cursor;
                    }
                }
            }

            // Didn't find a cached cursor, open a new one
            return this.OpenCursor();
        }

        /// <summary>
        /// Free a cursor. This will cache the cursor if the cache isn't full
        /// and dispose of it otherwise.
        /// </summary>
        /// <param name="cursor">The cursor to free.</param>
        public void FreeCursor(PersistentDictionaryCursor<TKey, TValue> cursor)
        {
            Debug.Assert(null != cursor, "Freeing a null cursor");

            lock (this.lockObject)
            {
                for (int i = 0; i < this.cursors.Length; ++i)
                {
                    if (null == this.cursors[i])
                    {
                        this.cursors[i] = cursor;
                        return;
                    }
                }
            }

            // Didn't find a slot to cache the cursor in
            cursor.Dispose();
        }

        /// <summary>
        /// Create a new cursor.
        /// </summary>
        /// <returns>A new cursor.</returns>
        private PersistentDictionaryCursor<TKey, TValue> OpenCursor()
        {
            return new PersistentDictionaryCursor<TKey, TValue>(
                this.instance, this.database, this.converters, this.config);
        }
    }
}