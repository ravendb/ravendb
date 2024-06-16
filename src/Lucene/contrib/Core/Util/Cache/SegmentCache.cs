/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

using Lucene.Net.Index;

namespace Lucene.Net.Util.Cache
{
    /// <summary>
    /// Root custom cache to allow a factory to retain references to the custom
    /// caches without having to be aware of the type.
    /// </summary>
    public abstract class AbstractSegmentCache
    {
        /// <summary>
        /// Used to warm up the cache.
        /// </summary>
        /// <param name="reader">The reader to warm the cache for.</param>
        /// <param name="key">The inner key.</param>
        public abstract void Warm(IndexReader reader, string key);
    }

    /// <summary>
    /// Custom cache with two levels of keys, outer key is the IndexReader
    /// with the inner key being a string, commonly a field name but can be anything.
    /// Refer to the unit tests for an example implementation.
    /// <typeparam name="T">The type that is being cached.</typeparam>
    /// </summary>
    public abstract class SegmentCache<T> : AbstractSegmentCache
    {
        /// <summary>
        /// The cache - outer key is the reader, inner key is the field name. Value is the item desired.
        /// </summary>
        private Dictionary<WeakKey, Dictionary<string, T>> readerCache = new Dictionary<WeakKey, Dictionary<string, T>>();

        /// <summary>
        /// Lock to use when accessing the cache.
        /// </summary>
        private ReaderWriterLockSlim cacheLock = new ReaderWriterLockSlim();

        /// <summary>
        /// Value creation.
        /// </summary>
        /// <param name="reader">The reader.</param>
        /// <param name="key">The key to the item under the reader.</param>
        /// <returns>The value.</returns>
        protected abstract T CreateValue(IndexReader reader, string key);

        /// <summary>
        /// The number of outermost keys in the collection.
        /// </summary>
        public int KeyCount
        {
            get { return this.readerCache.Count; }
        }

        /// <summary>
        /// Warm the cache - simply calls Get and ignores the return value.
        /// </summary>
        /// <param name="reader">The index reader to warm up.</param>
        /// <param name="key">The key of the item under the reader.</param>
        public override void Warm(IndexReader reader, string key)
        {
            this.Get(reader, key);
        }

        /// <summary>
        /// Get the item from the cache.
        /// </summary>
        /// <param name="reader">The IndexReader the cache is from.</param>
        /// <param name="key">The key of the item under the reader.</param>
        /// <returns>The item from cache.</returns>
        public virtual T Get(IndexReader reader, string key)
        {
            WeakKey readerRef = new SegmentCache<T>.WeakKey(reader);

            Dictionary<string, T> innerCache;
            T retVal = default(T);
            this.cacheLock.EnterReadLock();
            try
            {
                if (readerCache.TryGetValue(readerRef, out innerCache))
                {
                    innerCache.TryGetValue(key, out retVal);
                }
            }
            finally
            {
                this.cacheLock.ExitReadLock();
            }

            if (retVal == null)
            {
                retVal = this.CreateValue(reader, key);
                this.cacheLock.EnterWriteLock();
                try
                {
                    if (!readerCache.TryGetValue(readerRef, out innerCache))
                    {
                        innerCache = new Dictionary<string, T>();
                        readerCache.Add(readerRef, innerCache);
                    }
                    if (!innerCache.ContainsKey(key))
                    {
                        innerCache[key] = retVal;
                    }
                    else
                    {
                        // another thread must have put it in while waiting for the write lock
                        // assumption is that the previous thread already flushed the old items
                        return retVal;
                    }

                    // release the old items and yank the gc'd weak references
                    var keys = from wr in this.readerCache.Keys where !wr.IsAlive select wr;
                    List<WeakKey> keysToRemove = keys.ToList();
                    foreach (WeakKey wk in keysToRemove)
                    {
                        this.readerCache.Remove(wk);
                    }
                }
                finally
                {
                    this.cacheLock.ExitWriteLock();
                }
            }

            return retVal;
        }


        /// <summary>
        /// A weak referene wrapper for the hashtable keys. Whenever a key\value pair 
        /// is added to the hashtable, the key is wrapped using a WeakKey. WeakKey saves the
        /// value of the original object hashcode for fast comparison.
        /// </summary>
        internal class WeakKey : WeakReference
        {
            /// <summary>
            /// The hashcode for the target.
            /// </summary>
            private int hashCode;

            /// <summary>
            /// Create a new WeakKey
            /// </summary>
            /// <param name="target">The object to use as the target.</param>
            internal WeakKey(object target)
                : base(target)
            {
                this.hashCode = target.GetHashCode();
            }

            /// <summary>
            /// The hash code accessor.
            /// </summary>
            /// <returns></returns>
            public override int GetHashCode()
            {
                return this.hashCode;
            }

            /// <summary>
            /// Equality between keys.
            /// </summary>
            /// <param name="obj">The object to compare to.</param>
            /// <returns>True if they are equivalent.</returns>
            public override bool Equals(object obj)
            {
                WeakKey other = obj as WeakKey;
                if (other == null)
                {
                    return false;
                }

                object a = this.Target;
                object b = other.Target;

                if (a == null && b == null)
                {
                    return true;
                }
                else if (a == null || b == null)
                {
                    return false;
                }
                else
                {
                    return a.Equals(b);
                }
            }
        }
    }
}
