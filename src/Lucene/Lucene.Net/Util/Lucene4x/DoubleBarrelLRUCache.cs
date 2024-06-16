using Lucene.Net.Support;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Lucene.Net.Support.Lucene4x;

namespace Lucene.Net.Util.Lucene4x
{
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

    /// <summary>
    /// Simple concurrent LRU cache, using a "double barrel"
    /// approach where two ConcurrentHashMaps record entries.
    ///
    /// <para>At any given time, one hash is primary and the other
    /// is secondary.  <see cref="Get(TKey)"/> first checks primary, and if
    /// that's a miss, checks secondary.  If secondary has the
    /// entry, it's promoted to primary (<b>NOTE</b>: the key is
    /// cloned at this point).  Once primary is full, the
    /// secondary is cleared and the two are swapped.</para>
    ///
    /// <para>This is not as space efficient as other possible
    /// concurrent approaches (see LUCENE-2075): to achieve
    /// perfect LRU(N) it requires 2*N storage.  But, this
    /// approach is relatively simple and seems in practice to
    /// not grow unbounded in size when under hideously high
    /// load.</para>
    ///
    /// @lucene.internal
    /// </summary>
    public sealed class DoubleBarrelLRUCache<TKey, TValue> : DoubleBarrelLRUCache where TKey : DoubleBarrelLRUCache.CloneableKey
    {
        private readonly IDictionary<TKey, TValue> cache1;
        private readonly IDictionary<TKey, TValue> cache2;

        private readonly AtomicInt32 countdown;

        private volatile bool swapped;
        private readonly int maxSize;

        public DoubleBarrelLRUCache(int maxCount)
        {
            this.maxSize = maxCount;
            countdown = new AtomicInt32(maxCount);
            cache1 = new ConcurrentDictionary<TKey, TValue>();
            cache2 = new ConcurrentDictionary<TKey, TValue>();
        }

        public TValue Get(TKey key)
        {
            IDictionary<TKey, TValue> primary;
            IDictionary<TKey, TValue> secondary;
            if (swapped)
            {
                primary = cache2;
                secondary = cache1;
            }
            else
            {
                primary = cache1;
                secondary = cache2;
            }

            // Try primary first
            TValue result;
            if (!primary.TryGetValue(key, out result))
            {
                // Not found -- try secondary
                if (secondary.TryGetValue(key, out result))
                {
                    // Promote to primary
                    Put((TKey)key.Clone(), result);
                }
            }
            return result;
        }

        public void Put(TKey key, TValue value)
        {
            IDictionary<TKey, TValue> primary;
            IDictionary<TKey, TValue> secondary;
            if (swapped)
            {
                primary = cache2;
                secondary = cache1;
            }
            else
            {
                primary = cache1;
                secondary = cache2;
            }
            primary[key] = value;

            if (countdown.DecrementAndGet() == 0)
            {
                // Time to swap

                // NOTE: there is saturation risk here, that the
                // thread that's doing the clear() takes too long to
                // do so, while other threads continue to add to
                // primary, but in practice this seems not to be an
                // issue (see LUCENE-2075 for benchmark & details)

                // First, clear secondary
                secondary.Clear();

                // Second, swap
                swapped = !swapped;

                // Third, reset countdown
                countdown.Set(maxSize);
            }
        }
    }

    /// <summary>
    /// LUCENENET specific class to nest the <see cref="CloneableKey"/>
    /// so it can be accessed without referencing the generic closing types
    /// of <see cref="DoubleBarrelLRUCache{TKey, TValue}"/>.
    /// </summary>
    public abstract class DoubleBarrelLRUCache
    {
        /// <summary>
        /// Object providing clone(); the key class must subclass this.
        /// </summary>
        public abstract class CloneableKey
        {
            public abstract object Clone();
        }
    }
}