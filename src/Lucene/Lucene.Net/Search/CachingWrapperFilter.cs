/* 
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using Lucene.Net.Store;
using Lucene.Net.Support;
using IndexReader = Lucene.Net.Index.IndexReader;
using OpenBitSetDISI = Lucene.Net.Util.OpenBitSetDISI;
using Lucene.Net.Util;

namespace Lucene.Net.Search
{

    /// <summary> Wraps another filter's result and caches it.  The purpose is to allow
    /// filters to simply filter, and then wrap with this class to add caching.
    /// </summary>
    [Serializable]
    public class CachingWrapperFilter:Filter
	{
		protected internal Filter filter;

        ///
        /// Expert: Specifies how new deletions against a reopened
        /// reader should be handled.
        ///
        /// <para>The default is IGNORE, which means the cache entry
        /// will be re-used for a given segment, even when that
        /// segment has been reopened due to changes in deletions.
        /// This is a big performance gain, especially with
        /// near-real-timer readers, since you don't hit a cache
        /// miss on every reopened reader for prior segments.</para>
        ///
        /// <para>However, in some cases this can cause invalid query
        /// results, allowing deleted documents to be returned.
        /// This only happens if the main query does not rule out
        /// deleted documents on its own, such as a toplevel
        /// ConstantScoreQuery.  To fix this, use RECACHE to
        /// re-create the cached filter (at a higher per-reopen
        /// cost, but at faster subsequent search performance), or
        /// use DYNAMIC to dynamically intersect deleted docs (fast
        /// reopen time but some hit to search performance).</para>
        ///
        public enum DeletesMode { IGNORE, RECACHE, DYNAMIC }

		internal FilterCache<DocIdSet> cache;

        [Serializable]
        abstract internal class FilterCache<T> where T : class
        {
            /*
             * A transient Filter cache (package private because of test)
             */
            // NOTE: not final so that we can dynamically re-init
            // after de-serialize
            volatile ConditionalWeakTable<Object, T> _cache;

            private DeletesMode deletesMode;

            public FilterCache(DeletesMode deletesMode)
            {
                this.deletesMode = deletesMode;
                _cache = new ConditionalWeakTable<object, T>();
            }

            public T Get(IndexReader reader, object coreKey, object delCoreKey)
            {
                T value;

                if (deletesMode == DeletesMode.IGNORE)
                {
                    // key on core
                    _cache.TryGetValue(coreKey, out value);
                }
                else if (deletesMode == DeletesMode.RECACHE)
                {
                    // key on deletes, if any, else core
                    _cache.TryGetValue(delCoreKey, out value);
                }
                else
                {

                    System.Diagnostics.Debug.Assert(deletesMode == DeletesMode.DYNAMIC);

                    // first try for exact match
                    _cache.TryGetValue(delCoreKey, out value);

                    if (value == null)
                    {
                        // now for core match, but dynamically AND NOT
                        // deletions
                        _cache.TryGetValue(coreKey, out value);
                        if (value != null && reader.HasDeletions)
                        {
                            value = MergeDeletes(reader, value);
                        }
                    }
                }

                return value;

            }
       
            protected abstract T MergeDeletes(IndexReader reader, T value);

            public void Put(object coreKey, object delCoreKey, T value)
            {
                if (deletesMode == DeletesMode.IGNORE)
                {
                    _cache.AddOrUpdate(coreKey, value);
                }
                else if (deletesMode == DeletesMode.RECACHE)
                {
                    _cache.AddOrUpdate(delCoreKey, value);
                }
                else
                {
                    _cache.AddOrUpdate(coreKey, value);
                    _cache.AddOrUpdate(delCoreKey, value);
                }
            }
        }

        /// <summary>
        /// New deletes are ignored by default, which gives higher
        /// cache hit rate on reopened readers.  Most of the time
        /// this is safe, because the filter will be AND'd with a
        /// Query that fully enforces deletions.  If instead you
        /// need this filter to always enforce deletions, pass
        /// either <see cref="DeletesMode.RECACHE" /> or
        /// <see cref="DeletesMode.DYNAMIC"/>.
        /// </summary>
        /// <param name="filter">Filter to cache results of</param>
        ///
        public CachingWrapperFilter(Filter filter) : this(filter, DeletesMode.IGNORE)
		{
		}

        /// <summary>
        /// Expert: by default, the cached filter will be shared
        /// across reopened segments that only had changes to their
        /// deletions.  
        /// </summary>
        /// <param name="filter">Filter to cache results of</param>
        /// <param name="deletesMode">See <see cref="DeletesMode" /></param>
        ///
        public CachingWrapperFilter(Filter filter, DeletesMode deletesMode)
        {
            this.filter = filter;
            cache = new AnonymousFilterCache(deletesMode);
            
            //cache = new FilterCache(deletesMode) 
            // {
            //  public Object mergeDeletes(final IndexReader r, final Object docIdSet) {
            //    return new FilteredDocIdSet((DocIdSet) docIdSet) {
            //      protected boolean match(int docID) {
            //        return !r.isDeleted(docID);
            //      }
            //    };
            //  }
            //};
        }

        class AnonymousFilterCache : FilterCache<DocIdSet>
        {
            class AnonymousFilteredDocIdSet : FilteredDocIdSet
            {
                IndexReader r;
                public AnonymousFilteredDocIdSet(DocIdSet innerSet, IndexReader r) : base(innerSet)
                {
                    this.r = r;
                }
                public override bool Match(int docid)
                {
                    return !r.IsDeleted(docid);
                }
            }

            public AnonymousFilterCache(DeletesMode deletesMode) : base(deletesMode)
            { }

            protected override DocIdSet MergeDeletes(IndexReader reader, DocIdSet docIdSet)
            {
                return new AnonymousFilteredDocIdSet(docIdSet, reader);
            }
        }

		/// <summary>Provide the DocIdSet to be cached, using the DocIdSet provided
		/// by the wrapped Filter.
		/// This implementation returns the given DocIdSet.
		/// </summary>
		protected internal virtual DocIdSet DocIdSetToCache(DocIdSet docIdSet, IndexReader reader, IState state)
		{
            if (docIdSet == null)
            {
                // this is better than returning null, as the nonnull result can be cached
                return DocIdSet.EMPTY_DOCIDSET;
            }
            else if (docIdSet.IsCacheable) {
				return docIdSet;
			}
			else
			{
				DocIdSetIterator it = docIdSet.Iterator(state);
				// null is allowed to be returned by iterator(),
				// in this case we wrap with the empty set,
				// which is cacheable.
				return (it == null) ? DocIdSet.EMPTY_DOCIDSET : new OpenBitSetDISI(it, reader.MaxDoc, state);
			}
		}

        // for testing
        public int hitCount, missCount;
		
		public override DocIdSet GetDocIdSet(IndexReader reader, IState state)
		{
			object coreKey = reader.FieldCacheKey;
            object delCoreKey = reader.HasDeletions ? reader.DeletesCacheKey : coreKey;

            DocIdSet docIdSet = cache.Get(reader, coreKey, delCoreKey);

            if (docIdSet != null)
			{
                hitCount++;
			    return docIdSet;
			}
            missCount++;
            // cache miss
			docIdSet = DocIdSetToCache(filter.GetDocIdSet(reader, state), reader, state);
			
			if (docIdSet != null)
			{
                cache.Put(coreKey, delCoreKey, docIdSet);
			}

			return docIdSet;
		}
		
		public override System.String ToString()
		{
			return "CachingWrapperFilter(" + filter + ")";
		}
		
		public  override bool Equals(System.Object o)
		{
			if (!(o is CachingWrapperFilter))
				return false;
			return this.filter.Equals(((CachingWrapperFilter) o).filter);
		}
		
		public override int GetHashCode()
		{
			return filter.GetHashCode() ^ 0x1117BF25;
		}
	}
}