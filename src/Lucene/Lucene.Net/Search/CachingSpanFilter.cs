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

using System.Runtime.InteropServices;
using Lucene.Net.Store;
using IndexReader = Lucene.Net.Index.IndexReader;

namespace Lucene.Net.Search
{

    /// <summary> Wraps another SpanFilter's result and caches it.  The purpose is to allow
    /// filters to simply filter, and then wrap with this class to add caching.
    /// </summary>
    [Serializable]
    public class CachingSpanFilter:SpanFilter
	{
		private SpanFilter filter;
		
		/// <summary> A transient Filter cache (internal because of test)</summary>
		[NonSerialized]
        internal CachingWrapperFilter.FilterCache<SpanFilterResult> cache;

        /// <summary>
        /// New deletions always result in a cache miss, by default
        /// (<see cref="CachingWrapperFilter.DeletesMode.RECACHE" />.
        /// <param name="filter">Filter to cache results of
		/// </param>
        /// </summary>
        public CachingSpanFilter(SpanFilter filter): this(filter, CachingWrapperFilter.DeletesMode.RECACHE)
		{
			
		}

        /// <summary>New deletions always result in a cache miss, specify the <paramref name="deletesMode"/></summary>
        /// <param name="filter">Filter to cache results of</param>
        /// <param name="deletesMode">See <see cref="CachingWrapperFilter.DeletesMode" /></param>
        public CachingSpanFilter(SpanFilter filter, CachingWrapperFilter.DeletesMode deletesMode)
        {
            this.filter = filter;
            if (deletesMode == CachingWrapperFilter.DeletesMode.DYNAMIC)
            {
                throw new System.ArgumentException("DeletesMode.DYNAMIC is not supported");
            }
            this.cache = new AnonymousFilterCache(deletesMode);
        }

        class AnonymousFilterCache : CachingWrapperFilter.FilterCache<SpanFilterResult>
        {
            public AnonymousFilterCache(CachingWrapperFilter.DeletesMode deletesMode) : base(deletesMode)
            {
            }

            protected override SpanFilterResult MergeDeletes(IndexReader reader, SpanFilterResult docIdSet)
            {
                throw new System.ArgumentException("DeletesMode.DYNAMIC is not supported");
            }
        }
		
		public override DocIdSet GetDocIdSet(IndexReader reader, IState state)
		{
			SpanFilterResult result = GetCachedResult(reader, state);
			return result != null?result.DocIdSet:null;
		}

        // for testing
        public int hitCount, missCount;

		private SpanFilterResult GetCachedResult(IndexReader reader, IState state)
		{
            object coreKey = reader.FieldCacheKey;
            object delCoreKey = reader.HasDeletions ? reader.DeletesCacheKey : coreKey;

            SpanFilterResult result = cache.Get(reader, coreKey, delCoreKey);
            if (result != null) {
                hitCount++;
                return result;
            }

            missCount++;
            result = filter.BitSpans(reader, state);

            cache.Put(coreKey, delCoreKey, result);
            return result;
		}
		
		
		public override SpanFilterResult BitSpans(IndexReader reader, IState state)
		{
			return GetCachedResult(reader, state);
		}
		
		public override System.String ToString()
		{
			return "CachingSpanFilter(" + filter + ")";
		}
		
		public  override bool Equals(System.Object o)
		{
			if (!(o is CachingSpanFilter))
				return false;
			return this.filter.Equals(((CachingSpanFilter) o).filter);
		}
		
		public override int GetHashCode()
		{
			return filter.GetHashCode() ^ 0x1117BF25;
		}
	}
}