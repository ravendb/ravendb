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
using IndexReader = Lucene.Net.Index.IndexReader;

namespace Lucene.Net.Search
{
	
	/// <summary> Wraps another SpanFilter's result and caches it.  The purpose is to allow
	/// filters to simply filter, and then wrap with this class to add caching.
	/// </summary>
	[Serializable]
	public class CachingSpanFilter:SpanFilter
	{
		protected internal SpanFilter filter;
		
		/// <summary> A transient Filter cache.</summary>
		[NonSerialized]
		protected internal System.Collections.IDictionary cache;
		
		/// <param name="filter">Filter to cache results of
		/// </param>
		public CachingSpanFilter(SpanFilter filter)
		{
			this.filter = filter;
		}
		
		/// <deprecated> Use {@link #GetDocIdSet(IndexReader)} instead.
		/// </deprecated>
        [Obsolete("Use GetDocIdSet(IndexReader) instead.")]
		public override System.Collections.BitArray Bits(IndexReader reader)
		{
			SpanFilterResult result = GetCachedResult(reader);
			return result != null?result.GetBits():null;
		}
		
		public override DocIdSet GetDocIdSet(IndexReader reader)
		{
			SpanFilterResult result = GetCachedResult(reader);
			return result != null?result.GetDocIdSet():null;
		}
		
		private SpanFilterResult GetCachedResult(IndexReader reader)
		{
			SpanFilterResult result = null;
			if (cache == null)
			{
                cache = new SupportClass.WeakHashTable();
			}
			
			lock (cache.SyncRoot)
			{
				// check cache
				result = (SpanFilterResult) cache[reader];
				if (result == null)
				{
					result = filter.BitSpans(reader);
					cache[reader] = result;
				}
			}
			return result;
		}
		
		
		public override SpanFilterResult BitSpans(IndexReader reader)
		{
			return GetCachedResult(reader);
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