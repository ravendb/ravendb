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
using DocIdBitSet = Lucene.Net.Util.DocIdBitSet;
using OpenBitSetDISI = Lucene.Net.Util.OpenBitSetDISI;

namespace Lucene.Net.Search
{
	
	/// <summary> Wraps another filter's result and caches it.  The purpose is to allow
	/// filters to simply filter, and then wrap with this class to add caching.
	/// </summary>
	[Serializable]
	public class CachingWrapperFilter:Filter
	{
		protected internal Filter filter;
		
		/// <summary> A transient Filter cache.</summary>
		[NonSerialized]
		protected internal System.Collections.IDictionary cache;
		
		/// <param name="filter">Filter to cache results of
		/// </param>
		public CachingWrapperFilter(Filter filter)
		{
			this.filter = filter;
		}
		
		/// <deprecated> Use {@link #GetDocIdSet(IndexReader)} instead.
		/// </deprecated>
        [Obsolete("Use GetDocIdSet(IndexReader) instead.")]
		public override System.Collections.BitArray Bits(IndexReader reader)
		{
			if (cache == null)
			{
                cache = new SupportClass.WeakHashTable();
			}
			
			System.Object cached = null;
			lock (cache.SyncRoot)
			{
				// check cache
				cached = cache[reader];
			}
			
			if (cached != null)
			{
				if (cached is System.Collections.BitArray)
				{
					return (System.Collections.BitArray) cached;
				}
				else if (cached is DocIdBitSet)
					return ((DocIdBitSet) cached).GetBitSet();
				// It would be nice to handle the DocIdSet case, but that's not really possible
			}
			
			System.Collections.BitArray bits = filter.Bits(reader);
			
			lock (cache.SyncRoot)
			{
				// update cache
				cache[reader] = bits;
			}
			
			return bits;
		}
		
		/// <summary>Provide the DocIdSet to be cached, using the DocIdSet provided
		/// by the wrapped Filter.
		/// This implementation returns the given DocIdSet.
		/// </summary>
		protected internal virtual DocIdSet DocIdSetToCache(DocIdSet docIdSet, IndexReader reader)
		{
			if (docIdSet.IsCacheable())
			{
				return docIdSet;
			}
			else
			{
				DocIdSetIterator it = docIdSet.Iterator();
				// null is allowed to be returned by iterator(),
				// in this case we wrap with the empty set,
				// which is cacheable.
				return (it == null) ? DocIdSet.EMPTY_DOCIDSET : new OpenBitSetDISI(it, reader.MaxDoc());
			}
		}
		
		public override DocIdSet GetDocIdSet(IndexReader reader)
		{
			if (cache == null)
			{
                cache = new SupportClass.WeakHashTable();
			}
			
			System.Object cached = null;
			lock (cache.SyncRoot)
			{
				// check cache
				cached = cache[reader];
			}
			
			if (cached != null)
			{
				if (cached is DocIdSet)
					return (DocIdSet) cached;
				else
					return new DocIdBitSet((System.Collections.BitArray) cached);
			}
			
			DocIdSet docIdSet = DocIdSetToCache(filter.GetDocIdSet(reader), reader);
			
			if (docIdSet != null)
			{
				lock (cache.SyncRoot)
				{
					// update cache
					cache[reader] = docIdSet;
				}
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