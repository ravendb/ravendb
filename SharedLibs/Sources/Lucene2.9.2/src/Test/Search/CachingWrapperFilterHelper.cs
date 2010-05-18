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

using NUnit.Framework;

using IndexReader = Lucene.Net.Index.IndexReader;

namespace Lucene.Net.Search
{
	
	/// <summary> A unit test helper class to test when the filter is getting cached and when it is not.</summary>
	[Serializable]
	public class CachingWrapperFilterHelper:CachingWrapperFilter
	{
		
		private bool shouldHaveCache = false;
		
		/// <param name="filter">Filter to cache results of
		/// </param>
		public CachingWrapperFilterHelper(Filter filter):base(filter)
		{
		}
		
		public virtual void  SetShouldHaveCache(bool shouldHaveCache)
		{
			this.shouldHaveCache = shouldHaveCache;
		}
		
		public override DocIdSet GetDocIdSet(IndexReader reader)
		{
			if (cache == null)
			{
				cache = new System.Collections.Hashtable();
			}
			
			lock (cache.SyncRoot)
			{
				// check cache
				DocIdSet cached = (DocIdSet) cache[reader];
				if (shouldHaveCache)
				{
					Assert.IsNotNull(cached, "Cache should have data ");
				}
				else
				{
					Assert.IsNotNull( cached, "Cache should be null " + cached);
				}
				if (cached != null)
				{
					return cached;
				}
			}
			
			DocIdSet bits = filter.GetDocIdSet(reader);
			
			lock (cache.SyncRoot)
			{
				// update cache
				cache[reader] = bits;
			}
			
			return bits;
		}
		
		public override System.String ToString()
		{
			return "CachingWrapperFilterHelper(" + filter + ")";
		}
		
		public  override bool Equals(System.Object o)
		{
			if (!(o is CachingWrapperFilterHelper))
				return false;
			return this.filter.Equals((CachingWrapperFilterHelper) o);
		}
		
		public override int GetHashCode()
		{
			return this.filter.GetHashCode() ^ 0x5525aacb;
		}
	}
}