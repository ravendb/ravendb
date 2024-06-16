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
using System.Collections.Generic;
using System.Linq;
using Lucene.Net.Store;
using Lucene.Net.Support;

namespace Lucene.Net.Search
{
	
	/// <summary> Filter caching singleton.  It can be used 
	/// to save filters locally for reuse.
	/// This class makes it possble to cache Filters even when using RMI, as it
	/// keeps the cache on the seaercher side of the RMI connection.
	/// 
	/// Also could be used as a persistent storage for any filter as long as the
	/// filter provides a proper hashCode(), as that is used as the key in the cache.
	/// 
	/// The cache is periodically cleaned up from a separate thread to ensure the
	/// cache doesn't exceed the maximum size.
	/// </summary>
	public class FilterManager
	{
		
		protected internal static FilterManager manager;
		
		/// <summary>The default maximum number of Filters in the cache </summary>
		protected internal const int DEFAULT_CACHE_CLEAN_SIZE = 100;
		/// <summary>The default frequency of cache clenup </summary>
		protected internal const long DEFAULT_CACHE_SLEEP_TIME = 1000 * 60 * 10;
		
		/// <summary>The cache itself </summary>
		protected internal IDictionary<int, FilterItem> cache;
		/// <summary>Maximum allowed cache size </summary>
		protected internal int cacheCleanSize;
		/// <summary>Cache cleaning frequency </summary>
		protected internal long cleanSleepTime;
		/// <summary>Cache cleaner that runs in a separate thread </summary>
		protected internal FilterCleaner internalFilterCleaner;

	    private static readonly object _staticSyncObj = new object();
	    public static FilterManager Instance
	    {
	        get
	        {
	            lock (_staticSyncObj)
	            {
	                return manager ?? (manager = new FilterManager());
	            }
	        }
	    }

	    /// <summary> Sets up the FilterManager singleton.</summary>
		protected internal FilterManager()
		{
			cache = new HashMap<int, FilterItem>();
			cacheCleanSize = DEFAULT_CACHE_CLEAN_SIZE; // Let the cache get to 100 items
			cleanSleepTime = DEFAULT_CACHE_SLEEP_TIME; // 10 minutes between cleanings
			
			internalFilterCleaner = new FilterCleaner(this);
			ThreadClass fcThread = new ThreadClass(new System.Threading.ThreadStart(internalFilterCleaner.Run));
			// setto be a Daemon so it doesn't have to be stopped
			fcThread.IsBackground = true;
			fcThread.Start();
		}

	    /// <summary> Sets the max size that cache should reach before it is cleaned up</summary>
	    /// <param name="value"> maximum allowed cache size </param>
	    public virtual void SetCacheSize(int value)
	    {
	        this.cacheCleanSize = value;
	    }

	    /// <summary> Sets the cache cleaning frequency in milliseconds.</summary>
	    /// <param name="value"> cleaning frequency in millioseconds </param>
	    public virtual void SetCleanThreadSleepTime(long value)
	    {
	        this.cleanSleepTime = value;
	    }

	    /// <summary> Returns the cached version of the filter.  Allows the caller to pass up
		/// a small filter but this will keep a persistent version around and allow
		/// the caching filter to do its job.
		/// 
		/// </summary>
		/// <param name="filter">The input filter
		/// </param>
		/// <returns> The cached version of the filter
		/// </returns>
		public virtual Filter GetFilter(Filter filter)
		{
			lock (cache)
			{
				FilterItem fi = null;
				fi = cache[filter.GetHashCode()];
				if (fi != null)
				{
					fi.timestamp = System.DateTime.UtcNow.Ticks;
					return fi.filter;
				}
				cache[filter.GetHashCode()] = new FilterItem(filter);
				return filter;
			}
		}
		
		/// <summary> Holds the filter and the last time the filter was used, to make LRU-based
		/// cache cleaning possible.
		/// TODO: Clean this up when we switch to Java 1.5
		/// </summary>
		protected internal class FilterItem
		{
			public Filter filter;
			public long timestamp;
			
			public FilterItem(Filter filter)
			{
				this.filter = filter;
				this.timestamp = System.DateTime.UtcNow.Ticks;
			}
		}
		
		
		/// <summary> Keeps the cache from getting too big.
		/// If we were using Java 1.5, we could use LinkedHashMap and we would not need this thread
		/// to clean out the cache.
		/// 
		/// The SortedSet sortedFilterItems is used only to sort the items from the cache,
		/// so when it's time to clean up we have the TreeSet sort the FilterItems by
		/// timestamp.
		/// 
		/// Removes 1.5 * the numbers of items to make the cache smaller.
		/// For example:
		/// If cache clean size is 10, and the cache is at 15, we would remove (15 - 10) * 1.5 = 7.5 round up to 8.
		/// This way we clean the cache a bit more, and avoid having the cache cleaner having to do it frequently.
		/// </summary>
		protected internal class FilterCleaner : IThreadRunnable
		{
            private class FilterItemComparer : IComparer<KeyValuePair<int, FilterItem>>
            {
                #region IComparer<FilterItem> Members

                public int Compare(KeyValuePair<int, FilterItem> x, KeyValuePair<int, FilterItem> y)
                {
                    return x.Value.timestamp.CompareTo(y.Value.timestamp);
                }

                #endregion
            }
			
			private bool running = true;
            private FilterManager manager;
            private ISet<KeyValuePair<int, FilterItem>> sortedFilterItems;
			
			public FilterCleaner(FilterManager enclosingInstance)
			{
                this.manager = enclosingInstance;
                sortedFilterItems = new SortedSet<KeyValuePair<int, FilterItem>>(new FilterItemComparer());
            }
			
			public virtual void  Run()
			{
				while (running)
				{
					// sort items from oldest to newest 
					// we delete the oldest filters 
                    if (this.manager.cache.Count > this.manager.cacheCleanSize)
					{
						// empty the temporary set
						sortedFilterItems.Clear();
                        lock (this.manager.cache)
						{
                            sortedFilterItems.UnionWith(this.manager.cache);
                            int numToDelete = (int)((this.manager.cache.Count - this.manager.cacheCleanSize) * 1.5);

                            //delete all of the cache entries not used in a while
                            sortedFilterItems.ExceptWith(sortedFilterItems.Take(numToDelete).ToArray());
						}
						// empty the set so we don't tie up the memory
                        sortedFilterItems.Clear();
					}
					// take a nap
					System.Threading.Thread.Sleep(new System.TimeSpan((System.Int64)10000 * this.manager.cleanSleepTime));
					
				}
			}
		}
	}
}