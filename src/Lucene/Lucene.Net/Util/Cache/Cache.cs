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

namespace Lucene.Net.Util.Cache
{
	
	
	/// <summary> Base class for cache implementations.</summary>
	public abstract class Cache<TKey, TValue> : IDisposable
	{
		
		/// <summary> Simple Cache wrapper that synchronizes all
		/// calls that access the cache. 
		/// </summary>
		internal class SynchronizedCache_Renamed_Class : Cache<TKey, TValue>
		{
			internal System.Object mutex;
			internal Cache<TKey,TValue> cache;

            internal SynchronizedCache_Renamed_Class(Cache<TKey, TValue> cache)
			{
				this.cache = cache;
				this.mutex = this;
			}

            internal SynchronizedCache_Renamed_Class(Cache<TKey, TValue> cache, System.Object mutex)
			{
				this.cache = cache;
				this.mutex = mutex;
			}
			
			public override void Put(TKey key, TValue value_Renamed)
			{
				lock (mutex)
				{
					cache.Put(key, value_Renamed);
				}
			}
			
			public override TValue Get(System.Object key)
			{
				lock (mutex)
				{
					return cache.Get(key);
				}
			}
			
			public override bool ContainsKey(System.Object key)
			{
				lock (mutex)
				{
					return cache.ContainsKey(key);
				}
			}
			
            protected override void Dispose(bool disposing)
            {
                lock (mutex)
                {
                    cache.Dispose();
                }
            }
			
			internal override Cache<TKey,TValue> GetSynchronizedCache()
			{
				return this;
			}
		}
		
		/// <summary> Returns a thread-safe cache backed by the specified cache. 
		/// In order to guarantee thread-safety, all access to the backed cache must
		/// be accomplished through the returned cache.
		/// </summary>
        public static Cache<TKey, TValue> SynchronizedCache(Cache<TKey, TValue> cache)
		{
			return cache.GetSynchronizedCache();
		}
		
		/// <summary> Called by <see cref="SynchronizedCache(Cache{TKey,TValue})" />. This method
		/// returns a <see cref="SynchronizedCache" /> instance that wraps
		/// this instance by default and can be overridden to return
		/// e. g. subclasses of <see cref="SynchronizedCache" /> or this
		/// in case this cache is already synchronized.
		/// </summary>
        internal virtual Cache<TKey, TValue> GetSynchronizedCache()
		{
            return new SynchronizedCache_Renamed_Class(this);
		}
		
		/// <summary> Puts a (key, value)-pair into the cache. </summary>
		public abstract void  Put(TKey key, TValue value_Renamed);
		
		/// <summary> Returns the value for the given key. </summary>
		public abstract TValue Get(System.Object key);
		
		/// <summary> Returns whether the given key is in this cache. </summary>
		public abstract bool ContainsKey(System.Object key);

	    /// <summary> Closes the cache.</summary>
	    [Obsolete("Use Dispose() instead")]
	    public void Close()
	    {
	        Dispose();
	    }

	    public void Dispose()
	    {
	        Dispose(true);
	    }

	    protected abstract void Dispose(bool disposing);
	}
}