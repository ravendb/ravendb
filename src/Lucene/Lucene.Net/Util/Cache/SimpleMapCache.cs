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

namespace Lucene.Net.Util.Cache
{
	
	/// <summary> Simple cache implementation that uses a HashMap to store (key, value) pairs.
    /// This cache is not synchronized, use <see cref="Cache{TKey, TValue}.SynchronizedCache(Cache{TKey, TValue})" />
	/// if needed.
	/// </summary>
	public class SimpleMapCache<TKey, TValue> : Cache<TKey, TValue>
	{
		internal System.Collections.Generic.Dictionary<TKey, TValue> map;

        public SimpleMapCache()
            : this(new System.Collections.Generic.Dictionary<TKey, TValue>())
		{
		}

        public SimpleMapCache(System.Collections.Generic.Dictionary<TKey, TValue> map)
		{
			this.map = map;
		}
		
		public override TValue Get(System.Object key)
		{
			return map[(TKey)key];
		}
		
		public override void  Put(TKey key, TValue value_Renamed)
		{
			map[key] = value_Renamed;
		}
		
		public override bool ContainsKey(System.Object key)
		{
			return map.ContainsKey((TKey)key);
		}

	    protected override void Dispose(bool disposing)
	    {
	        // do nothing
	    }

	    /// <summary> Returns a Set containing all keys in this cache.</summary>
		public virtual System.Collections.Generic.HashSet<TKey> KeySet()
		{
			return new HashSet<TKey>(map.Keys);
		}
		
		internal override Cache<TKey, TValue> GetSynchronizedCache()
		{
			return new SynchronizedSimpleMapCache(this);
		}
		
        // Why does does this use both inheritance and composition?
		private class SynchronizedSimpleMapCache : SimpleMapCache<TKey, TValue>
		{
			private System.Object mutex;
            private SimpleMapCache<TKey, TValue> cache;

		    private bool isDisposed;

            internal SynchronizedSimpleMapCache(SimpleMapCache<TKey, TValue> cache)
			{
				this.cache = cache;
				this.mutex = this;
			}
			
			public override void  Put(TKey key, TValue value_Renamed)
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
                    if (isDisposed) return;

                    if (disposing)
                    {
                        cache.Dispose(disposing);
                    }

                    isDisposed = true;
                    base.Dispose(disposing);
				}
			}
			
			public override HashSet<TKey> KeySet()
			{
				lock (mutex)
				{
					return cache.KeySet();
				}
			}
			
			internal override Cache<TKey, TValue> GetSynchronizedCache()
			{
				return this;
			}
		}
	}
}