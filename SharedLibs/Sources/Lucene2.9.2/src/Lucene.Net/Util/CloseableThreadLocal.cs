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
using System.Collections.Concurrent;
using System.Threading;

namespace Lucene.Net.Util
{
	
	/// <summary>Java's builtin ThreadLocal has a serious flaw:
	/// it can take an arbitrarily long amount of time to
	/// dereference the things you had stored in it, even once the
	/// ThreadLocal instance itself is no longer referenced.
	/// This is because there is single, master map stored for
	/// each thread, which all ThreadLocals share, and that
	/// master map only periodically purges "stale" entries.
	/// 
	/// While not technically a memory leak, because eventually
	/// the memory will be reclaimed, it can take a long time
	/// and you can easily hit OutOfMemoryError because from the
	/// GC's standpoint the stale entries are not reclaimaible.
	/// 
	/// This class works around that, by only enrolling
	/// WeakReference values into the ThreadLocal, and
	/// separately holding a hard reference to each stored
	/// value.  When you call {@link #close}, these hard
	/// references are cleared and then GC is freely able to
	/// reclaim space by objects stored in it. 
	/// </summary>
	
    ///<remarks>
    /// .NET doesn't have this problem, this was adapted to use the .NET version
    /// </remarks>
	public class CloseableThreadLocal
	{
		readonly ThreadLocal<object> self ;
		readonly ConcurrentDictionary<object, object> threadLocals = new ConcurrentDictionary<object, object>();

		private static readonly object ignored = new object();

	    public CloseableThreadLocal()
	    {
	        self = new ThreadLocal<object>(() => Track(InitialValue()));
	    }

		private object Track(object initialValue)
		{
			if(initialValue != null)
			{
				threadLocals.AddOrUpdate(initialValue, ignored, (o, o1) => ignored);
			}
			return initialValue;
		}


		public  virtual System.Object InitialValue()
        {
            return null;
        }

        public virtual System.Object Get()
        {
            return self.Value;
        }
		
		public virtual void  Set(System.Object object_Renamed)
		{
			if(self.IsValueCreated && self.Value != null)
			{
				object _;
				threadLocals.TryRemove(self.Value, out _);
			}
			self.Value = object_Renamed;
			if(object_Renamed != null)
			{
				threadLocals.AddOrUpdate(object_Renamed, ignored, (k, v) => ignored);
			}
		}
		
		public virtual void  Close()
		{
			foreach (var threadLocal in threadLocals)
			{
				using (threadLocal.Key as IDisposable)
				{
					
				}
			}
            self.Dispose();
		}
	}
}
