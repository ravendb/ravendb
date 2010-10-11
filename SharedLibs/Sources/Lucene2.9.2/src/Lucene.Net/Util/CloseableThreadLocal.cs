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
	
	public class CloseableThreadLocal
	{
		
		[ThreadStatic]
		static System.Collections.Generic.Dictionary<object, object> slots;
		
		public /*protected internal*/ virtual System.Object InitialValue()
		{
			return null;
		}

        public virtual System.Object Get()
        {
            object value;

            if (slots == null)
            {
                value = InitialValue();
                if (value != null)
                    Set(value);

                return value;
            }

            if (slots.TryGetValue(this, out value))
            {
                return value;
            }
            else
            {
                value = InitialValue();
                slots[this] = value;
                return value;
            }
        }
		
		public virtual void  Set(System.Object object_Renamed)
		{
			if (slots == null)
				slots = new System.Collections.Generic.Dictionary<object, object>();

			slots[this] = object_Renamed;	
		}
		
		public virtual void  Close()
		{
            if(slots != null)
                slots.Remove(this);
		}
	}
}
