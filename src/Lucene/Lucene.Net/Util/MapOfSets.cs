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

namespace Lucene.Net.Util
{
	
	/// <summary> Helper class for keeping Listss of Objects associated with keys. <b>WARNING: THIS CLASS IS NOT THREAD SAFE</b></summary>
    public class MapOfSets<TKey, TValue>
    {
		private IDictionary<TKey, HashSet<TValue>> theMap;
		
		/// <param name="m">the backing store for this object
		/// </param>
        public MapOfSets(IDictionary<TKey, HashSet<TValue>> m)
		{
			theMap = m;
		}

	    /// <value> direct access to the map backing this object. </value>
	    public virtual IDictionary<TKey, HashSet<TValue>> Map
	    {
	        get { return theMap; }
	    }

	    /// <summary> Adds val to the Set associated with key in the Map.  If key is not 
		/// already in the map, a new Set will first be created.
		/// </summary>
		/// <returns> the size of the Set associated with key once val is added to it.
		/// </returns>
		public virtual int Put(TKey key, TValue val)
		{
            HashSet<TValue> theSet;
            if (!theMap.TryGetValue(key, out theSet))
            {
                theSet = new HashSet<TValue>();
                theMap[key] = theSet;
            }
            theSet.Add(val);
			return theSet.Count;
		}
		/// <summary> Adds multiple vals to the Set associated with key in the Map.  
		/// If key is not 
		/// already in the map, a new Set will first be created.
		/// </summary>
		/// <returns> the size of the Set associated with key once val is added to it.
		/// </returns>
		public virtual int PutAll(TKey key, IEnumerable<TValue> vals)
		{
            HashSet<TValue> theSet;
            if (!theMap.TryGetValue(key, out theSet))
            {
                theSet = new HashSet<TValue>();
                theMap[key] = theSet;
            }
		    theSet.UnionWith(vals);
			return theSet.Count;
		}
	}
}