/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
*/

using System.Collections.Generic;

namespace Lucene.Net.Search.Vectorhighlight
{
    public class HashMap<K, V> : Dictionary<K, V>
    {
        V _NullKeyValue = default(V);

        public new void Add(K key,V value)
        {
            if (key == null)
                _NullKeyValue = value;
            else
                base.Add(key,value);
        }

        public new int Count
        {
            get
            {
                return base.Count + (_NullKeyValue!= null ? 1 : 0);
            }
        }

        public new V this[K key]
        {
            get{
                return Get(key);
            }
            set{
                Add(key,value);
            }
        }

        public V Get(K key)
        {
            if (key == null) return _NullKeyValue;

            V v = default(V);
            base.TryGetValue(key, out v);
            return v;
        }

        public void Put(K key, V val) 
        {
            Add(key,val);
        }
    }
}

