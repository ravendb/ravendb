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
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

#if NET35

namespace Lucene.Net.Support.Compatibility
{
    /// <summary>
    /// Support class that emulates the behavior of the ConcurrentDictionary
    /// from .NET 4.0.  This class will, in most cases, perform slightly slower
    /// than the 4.0 equivalent.  Note that all behavior is emulated, which means
    /// that <see cref="GetEnumerator"/>, <see cref="Keys"/>, and <see cref="Values"/>
    /// all return a snapshot of the data at the time it was called.
    /// </summary>
    [Serializable]
    public class ConcurrentDictionary<TKey, TValue> : IDictionary<TKey, TValue>
    {
        private readonly object _lockObj = new object();
        private readonly Dictionary<TKey, TValue> _dictInst; 

        public ConcurrentDictionary()
            : this(16)
        { }

        public ConcurrentDictionary(int capacity)
            : this(capacity, EqualityComparer<TKey>.Default)
        { }

        public ConcurrentDictionary(int capacity, IEqualityComparer<TKey> comparer)
        {
            _dictInst = new Dictionary<TKey, TValue>(capacity, comparer);
        }

        public ConcurrentDictionary(IEnumerable<KeyValuePair<TKey, TValue>> keyValuePairs)
            : this(16)
        {
            foreach(var value in keyValuePairs)
            {
                _dictInst.Add(value.Key, value.Value);
            }
        }

        #region Concurrent Dictionary Special Methods

        public TValue AddOrUpdate(TKey key, Func<TKey, TValue> addValueFactory, Func<TKey, TValue, TValue> updateValueFactory)
        {
            lock(_lockObj)
            {
                if(_dictInst.ContainsKey(key))
                {
                    _dictInst[key] = updateValueFactory(key, _dictInst[key]);
                }
                else
                {
                    _dictInst[key] = addValueFactory(key);
                }

                return _dictInst[key];
            }
        }

        public TValue AddOrUpdate(TKey key, TValue addValue, Func<TKey, TValue, TValue> updateValueFactory)
        {
            lock (_lockObj)
            {
                if (_dictInst.ContainsKey(key))
                {
                    _dictInst[key] = updateValueFactory(key, _dictInst[key]);
                }
                else
                {
                    _dictInst[key] = addValue;
                }

                return _dictInst[key];
            }
        }

        public TValue GetOrAdd(TKey key, Func<TKey, TValue> valueFactory)
        {
            lock (_lockObj)
            {
                if (!_dictInst.ContainsKey(key))
                {
                    _dictInst[key] = valueFactory(key);
                }

                return _dictInst[key];
            }
        }

        public TValue GetOrAdd(TKey key, TValue value)
        {
            lock (_lockObj)
            {
                if (!_dictInst.ContainsKey(key))
                {
                    _dictInst[key] = value;
                }

                return _dictInst[key];
            }
        }

        public bool TryAdd(TKey key, TValue value)
        {
            lock (_lockObj)
            {
                if (_dictInst.ContainsKey(key))
                {
                    return false;
                }

                _dictInst[key] = value;
                return true;
            }
        }

        public bool TryRemove(TKey key, out TValue value)
        {
            lock (_lockObj)
            {
                if (_dictInst.ContainsKey(key))
                {
                    value = _dictInst[key];
                    _dictInst.Remove(key);
                    return true;
                }

                value = default(TValue);
                return false;
            }
        }

        public bool TryUpdate(TKey key, TValue newValue, TValue comparisonValue)
        {
            lock (_lockObj)
            {
                if (_dictInst.ContainsKey(key) && _dictInst[key].Equals(comparisonValue))
                {
                    _dictInst[key] = newValue;
                    return true;
                }

                return false;
            }
        }

        #endregion

        #region IDictionary Methods

        // .NET4 ConcurrentDictionary returns an enumerator that can enumerate even
        // if the collection is modified.  We can't do that, so create a copy (expensive)
        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
        {
            lock (_lockObj)
            {
                return _dictInst.ToList().GetEnumerator();
            }
        }

        public bool TryGetValue(TKey key, out TValue value)
        {
            lock (_lockObj)
            {
                return _dictInst.TryGetValue(key, out value);
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public void Clear()
        {
            lock (_lockObj)
            {
                _dictInst.Clear();
            }
        }
        
        public int Count
        {
            get
            {
                lock (_lockObj)
                {
                    return _dictInst.Count;
                }
            }
        }

        public bool ContainsKey(TKey key)
        {
            lock (_lockObj)
            {
                return _dictInst.ContainsKey(key);
            }
        }

        public TValue this[TKey key]
        {
            get
            {
                lock (_lockObj)
                {
                    return _dictInst[key];
                }
            }
            set
            {
                lock (_lockObj)
                {
                    _dictInst[key] = value;
                }
            }
        }

        public ICollection<TKey> Keys
        {
            get { return _dictInst.Keys.ToArray(); }
        }

        public ICollection<TValue> Values
        {
            get { return _dictInst.Values.ToArray(); }
        }

        #endregion

        #region Explicit Interface Definitions

        bool ICollection<KeyValuePair<TKey, TValue>>.IsReadOnly
        {
            get { return ((ICollection<KeyValuePair<TKey, TValue>>) _dictInst).IsReadOnly; }
        }

        void IDictionary<TKey, TValue>.Add(TKey key, TValue value)
        {
            lock (_lockObj)
            {
                _dictInst.Add(key, value);
            }
        }

        bool ICollection<KeyValuePair<TKey, TValue>>.Contains(KeyValuePair<TKey, TValue> item)
        {
            lock (_lockObj)
            {
                return _dictInst.Contains(item);
            }
        }

        bool IDictionary<TKey, TValue>.Remove(TKey key)
        {
            lock (_lockObj)
            {
                return _dictInst.Remove(key);
            }
        }

        void ICollection<KeyValuePair<TKey, TValue>>.Add(KeyValuePair<TKey, TValue> item)
        {
            lock (_lockObj)
            {
                ((ICollection<KeyValuePair<TKey, TValue>>)_dictInst).Add(item);
            }
        }

        void ICollection<KeyValuePair<TKey, TValue>>.CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex)
        {
            lock (_lockObj)
            {
                ((ICollection<KeyValuePair<TKey, TValue>>)_dictInst).CopyTo(array, arrayIndex);
            }
        }

        bool ICollection<KeyValuePair<TKey, TValue>>.Remove(KeyValuePair<TKey, TValue> item)
        {
            lock (_lockObj)
            {
                return ((ICollection<KeyValuePair<TKey, TValue>>)_dictInst).Remove(item);
            }
        }

        #endregion
    }
}

#endif