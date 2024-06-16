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

#if NET35

namespace System.Collections.Generic
{
    [Serializable]
    public class SortedSet<T> : ISet<T>, ICollection
    {
        private readonly SortedList<T, byte> _list;

        public SortedSet()
            : this(Comparer<T>.Default)
        { }

        public SortedSet(IComparer<T> comparer)
        {
            _list = new SortedList<T, byte>(comparer);
        }
        
        public T Min { get { return (_list.Count) >= 1 ? _list.Keys[0] : default(T); } }
        
        public T Max { get { return (_list.Count) >= 1 ? _list.Keys[_list.Count - 1] : default(T); } }


        /// <summary>
        /// Removes all items from the <see cref="T:System.Collections.Generic.ICollection`1"/>.
        /// </summary>
        /// <exception cref="T:System.NotSupportedException">The <see cref="T:System.Collections.Generic.ICollection`1"/> is read-only. 
        ///                 </exception>
        public void Clear()
        {
            _list.Clear();
        }

        public void CopyTo(T[] array, int arrayIndex)
        {
            _list.Keys.CopyTo(array, arrayIndex);
        }

        public bool Remove(T item)
        {
            return _list.Remove(item);
        }

        public bool Contains(T value)
        {
            return _list.ContainsKey(value);
        }

        public bool Add(T item)
        {
            if (!_list.ContainsKey(item))
            {
                _list.Add(item, 0);
                return true;
            }
            return false;
        }

        public void UnionWith(IEnumerable<T> other)
        {
            foreach (var obj in other)
                Add(obj);
        }

        public IEnumerator<T> GetEnumerator()
        {
            return _list.Keys.GetEnumerator();
        }

        public IComparer<T> Comparer { get { return _list.Comparer; } }

        public int Count
        {
            get { return _list.Count; }
        }

        #region Explicit Interface Implementations

        void ICollection<T>.Add(T item)
        {
            Add(item);
        }

        void ICollection.CopyTo(Array array, int index)
        {
            CopyTo((T[]) array, index);
        }

        bool ICollection<T>.IsReadOnly
        {
            get { return false; }
        }

        bool ICollection.IsSynchronized
        {
            get { return false; }
        }

        object ICollection.SyncRoot
        {
            get { throw new NotSupportedException(); }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        int ICollection.Count
        {
            get { return Count; }
        }

        #endregion

        #region ISet<T> Implementation

        void ISet<T>.ExceptWith(IEnumerable<T> other)
        {
            foreach(var obj in other)
            {
                _list.Remove(obj);
            }
        }

        void ISet<T>.IntersectWith(IEnumerable<T> other)
        {
            throw new NotImplementedException();
        }

        bool ISet<T>.IsProperSubsetOf(IEnumerable<T> other)
        {
            throw new NotImplementedException();
        }

        bool ISet<T>.IsProperSupersetOf(IEnumerable<T> other)
        {
            throw new NotImplementedException();
        }

        bool ISet<T>.IsSubsetOf(IEnumerable<T> other)
        {
            throw new NotImplementedException();
        }

        bool ISet<T>.IsSupersetOf(IEnumerable<T> other)
        {
            throw new NotImplementedException();
        }

        bool ISet<T>.Overlaps(IEnumerable<T> other)
        {
            throw new NotImplementedException();
        }

        bool ISet<T>.SetEquals(IEnumerable<T> other)
        {
            throw new NotImplementedException();
        }

        void ISet<T>.SymmetricExceptWith(IEnumerable<T> other)
        {
            throw new NotImplementedException();
        }

        #endregion
    }
}

#endif