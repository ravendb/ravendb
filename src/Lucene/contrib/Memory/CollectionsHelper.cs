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

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Lucene.Net.Index.Memory
{
    internal static class CollectionsHelper<T>
    {
        private static readonly T[] EmptyArray = new T[0];

        /// <summary>
        /// Returns an empty list of type T
        /// </summary>
        public static IList<T> EmptyList()
        {
            return EmptyArray;
        }
    }

    public static class CollectionsExtensions
    {
        public static ICollection<T> AsReadOnly<T>(this ICollection<T> collection)
        {
            return new ReadOnlyCollection<T>(collection);
        }

        private sealed class ReadOnlyCollection<T> : ICollection<T>
        {
            private readonly ICollection<T> _other;

            public ReadOnlyCollection(ICollection<T> other)
            {
                _other = other;
            }

            public IEnumerator<T> GetEnumerator()
            {
                return _other.GetEnumerator();
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }

            public void Add(T item)
            {
                throw new NotSupportedException("Collection is read only!");
            }

            public void Clear()
            {
                throw new NotSupportedException("Collection is read only!");
            }

            public bool Contains(T item)
            {
                return _other.Contains(item);
            }

            public void CopyTo(T[] array, int arrayIndex)
            {
                _other.CopyTo(array, arrayIndex);
            }

            public bool Remove(T item)
            {
                throw new NotSupportedException("Collection is read only!");
            }

            public int Count
            {
                get { return _other.Count; }
            }

            public bool IsReadOnly
            {
                get { return true; }
            }
        }
    }
}
