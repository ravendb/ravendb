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
    public interface ISet<T> : ICollection<T>
    {
#region METHODS

        new bool Add(T item);

        void ExceptWith(IEnumerable<T> other);

        void IntersectWith(IEnumerable<T> other);

        bool IsProperSubsetOf(IEnumerable<T> other);

        bool IsProperSupersetOf(IEnumerable<T> other);

        bool IsSubsetOf(IEnumerable<T> other);

        bool IsSupersetOf(IEnumerable<T> other);

        bool Overlaps(IEnumerable<T> other);

        bool SetEquals(IEnumerable<T> other);

        void SymmetricExceptWith(IEnumerable<T> other);

        void UnionWith(IEnumerable<T> other);

        #endregion

#region EXTENSION METHODS



        #endregion
    }

}

#endif