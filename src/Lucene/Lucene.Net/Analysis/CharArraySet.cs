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
using System.Linq;
using System.Collections.Generic;

namespace Lucene.Net.Analysis
{
    /// <summary> A simple class that stores Strings as char[]'s in a
    /// hash table.  Note that this is not a general purpose
    /// class.  For example, it cannot remove items from the
    /// set, nor does it resize its hash table to be smaller,
    /// etc.  It is designed to be quick to test if a char[]
    /// is in the set without the necessity of converting it
    /// to a String first.
    /// <p/>
    /// <em>Please note:</em> This class implements <see cref="System.Collections.Generic.ISet{T}"/> but
    /// does not behave like it should in all cases. The generic type is
    /// <see cref="System.Collections.Generic.ICollection{T}"/>, because you can add any object to it,
    /// that has a string representation. The add methods will use
    /// <see cref="object.ToString()"/> and store the result using a <see cref="char"/>
    /// buffer. The same behaviour have the <see cref="Contains(object)"/> methods.
    /// The <see cref="GetEnumerator"/> method returns an <see cref="string"/> IEnumerable.
    /// For type safety also {@link #stringIterator()} is provided.
    /// </summary>
    // TODO: java uses wildcards, .net doesn't have this, easiest way is to 
    //       make the entire class generic.  Ultimately, though, since this
    //       works with strings, I can't think of a reason not to just declare
    //       this as an ISet<string>.
    public class CharArraySet : ISet<string>
    {
        bool _ReadOnly = false;
        const int INIT_SIZE = 8;
        char[][] _Entries;
        int _Count;
        bool _IgnoreCase;
        public static CharArraySet EMPTY_SET = UnmodifiableSet(new CharArraySet(0, false));

        private void Init(int startSize, bool ignoreCase)
        {
            this._IgnoreCase = ignoreCase;
            int size = INIT_SIZE;
            while (startSize + (startSize >> 2) > size)
                size <<= 1;
            _Entries = new char[size][];
        }

        /// <summary>Create set with enough capacity to hold startSize
        /// terms 
        /// </summary>
        public CharArraySet(int startSize, bool ignoreCase)
        {
            Init(startSize, ignoreCase);
        }

        public CharArraySet(IEnumerable<string> c, bool ignoreCase)
        {
            Init(c.Count(), ignoreCase);
            AddItems(c);
        }

        /// <summary>Create set from a Collection of char[] or String </summary>
        public CharArraySet(IEnumerable<object> c, bool ignoreCase)
        {
            Init(c.Count(), ignoreCase);
            AddItems(c);
        }

        private void AddItems<T>(IEnumerable<T> items)
        {
            foreach(var item in items)
            {
                Add(item.ToString());
            }
        }

        /// <summary>Create set from entries </summary>
        private CharArraySet(char[][] entries, bool ignoreCase, int count)
        {
            this._Entries = entries;
            this._IgnoreCase = ignoreCase;
            this._Count = count;
        }

        /// <summary>true if the <c>len</c> chars of <c>text</c> starting at <c>off</c>
        /// are in the set 
        /// </summary>
        public virtual bool Contains(char[] text, int off, int len)
        {
            return _Entries[GetSlot(text, off, len)] != null;
        }

        public virtual bool Contains(string text)
        {
            return _Entries[GetSlot(text)] != null;
        }


        private int GetSlot(char[] text, int off, int len)
        {
            int code = GetHashCode(text, off, len);
            int pos = code & (_Entries.Length - 1);
            char[] text2 = _Entries[pos];
            if (text2 != null && !Equals(text, off, len, text2))
            {
                int inc = ((code >> 8) + code) | 1;
                do
                {
                    code += inc;
                    pos = code & (_Entries.Length - 1);
                    text2 = _Entries[pos];
                }
                while (text2 != null && !Equals(text, off, len, text2));
            }
            return pos;
        }

        /// <summary>Returns true if the String is in the set </summary>
        private int GetSlot(string text)
        {
            int code = GetHashCode(text);
            int pos = code & (_Entries.Length - 1);
            char[] text2 = _Entries[pos];
            if (text2 != null && !Equals(text, text2))
            {
                int inc = ((code >> 8) + code) | 1;
                do
                {
                    code += inc;
                    pos = code & (_Entries.Length - 1);
                    text2 = _Entries[pos];
                }
                while (text2 != null && !Equals(text, text2));
            }
            return pos;
        }

        public bool Add(string text)
        {
            if (_ReadOnly) throw new NotSupportedException();
            return Add(text.ToCharArray());
        }

        /// <summary>Add this char[] directly to the set.
        /// If ignoreCase is true for this Set, the text array will be directly modified.
        /// The user should never modify this text array after calling this method.
        /// </summary>
        public bool Add(char[] text)
        {
            if (_ReadOnly) throw new NotSupportedException();

            if (_IgnoreCase)
                for (int i = 0; i < text.Length; i++)
                    text[i] = Char.ToLower(text[i]);
            int slot = GetSlot(text, 0, text.Length);
            if (_Entries[slot] != null)
                return false;
            _Entries[slot] = text;
            _Count++;

            if (_Count + (_Count >> 2) > _Entries.Length)
            {
                Rehash();
            }

            return true;
        }

        private bool Equals(char[] text1, int off, int len, char[] text2)
        {
            if (len != text2.Length)
                return false;
            if (_IgnoreCase)
            {
                for (int i = 0; i < len; i++)
                {
                    if (char.ToLower(text1[off + i]) != text2[i])
                        return false;
                }
            }
            else
            {
                for (int i = 0; i < len; i++)
                {
                    if (text1[off + i] != text2[i])
                        return false;
                }
            }
            return true;
        }

        private bool Equals(string text1, char[] text2)
        {
            int len = text1.Length;
            if (len != text2.Length)
                return false;
            if (_IgnoreCase)
            {
                for (int i = 0; i < len; i++)
                {
                    if (char.ToLower(text1[i]) != text2[i])
                        return false;
                }
            }
            else
            {
                for (int i = 0; i < len; i++)
                {
                    if (text1[i] != text2[i])
                        return false;
                }
            }
            return true;
        }

        private void Rehash()
        {
            int newSize = 2 * _Entries.Length;
            char[][] oldEntries = _Entries;
            _Entries = new char[newSize][];

            for (int i = 0; i < oldEntries.Length; i++)
            {
                char[] text = oldEntries[i];
                if (text != null)
                {
                    // todo: could be faster... no need to compare strings on collision
                    _Entries[GetSlot(text, 0, text.Length)] = text;
                }
            }
        }

        private int GetHashCode(char[] text, int offset, int len)
        {
            int code = 0;
            int stop = offset + len;
            if (_IgnoreCase)
            {
                for (int i = offset; i < stop; i++)
                {
                    code = code * 31 + char.ToLower(text[i]);
                }
            }
            else
            {
                for (int i = offset; i < stop; i++)
                {
                    code = code * 31 + text[i];
                }
            }
            return code;
        }

        private int GetHashCode(string text)
        {
            int code = 0;
            int len = text.Length;
            if (_IgnoreCase)
            {
                for (int i = 0; i < len; i++)
                {
                    code = code * 31 + char.ToLower(text[i]);
                }
            }
            else
            {
                for (int i = 0; i < len; i++)
                {
                    code = code * 31 + text[i];
                }
            }
            return code;
        }

        public int Count
        {
            get { return _Count; }
        }

        public bool IsEmpty
        {
            get { return _Count == 0; }
        }

        public bool Contains(object item)
        {
        	var text = item as char[];
        	return text != null ? Contains(text, 0, text.Length) : Contains(item.ToString());
        }

        public bool Add(object item)
        {
            return Add(item.ToString());
        }

        void ICollection<string>.Add(string item)
        {
            this.Add(item);
        }

        /// <summary>
        /// Returns an unmodifiable <see cref="CharArraySet"/>.  This allows to provide
        /// unmodifiable views of internal sets for "read-only" use
        /// </summary>
        /// <param name="set">A Set for which the unmodifiable set it returns.</param>
        /// <returns>A new unmodifiable <see cref="CharArraySet"/></returns>
        /// <throws>ArgumentNullException of the given set is <c>null</c></throws>
        public static CharArraySet UnmodifiableSet(CharArraySet set)
        {
            if(set == null)
                throw new ArgumentNullException("Given set is null");
            if (set == EMPTY_SET)
                return EMPTY_SET;
            if (set._ReadOnly)
                return set;

            var newSet = new CharArraySet(set._Entries, set._IgnoreCase, set.Count) {IsReadOnly = true};
            return newSet;
        }

        /// <summary>
        /// returns a copy of the given set as a <see cref="CharArraySet"/>.  If the given set
        /// is a <see cref="CharArraySet"/> the ignoreCase property will be preserved.
        /// </summary>
        /// <param name="set">A set to copy</param>
        /// <returns>a copy of the given set as a <see cref="CharArraySet"/>.  If the given set
        /// is a <see cref="CharArraySet"/> the ignoreCase property will be preserved.</returns>
        public static CharArraySet Copy<T>(ISet<T> set)
        {
            if (set == null)
                throw new ArgumentNullException("set", "Given set is null!");
            if (set == EMPTY_SET)
                return EMPTY_SET;
            bool ignoreCase = set is CharArraySet && ((CharArraySet)set)._IgnoreCase;
            var arrSet = new CharArraySet(set.Count, ignoreCase);
            arrSet.AddItems(set);
            return arrSet;
        }

        public void Clear()
        {
            throw new NotSupportedException("Remove not supported!");
        }

        public bool IsReadOnly
        {
            get { return _ReadOnly; }
            private set { _ReadOnly = value; }
        }

        /// <summary>Adds all of the elements in the specified collection to this collection </summary>
        public void UnionWith(IEnumerable<string> other)
        {
            if (_ReadOnly) throw new NotSupportedException();

            foreach (string s in other)
            {
                Add(s.ToCharArray());
            }
        }

        /// <summary>Wrapper that calls UnionWith</summary>
        public void AddAll(IEnumerable<string> coll)
        {
            UnionWith(coll);
        }

        #region Unneeded methods
        public void RemoveAll(ICollection<string> c)
        {
            throw new NotSupportedException();
        }

        public void RetainAll(ICollection<string> c)
        {
            throw new NotSupportedException();
        }

        void ICollection<string>.CopyTo(string[] array, int arrayIndex)
        {
            throw new NotSupportedException();
        }

        void ISet<string>.IntersectWith(IEnumerable<string> other)
        {
            throw new NotSupportedException();
        }

        void ISet<string>.ExceptWith(IEnumerable<string> other)
        {
            throw new NotSupportedException();
        }

        void ISet<string>.SymmetricExceptWith(IEnumerable<string> other)
        {
            throw new NotSupportedException();
        }

        bool ISet<string>.IsSubsetOf(IEnumerable<string> other)
        {
            throw new NotSupportedException();
        }

        bool ISet<string>.IsSupersetOf(IEnumerable<string> other)
        {
            throw new NotSupportedException();
        }

        bool ISet<string>.IsProperSupersetOf(IEnumerable<string> other)
        {
            throw new NotSupportedException();
        }

        bool ISet<string>.IsProperSubsetOf(IEnumerable<string> other)
        {
            throw new NotSupportedException();
        }

        bool ISet<string>.Overlaps(IEnumerable<string> other)
        {
            throw new NotSupportedException();
        }

        bool ISet<string>.SetEquals(IEnumerable<string> other)
        {
            throw new NotSupportedException();
        }

        bool ICollection<string>.Remove(string item)
        {
            throw new NotSupportedException();
        }
        #endregion

        /// <summary>
        /// The IEnumerator&lt;String&gt; for this set.  Strings are constructed on the fly,
        /// so use <c>nextCharArray</c> for more efficient access
        /// </summary>
        public class CharArraySetEnumerator : IEnumerator<string>
        {
        	readonly CharArraySet _Creator;
            int pos = -1;
            char[] cur;

            protected internal CharArraySetEnumerator(CharArraySet creator)
            {
                _Creator = creator;
            }

            public bool MoveNext()
            {
                cur = null;
                pos++;
                while (pos < _Creator._Entries.Length && (cur = _Creator._Entries[pos]) == null)
                    pos++;
                return cur != null;
            }

            /// <summary>do not modify the returned char[] </summary>
            public char[] NextCharArray()
            {
                return cur;
            }

            public string Current
            {
                get { return new string(NextCharArray()); }
            }

            public void Dispose()
            {
            }

            object IEnumerator.Current
            {
                get { return new string(NextCharArray()); }
            }

            public void Reset()
            {
                throw new NotImplementedException();
            }
        }

        public IEnumerator<string> StringEnumerator()
        {
            return new CharArraySetEnumerator(this);
        }

        public IEnumerator<string> GetEnumerator()
        {
            return new CharArraySetEnumerator(this);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }

}