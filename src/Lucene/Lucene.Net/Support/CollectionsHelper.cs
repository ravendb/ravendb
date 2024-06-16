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

namespace Lucene.Net.Support
{
    /// <summary>
    /// Support class used to handle Hashtable addition, which does a check 
    /// first to make sure the added item is unique in the hash.
    /// </summary>
    public class CollectionsHelper
    {
        public static void Add(System.Collections.Hashtable hashtable, System.Object item)
        {
            hashtable.Add(item, item);
        }

        public static void AddIfNotContains(System.Collections.Hashtable hashtable, System.Object item)
        {
            // Added lock around check.  Even though the collection should already have 
            // a synchronized wrapper around it, it doesn't prevent this test from having
            // race conditions.  Two threads can (and have in TestIndexReaderReopen) call
            // hashtable.Contains(item) == false at the same time, then both try to add to
            // the hashtable, causing an ArgumentException.  locking on the collection
            // prevents this. -- cc
            lock (hashtable)
            {
                if (hashtable.Contains(item) == false)
                {
                    hashtable.Add(item, item);
                }
            }
        }

        public static void AddIfNotContains(System.Collections.ArrayList hashtable, System.Object item)
        {
            // see AddIfNotContains(Hashtable, object) for information about the lock
            lock (hashtable)
            {
                if (hashtable.Contains(item) == false)
                {
                    hashtable.Add(item);
                }
            }
        }

        public static void AddAll(System.Collections.Hashtable hashtable, System.Collections.ICollection items)
        {
            System.Collections.IEnumerator iter = items.GetEnumerator();
            System.Object item;
            while (iter.MoveNext())
            {
                item = iter.Current;
                hashtable.Add(item, item);
            }
        }

        public static void AddAllIfNotContains(System.Collections.Hashtable hashtable, System.Collections.IList items)
        {
            System.Object item;
            for (int i = 0; i < items.Count; i++)
            {
                item = items[i];
                if (hashtable.Contains(item) == false)
                {
                    hashtable.Add(item, item);
                }
            }
        }

        public static void AddAllIfNotContains(System.Collections.Hashtable hashtable, System.Collections.ICollection items)
        {
            System.Collections.IEnumerator iter = items.GetEnumerator();
            System.Object item;
            while (iter.MoveNext())
            {
                item = iter.Current;
                if (hashtable.Contains(item) == false)
                {
                    hashtable.Add(item, item);
                }
            }
        }

        public static void AddAllIfNotContains(System.Collections.Generic.IDictionary<string, string> hashtable, System.Collections.Generic.ICollection<string> items)
        {
            foreach (string s in items)
            {
                if (hashtable.ContainsKey(s) == false)
                {
                    hashtable.Add(s, s);
                }
            }
        }

        public static void AddAll(System.Collections.Generic.IDictionary<string, string> hashtable, System.Collections.Generic.ICollection<string> items)
        {
            foreach (string s in items)
            {
                hashtable.Add(s, s);
            }
        }

        public static bool Contains(System.Collections.Generic.ICollection<string> col, string item)
        {
            foreach (string s in col) if (s == item) return true;
            return false;
        }

        public static bool Contains(System.Collections.ICollection col, System.Object item)
        {
            System.Collections.IEnumerator iter = col.GetEnumerator();
            while (iter.MoveNext())
            {
                if (iter.Current.Equals(item))
                    return true;
            }
            return false;
        }


        public static System.String CollectionToString(System.Collections.Generic.IDictionary<string, string> c)
        {
            Hashtable t = new Hashtable();
            foreach (string key in c.Keys)
            {
                t.Add(key, c[key]);
            }
            return CollectionToString(t);
        }

        /// <summary>
        /// Converts the specified collection to its string representation.
        /// </summary>
        /// <param name="c">The collection to convert to string.</param>
        /// <returns>A string representation of the specified collection.</returns>
        public static System.String CollectionToString(System.Collections.ICollection c)
        {
            System.Text.StringBuilder s = new System.Text.StringBuilder();

            if (c != null)
            {

                System.Collections.ArrayList l = new System.Collections.ArrayList(c);

                bool isDictionary = (c is System.Collections.BitArray || c is System.Collections.Hashtable || c is System.Collections.IDictionary || c is System.Collections.Specialized.NameValueCollection || (l.Count > 0 && l[0] is System.Collections.DictionaryEntry));
                for (int index = 0; index < l.Count; index++)
                {
                    if (l[index] == null)
                        s.Append("null");
                    else if (!isDictionary)
                        s.Append(l[index]);
                    else
                    {
                        isDictionary = true;
                        if (c is System.Collections.Specialized.NameValueCollection)
                            s.Append(((System.Collections.Specialized.NameValueCollection)c).GetKey(index));
                        else
                            s.Append(((System.Collections.DictionaryEntry)l[index]).Key);
                        s.Append("=");
                        if (c is System.Collections.Specialized.NameValueCollection)
                            s.Append(((System.Collections.Specialized.NameValueCollection)c).GetValues(index)[0]);
                        else
                            s.Append(((System.Collections.DictionaryEntry)l[index]).Value);

                    }
                    if (index < l.Count - 1)
                        s.Append(", ");
                }

                if (isDictionary)
                {
                    if (c is System.Collections.ArrayList)
                        isDictionary = false;
                }
                if (isDictionary)
                {
                    s.Insert(0, "{");
                    s.Append("}");
                }
                else
                {
                    s.Insert(0, "[");
                    s.Append("]");
                }
            }
            else
                s.Insert(0, "null");
            return s.ToString();
        }

        /// <summary>
        /// Compares two string arrays for equality.
        /// </summary>
        /// <param name="l1">First string array list to compare</param>
        /// <param name="l2">Second string array list to compare</param>
        /// <returns>true if the strings are equal in both arrays, false otherwise</returns>
        public static bool CompareStringArrays(System.String[] l1, System.String[] l2)
        {
            if (l1.Length != l2.Length)
                return false;
            for (int i = 0; i < l1.Length; i++)
            {
                if (l1[i] != l2[i])
                    return false;
            }
            return true;
        }

        /// <summary>
        /// Sorts an IList collections
        /// </summary>
        /// <param name="list">The System.Collections.IList instance that will be sorted</param>
        /// <param name="Comparator">The Comparator criteria, null to use natural comparator.</param>
        public static void Sort(System.Collections.IList list, System.Collections.IComparer Comparator)
        {
            if (((System.Collections.ArrayList)list).IsReadOnly)
                throw new System.NotSupportedException();

            if ((Comparator == null) || (Comparator is System.Collections.Comparer))
            {
                try
                {
                    ((System.Collections.ArrayList)list).Sort();
                }
                catch (System.InvalidOperationException e)
                {
                    throw new System.InvalidCastException(e.Message);
                }
            }
            else
            {
                try
                {
                    ((System.Collections.ArrayList)list).Sort(Comparator);
                }
                catch (System.InvalidOperationException e)
                {
                    throw new System.InvalidCastException(e.Message);
                }
            }
        }

        /// <summary>
        /// Fills the array with an specific value from an specific index to an specific index.
        /// </summary>
        /// <param name="array">The array to be filled.</param>
        /// <param name="fromindex">The first index to be filled.</param>
        /// <param name="toindex">The last index to be filled.</param>
        /// <param name="val">The value to fill the array with.</param>
        public static void Fill(System.Array array, System.Int32 fromindex, System.Int32 toindex, System.Object val)
        {
            System.Object Temp_Object = val;
            System.Type elementtype = array.GetType().GetElementType();
            if (elementtype != val.GetType())
                Temp_Object = Convert.ChangeType(val, elementtype);
            if (array.Length == 0)
                throw (new System.NullReferenceException());
            if (fromindex > toindex)
                throw (new System.ArgumentException());
            if ((fromindex < 0) || ((System.Array)array).Length < toindex)
                throw (new System.IndexOutOfRangeException());
            for (int index = (fromindex > 0) ? fromindex-- : fromindex; index < toindex; index++)
                array.SetValue(Temp_Object, index);
        }


        /// <summary>
        /// Fills the array with an specific value.
        /// </summary>
        /// <param name="array">The array to be filled.</param>
        /// <param name="val">The value to fill the array with.</param>
        public static void Fill(System.Array array, System.Object val)
        {
            Fill(array, 0, array.Length, val);
        }

        /// <summary>
        /// Compares the entire members of one array whith the other one.
        /// </summary>
        /// <param name="array1">The array to be compared.</param>
        /// <param name="array2">The array to be compared with.</param>
        /// <returns>Returns true if the two specified arrays of Objects are equal 
        /// to one another. The two arrays are considered equal if both arrays 
        /// contain the same number of elements, and all corresponding pairs of 
        /// elements in the two arrays are equal. Two objects e1 and e2 are 
        /// considered equal if (e1==null ? e2==null : e1.equals(e2)). In other 
        /// words, the two arrays are equal if they contain the same elements in 
        /// the same order. Also, two array references are considered equal if 
        /// both are null.</returns>
        public static bool Equals(System.Array array1, System.Array array2)
        {
            bool result = false;
            if ((array1 == null) && (array2 == null))
                result = true;
            else if ((array1 != null) && (array2 != null))
            {
                if (array1.Length == array2.Length)
                {
                    int length = array1.Length;
                    result = true;
                    for (int index = 0; index < length; index++)
                    {
                        System.Object o1 = array1.GetValue(index);
                        System.Object o2 = array2.GetValue(index);
                        if (o1 == null && o2 == null)
                            continue;   // they match
                        else if (o1 == null || !o1.Equals(o2))
                        {
                            result = false;
                            break;
                        }
                    }
                }
            }
            return result;
        }
    }
}
