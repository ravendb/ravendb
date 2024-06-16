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

namespace Lucene.Net.Support
{
    /// <summary>Represents a strongly typed list of objects that can be accessed by index.
    /// Provides methods to search, sort, and manipulate lists. Also provides functionality
    /// to compare lists against each other through an implementations of
    /// <see cref="IEquatable{T}"/>.</summary>
    /// <typeparam name="T">The type of elements in the list.</typeparam>
        [Serializable]
    public class EquatableList<T> : System.Collections.Generic.List<T>,
                                    IEquatable<System.Collections.Generic.IEnumerable<T>>,
                                    ICloneable
    {
        /// <summary>Initializes a new instance of the 
        /// <see cref="EquatableList{T}"/> class that is empty and has the 
        /// default initial capacity.</summary>
        public EquatableList() : base() { }

        /// <summary>Initializes a new instance of the <see cref="EquatableList{T}"/>
        /// class that contains elements copied from the specified collection and has
        /// sufficient capacity to accommodate the number of elements copied.</summary>
        /// <param name="collection">The collection whose elements are copied to the new list.</param>
        public EquatableList(System.Collections.Generic.IEnumerable<T> collection) : base(collection) { }

        /// <summary>Initializes a new instance of the <see cref="EquatableList{T}"/> 
        /// class that is empty and has the specified initial capacity.</summary>
        /// <param name="capacity">The number of elements that the new list can initially store.</param>
        public EquatableList(int capacity) : base(capacity) { }

        /// <summary>Adds a range of objects represented by the <see cref="ICollection"/>
        /// implementation.</summary>
        /// <param name="c">The <see cref="ICollection"/>
        /// implementation to add to this list.</param>
        public void AddRange(ICollection c)
        {
            // If the collection is null, throw an exception.
            if (c == null) throw new ArgumentNullException("c");

            // Pre-compute capacity.
            Capacity = Math.Max(c.Count + Count, Capacity);

            // Cycle through the items and add.
            foreach (T item in c)
            {
                // Add the item.
                Add(item);
            }
        }

        /// <summary>Compares the counts of two <see cref="System.Collections.Generic.IEnumerable{T}"/>
        /// implementations.</summary>
        /// <remarks>This uses a trick in LINQ, sniffing types for implementations
        /// of interfaces that might supply shortcuts when trying to make comparisons.
        /// In this case, that is the <see cref="System.Collections.Generic.ICollection{T}"/> and
        /// <see cref="ICollection"/> interfaces, either of which can provide a count
        /// which can be used in determining the equality of sequences (if they don't have
        /// the same count, then they can't be equal).</remarks>
        /// <param name="x">The <see cref="System.Collections.Generic.IEnumerable{T}"/> from the left hand side of the
        /// comparison to check the count of.</param>
        /// <param name="y">The <see cref="System.Collections.Generic.IEnumerable{T}"/> from the right hand side of the
        /// comparison to check the count of.</param>
        /// <returns>Null if the result is indeterminate.  This occurs when either <paramref name="x"/>
        /// or <paramref name="y"/> doesn't implement <see cref="ICollection"/> or <see cref="System.Collections.Generic.ICollection{T}"/>.
        /// Otherwise, it will get the count from each and return true if they are equal, false otherwise.</returns>
        private static bool? EnumerableCountsEqual(System.Collections.Generic.IEnumerable<T> x, System.Collections.Generic.IEnumerable<T> y)
        {
            // Get the ICollection<T> and ICollection interfaces.
            System.Collections.Generic.ICollection<T> xOfTCollection = x as System.Collections.Generic.ICollection<T>;
            System.Collections.Generic.ICollection<T> yOfTCollection = y as System.Collections.Generic.ICollection<T>;
            ICollection xCollection = x as ICollection;
            ICollection yCollection = y as ICollection;

            // The count in x and y.
            int? xCount = xOfTCollection != null ? xOfTCollection.Count : xCollection != null ? xCollection.Count : (int?)null;
            int? yCount = yOfTCollection != null ? yOfTCollection.Count : yCollection != null ? yCollection.Count : (int?)null;

            // If either are null, return null, the result is indeterminate.
            if (xCount == null || yCount == null)
            {
                // Return null, indeterminate.
                return null;
            }

            // Both counts are non-null, compare.
            return xCount == yCount;
        }

        /// <summary>Compares the contents of a <see cref="System.Collections.Generic.IEnumerable{T}"/>
        /// implementation to another one to determine equality.</summary>
        /// <remarks>Thinking of the <see cref="System.Collections.Generic.IEnumerable{T}"/> implementation as
        /// a string with any number of characters, the algorithm checks
        /// each item in each list.  If any item of the list is not equal (or
        /// one list contains all the elements of another list), then that list
        /// element is compared to the other list element to see which
        /// list is greater.</remarks>
        /// <param name="x">The <see cref="System.Collections.Generic.IEnumerable{T}"/> implementation
        /// that is considered the left hand side.</param>
        /// <param name="y">The <see cref="System.Collections.Generic.IEnumerable{T}"/> implementation
        /// that is considered the right hand side.</param>
        /// <returns>True if the items are equal, false otherwise.</returns>
        private static bool Equals(System.Collections.Generic.IEnumerable<T> x,
                                   System.Collections.Generic.IEnumerable<T> y)
        {
            // If x and y are null, then return true, they are the same.
            if (x == null && y == null)
            {
                // They are the same, return 0.
                return true;
            }

            // If one is null, then return a value based on whether or not
            // one is null or not.
            if (x == null || y == null)
            {
                // Return false, one is null, the other is not.
                return false;
            }

            // Check to see if the counts on the IEnumerable implementations are equal.
            // This is a shortcut, if they are not equal, then the lists are not equal.
            // If the result is indeterminate, then get out.
            bool? enumerableCountsEqual = EnumerableCountsEqual(x, y);

            // If the enumerable counts have been able to be calculated (indicated by
            // a non-null value) and it is false, then no need to iterate through the items.
            if (enumerableCountsEqual != null && !enumerableCountsEqual.Value)
            {
                // The sequences are not equal.
                return false;
            }

            // The counts of the items in the enumerations are equal, or indeterminate
            // so a full iteration needs to be made to compare each item.
            // Get the default comparer for T first.
            System.Collections.Generic.EqualityComparer<T> defaultComparer =
                EqualityComparer<T>.Default;

            // Get the enumerator for y.
            System.Collections.Generic.IEnumerator<T> otherEnumerator = y.GetEnumerator();

            // Call Dispose on IDisposable if there is an implementation on the
            // IEnumerator<T> returned by a call to y.GetEnumerator().
            using (otherEnumerator as IDisposable)
            {
                // Cycle through the items in this list.
                foreach (T item in x)
                {
                    // If there isn't an item to get, then this has more
                    // items than that, they are not equal.
                    if (!otherEnumerator.MoveNext())
                    {
                        // Return false.
                        return false;
                    }

                    // Perform a comparison.  Must check this on the left hand side
                    // and that on the right hand side.
                    bool comparison = defaultComparer.Equals(item, otherEnumerator.Current);

                    // If the value is false, return false.
                    if (!comparison)
                    {
                        // Return the value.
                        return comparison;
                    }
                }

                // If there are no more items, then return true, the sequences
                // are equal.
                if (!otherEnumerator.MoveNext())
                {
                    // The sequences are equal.
                    return true;
                }

                // The other sequence has more items than this one, return
                // false, these are not equal.
                return false;
            }
        }

        #region IEquatable<IEnumerable<T>> Members
        /// <summary>Compares this sequence to another <see cref="System.Collections.Generic.IEnumerable{T}"/>
        /// implementation, returning true if they are equal, false otherwise.</summary>
        /// <param name="other">The other <see cref="System.Collections.Generic.IEnumerable{T}"/> implementation
        /// to compare against.</param>
        /// <returns>True if the sequence in <paramref name="other"/> 
        /// is the same as this one.</returns>
        public bool Equals(System.Collections.Generic.IEnumerable<T> other)
        {
            // Compare to the other sequence.  If 0, then equal.
            return Equals(this, other);
        }
        #endregion

        /// <summary>Compares this object for equality against other.</summary>
        /// <param name="obj">The other object to compare this object against.</param>
        /// <returns>True if this object and <paramref name="obj"/> are equal, false
        /// otherwise.</returns>
        public override bool Equals(object obj)
        {
            // Call the strongly typed version.
            return Equals(obj as System.Collections.Generic.IEnumerable<T>);
        }

        /// <summary>Gets the hash code for the list.</summary>
        /// <returns>The hash code value.</returns>
        public override int GetHashCode()
        {
            // Call the static method, passing this.
            return GetHashCode(this);
        }

#if __MonoCS__
        public static int GetHashCode<T>(System.Collections.Generic.IEnumerable<T> source)
#else
        /// <summary>Gets the hash code for the list.</summary>
        /// <param name="source">The <see cref="System.Collections.Generic.IEnumerable{T}"/>
        /// implementation which will have all the contents hashed.</param>
        /// <returns>The hash code value.</returns>
        public static int GetHashCode(System.Collections.Generic.IEnumerable<T> source)
#endif
        {
            // If source is null, then return 0.
            if (source == null) return 0;

            // Seed the hash code with the hash code of the type.
            // This is done so that you don't have a lot of collisions of empty
            // ComparableList instances when placed in dictionaries
            // and things that rely on hashcodes.
            int hashCode = typeof(T).GetHashCode();

            // Iterate through the items in this implementation.
            foreach (T item in source)
            {
                // Adjust the hash code.
                hashCode = 31 * hashCode + (item == null ? 0 : item.GetHashCode());
            }

            // Return the hash code.
            return hashCode;
        }

        // TODO: When diverging from Java version of Lucene, can uncomment these to adhere to best practices when overriding the Equals method and implementing IEquatable<T>.
        ///// <summary>Overload of the == operator, it compares a
        ///// <see cref="ComparableList{T}"/> to an <see cref="IEnumerable{T}"/>
        ///// implementation.</summary>
        ///// <param name="x">The <see cref="ComparableList{T}"/> to compare
        ///// against <paramref name="y"/>.</param>
        ///// <param name="y">The <see cref="IEnumerable{T}"/> to compare
        ///// against <paramref name="x"/>.</param>
        ///// <returns>True if the instances are equal, false otherwise.</returns>
        //public static bool operator ==(EquatableList<T> x, System.Collections.Generic.IEnumerable<T> y)
        //{
        //    // Call Equals.
        //    return Equals(x, y);
        //}

        ///// <summary>Overload of the == operator, it compares a
        ///// <see cref="ComparableList{T}"/> to an <see cref="IEnumerable{T}"/>
        ///// implementation.</summary>
        ///// <param name="y">The <see cref="ComparableList{T}"/> to compare
        ///// against <paramref name="x"/>.</param>
        ///// <param name="x">The <see cref="IEnumerable{T}"/> to compare
        ///// against <paramref name="y"/>.</param>
        ///// <returns>True if the instances are equal, false otherwise.</returns>
        //public static bool operator ==(System.Collections.Generic.IEnumerable<T> x, EquatableList<T> y)
        //{
        //    // Call equals.
        //    return Equals(x, y);
        //}

        ///// <summary>Overload of the != operator, it compares a
        ///// <see cref="ComparableList{T}"/> to an <see cref="IEnumerable{T}"/>
        ///// implementation.</summary>
        ///// <param name="x">The <see cref="ComparableList{T}"/> to compare
        ///// against <paramref name="y"/>.</param>
        ///// <param name="y">The <see cref="IEnumerable{T}"/> to compare
        ///// against <paramref name="x"/>.</param>
        ///// <returns>True if the instances are not equal, false otherwise.</returns>
        //public static bool operator !=(EquatableList<T> x, System.Collections.Generic.IEnumerable<T> y)
        //{
        //    // Return the negative of the equals operation.
        //    return !(x == y);
        //}

        ///// <summary>Overload of the != operator, it compares a
        ///// <see cref="ComparableList{T}"/> to an <see cref="IEnumerable{T}"/>
        ///// implementation.</summary>
        ///// <param name="y">The <see cref="ComparableList{T}"/> to compare
        ///// against <paramref name="x"/>.</param>
        ///// <param name="x">The <see cref="IEnumerable{T}"/> to compare
        ///// against <paramref name="y"/>.</param>
        ///// <returns>True if the instances are not equal, false otherwise.</returns>
        //public static bool operator !=(System.Collections.Generic.IEnumerable<T> x, EquatableList<T> y)
        //{
        //    // Return the negative of the equals operation.
        //    return !(x == y);
        //}

        #region ICloneable Members

        /// <summary>Clones the <see cref="EquatableList{T}"/>.</summary>
        /// <remarks>This is a shallow clone.</remarks>
        /// <returns>A new shallow clone of this
        /// <see cref="EquatableList{T}"/>.</returns>
        public object Clone()
        {
            // Just create a new one, passing this to the constructor.
            return new EquatableList<T>(this);
        }

        #endregion
    }
}
