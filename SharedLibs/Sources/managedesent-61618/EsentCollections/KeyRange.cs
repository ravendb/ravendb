// --------------------------------------------------------------------------------------------------------------------
// <copyright file="KeyRange.cs" company="Microsoft Corporation">
//   Copyright (c) Microsoft Corporation.
// </copyright>
// <summary>
//   Code that represents a range of keys, where each end can be inclusive or
//   exclusive.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Collections.Generic
{
    using System;
    using System.Diagnostics;
    using System.Text;

    /// <summary>
    /// Represents a range of keys, where each end can be inclusive or
    /// exclusive.
    /// </summary>
    /// <typeparam name="T">The type of the key.</typeparam>
    internal sealed class KeyRange<T> : IEquatable<KeyRange<T>> where T : IComparable<T>
    {
        /// <summary>
        /// A singleton instance of the open range (a range with no limits).
        /// </summary>
        private static readonly KeyRange<T> openRange = new KeyRange<T>(null, null);

        /// <summary>
        /// A singleton instance of the empty range (a range with no records).
        /// </summary>
        private static readonly KeyRange<T> emptyRange = new KeyRange<T>(
            Key<T>.CreateKey(default(T), false), Key<T>.CreateKey(default(T), false));

        /// <summary>
        /// Initializes a new instance of the KeyRange class.
        /// </summary>
        /// <param name="min">The minimum key. This can be null.</param>
        /// <param name="max">The maximum key. This can be null.</param>
        public KeyRange(Key<T> min, Key<T> max)
        {
            this.Min = min;
            this.Max = max;
        }

        /// <summary>
        /// Gets a singleton instance of the open range (a range with no limits).
        /// </summary>
        public static KeyRange<T> OpenRange
        {
            get
            {
                return openRange;
            }
        }

        /// <summary>
        /// Gets a singleton instance of the empty range (a range with no records).
        /// </summary>
        public static KeyRange<T> EmptyRange
        {
            get
            {
                return emptyRange;
            }
        }

        /// <summary>
        /// Gets the minimum key value. This is null if there is
        /// no minumum.
        /// </summary>
        public Key<T> Min { get; private set; }

        /// <summary>
        /// Gets the maximum key value. This is null if there is
        /// no maximum.
        /// </summary>
        public Key<T> Max { get; private set; }

        /// <summary>
        /// Gets a value indicating whether the range is empty.
        /// </summary>
        /// <value>
        /// A value indicating whether the range is empty.
        /// </value>
        public bool IsEmpty
        {
            get
            {
                if (null != this.Min && null != this.Max)
                {
                    if (this.Max.IsPrefix)
                    {
                        // Check for the special case where min is greater than
                        // max, but max is a prefix of min
                        // e.g.: ["ggg", "g*"]
                        Debug.Assert(typeof(string) == typeof(T), "Expected string type");
                        string prefix = this.Max.Value.ToString();
                        string min = this.Min.Value.ToString();
                        if (min.StartsWith(prefix, StringComparison.InvariantCulture))
                        {
                            return false;
                        }
                    }

                    int cmp = CompareKeys(this.Min, this.Max);

                    // If Min and Max are equal then the only way this range
                    // can contain any records is if both min and max are
                    // inclusive.
                    return (cmp > 0) || (0 == cmp && (!this.Max.IsInclusive || !this.Min.IsInclusive));
                }

                return false;
            }
        }

        /// <summary>
        /// Return the intersection of two ranges.
        /// </summary>
        /// <param name="a">The first range.</param>
        /// <param name="b">The second range.</param>
        /// <returns>The intersection of the two ranges.</returns>
        public static KeyRange<T> operator &(KeyRange<T> a, KeyRange<T> b)
        {
            // Intersecting anything with an empty range returns the empty range.
            if (a.IsEmpty || b.IsEmpty)
            {
                return EmptyRange;
            }

            var intersected = new KeyRange<T>(LowerIntersection(a.Min, b.Min), UpperIntersection(a.Max, b.Max));
            if (intersected.IsEmpty)
            {
                intersected = EmptyRange;
            }

            return intersected;
        }

        /// <summary>
        /// Return the union of two ranges.
        /// </summary>
        /// <param name="a">The first range.</param>
        /// <param name="b">The second range.</param>
        /// <returns>The union of the two ranges.</returns>
        public static KeyRange<T> operator |(KeyRange<T> a, KeyRange<T> b)
        {
            // The union of anything with an empty range gives itself
            if (a.IsEmpty)
            {
                return b;
            }

            if (b.IsEmpty)
            {
                return a;
            }

            return new KeyRange<T>(LowerUnion(a.Min, b.Min), UpperUnion(a.Max, b.Max));
        }

        /// <summary>
        /// Invert the key range, if possible. If only one of the lower
        /// or upper bounds is set then swap them and invert the inclusive
        /// setting, otherwise return an open range.
        /// </summary>
        /// <returns>
        /// An inversion of the key range.
        /// </returns>
        public KeyRange<T> Invert()
        {
            if (this.IsEmpty)
            {
                return EmptyRange;
            }

            if (null != this.Min && null == this.Max && !this.Min.IsPrefix)
            {
                Debug.Assert(!this.Min.IsPrefix, "Inverting prefixes not supported");
                return new KeyRange<T>(null, Key<T>.CreateKey(this.Min.Value, !this.Min.IsInclusive));
            }
            else if (null == this.Min && null != this.Max && !this.Max.IsPrefix)
            {
                Debug.Assert(!this.Max.IsPrefix, "Inverting prefixes not supported");
                return new KeyRange<T>(Key<T>.CreateKey(this.Max.Value, !this.Max.IsInclusive), null);
            }

            // Can't invert. Return an open range.
            return OpenRange;
        }

        /// <summary>
        /// Compare an object to this one, to see if they are equal.
        /// </summary>
        /// <param name="obj">The object to compare against.</param>
        /// <returns>True if this range equals the other object.</returns>
        public override bool Equals(object obj)
        {
            if (null == obj || this.GetType() != obj.GetType())
            {
                return false;
            }

            return this.Equals((KeyRange<T>)obj);
        }

        /// <summary>
        /// Gets a hash code for this object.
        /// </summary>
        /// <returns>
        /// A hash code for this object.
        /// </returns>
        public override int GetHashCode()
        {
            // All empty ranges are identical
            if (this.IsEmpty)
            {
                return int.MinValue;
            }

            // Make sure that entries with their keys reversed
            // don't get the same hash code.
            int hash1 = null == this.Min ? 0 : this.Min.GetHashCode();
            int hash2 = null == this.Max ? 0 : this.Max.GetHashCode();
            int hash = hash1 ^ hash2 ^ unchecked((hash2 - hash1) * 10);
            return hash;
        }

        /// <summary>
        /// Compare two key ranges to see if they are equal.
        /// </summary>
        /// <param name="other">The key range to compare against.</param>
        /// <returns>True if they are equal, false otherwise.</returns>
        public bool Equals(KeyRange<T> other)
        {
            if (null == other)
            {
                return false;
            }

            // All empty ranges are equal
            if (this.IsEmpty && other.IsEmpty)
            {
                return true;
            }

            bool minIsEqual = (null == this.Min && null == other.Min)
                              || (null != this.Min && this.Min.Equals(other.Min));
            bool maxIsEqual = (null == this.Max && null == other.Max)
                              || (null != this.Max && this.Max.Equals(other.Max));
            return minIsEqual && maxIsEqual;
        }

        /// <summary>
        /// Generate a string representation of the range.
        /// </summary>
        /// <returns>A string representation of the range.</returns>
        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.Append("KeyRange: ");
            if (null != this.Min)
            {
                sb.AppendFormat("min = {0}, ", this.Min);
            }
            else
            {
                sb.Append("min = null, ");
            }

            if (null != this.Max)
            {
                sb.AppendFormat("max = {0}, ", this.Max);
            }
            else
            {
                sb.Append("max = null");
            }

            if (this.IsEmpty)
            {
                sb.Append("(empty)");
            }

            return sb.ToString();
        }

        /// <summary>
        /// Returns the lower bound of two keys for range intersection.
        /// This is the maximum of the keys, where null represents the
        /// minimum value and exclusive ranges are preferred.
        /// </summary>
        /// <param name="a">The first key.</param>
        /// <param name="b">The second key.</param>
        /// <returns>The upper bound of the two keys.</returns>
        private static Key<T> LowerIntersection(Key<T> a, Key<T> b)
        {
            Key<T> max;
            if (null == a && null != b)
            {
                max = b;
            }
            else if (null != a && null == b)
            {
                max = a;
            }
            else if (null != a && null != b)
            {
                int compare = CompareKeys(a, b);
                if (0 == compare)
                {
                    // Prefer the exclusive range
                    max = a.IsInclusive ? b : a;
                }
                else
                {
                    max = compare > 0 ? a : b;
                }
            }
            else
            {
                max = null;
            }

            return max;
        }

        /// <summary>
        /// Returns the upper bound of two keys for range union.
        /// This is the minimum of the keys, where null represents the
        /// maximum value and exclusive ranges are preferred.
        /// </summary>
        /// <param name="a">The first key.</param>
        /// <param name="b">The second key.</param>
        /// <returns>The lower bound of the two keys.</returns>
        private static Key<T> UpperIntersection(Key<T> a, Key<T> b)
        {
            Key<T> min;
            if (null == a && null != b)
            {
                min = b;
            }
            else if (null != a && null == b)
            {
                min = a;
            }
            else if (null != a && null != b)
            {
                int compare = CompareKeys(a, b);
                if (0 == compare)
                {
                    // Prefer the non-prefix/exclusive range
                    if (a.IsPrefix || b.IsPrefix)
                    {
                        min = a.IsPrefix ? b : a;
                    }
                    else
                    {
                        min = !a.IsInclusive ? a : b;
                    }
                }
                else
                {
                    if (a.IsPrefix && b.IsPrefix)
                    {
                        min = ComparePrefixes(a, b) < 0 ? a : b;
                    }
                    else if (a.IsPrefix)
                    {
                        min = CompareNonPrefixAndPrefix(b, a) < 0 ? b : a;
                    }
                    else if (b.IsPrefix)
                    {
                        min = CompareNonPrefixAndPrefix(a, b) < 0 ? a : b;
                    }
                    else
                    {
                        min = compare < 0 ? a : b;
                    }
                }
            }
            else
            {
                min = null;
            }

            return min;
        }

        /// <summary>
        /// Returns the lower bound of two keys for range union.
        /// This is the minimum of the keys, where null represents the
        /// minimum value and inclusive ranges are preferred.
        /// </summary>
        /// <param name="a">The first key.</param>
        /// <param name="b">The second key.</param>
        /// <returns>The upper bound of the two keys.</returns>
        private static Key<T> LowerUnion(Key<T> a, Key<T> b)
        {
            Key<T> min;
            if (null == a || null == b)
            {
                min = null;
            }
            else
            {
                int compare = CompareKeys(a, b);
                if (0 == compare)
                {
                    // Prefer the inclusive range
                    min = a.IsInclusive ? a : b;
                }
                else
                {
                    min = compare > 0 ? b : a;
                }
            }

            return min;
        }

        /// <summary>
        /// Returns the upper bound of two keys for range union.
        /// This is the maximum of the keys, where null represents the
        /// maximum value. When two keys are equal the order of preference is:
        ///  1. prefix
        ///  2. inclusive
        ///  3. exclusive.
        /// </summary>
        /// <param name="a">The first key.</param>
        /// <param name="b">The second key.</param>
        /// <returns>The lower bound of the two keys.</returns>
        private static Key<T> UpperUnion(Key<T> a, Key<T> b)
        {
            Key<T> max;
            if (null == a || null == b)
            {
                max = null;
            }
            else
            {
                int compare = CompareKeys(a, b);
                if (0 == compare)
                {
                    // Prefer the prefix/inclusive range
                    if (a.IsPrefix || b.IsPrefix)
                    {
                        max = a.IsPrefix ? a : b;
                    }
                    else
                    {
                        max = a.IsInclusive ? a : b;
                    }
                }
                else
                {
                    if (a.IsPrefix && b.IsPrefix)
                    {
                        max = ComparePrefixes(a, b) > 0 ? a : b;
                    }
                    else if (a.IsPrefix)
                    {
                        max = CompareNonPrefixAndPrefix(b, a) > 0 ? b : a;
                    }
                    else if (b.IsPrefix)
                    {
                        max = CompareNonPrefixAndPrefix(a, b) > 0 ? a : b;
                    }
                    else
                    {
                        max = compare > 0 ? a : b;
                    }
                }
            }

            return max;
        }

        /// <summary>
        /// Compare a prefix and non-prefix key.
        /// </summary>
        /// <param name="nonPrefix">The non prefix key.</param>
        /// <param name="prefix">The prefix key.</param>
        /// <returns>If the first key is less than the second then -1. 0 or 1 otherwise.</returns>
        private static int CompareNonPrefixAndPrefix(Key<T> nonPrefix, Key<T> prefix)
        {
            Debug.Assert(!nonPrefix.IsPrefix, "Expected a non-prefix key");
            Debug.Assert(prefix.IsPrefix, "Expected a prefix key");
            Debug.Assert(typeof(string) == typeof(T), "Expected a string");

            string nonPrefixValue = nonPrefix.Value.ToString();
            string prefixValue = prefix.Value.ToString();

            // The two cases we want are:
            //   nonPrefix = "LE", prefix = "LEAD"
            //   nonPrefix = "LEAD", prefix = "LE"
            // In both cases the non-prefix is more restrictive
            if (String.Compare(nonPrefixValue, prefixValue, StringComparison.InvariantCulture) <= 0
                || nonPrefixValue.StartsWith(prefixValue, StringComparison.InvariantCulture))
            {
                return -1;
            }

            return 1;
        }

        /// <summary>
        /// Compare two prefix keys.
        /// </summary>
        /// <param name="a">The first prefix key.</param>
        /// <param name="b">The second prefix key.</param>
        /// <returns>If the first key is less than the second then -1. 0 or 1 otherwise.</returns>
        private static int ComparePrefixes(Key<T> a, Key<T> b)
        {
            Debug.Assert(a.IsPrefix, "Expected a prefix key");
            Debug.Assert(b.IsPrefix, "Expected a prefix key");
            Debug.Assert(typeof(string) == typeof(T), "Expected a string");

            string valueA = a.Value.ToString();
            string valueB = b.Value.ToString();

            if (valueA.Length == valueB.Length)
            {
                // Same length: return the lower string
                // e.g.: a = "AB", b = "CD"
                return String.Compare(valueA, valueB, StringComparison.InvariantCulture);
            }

            if (valueA.StartsWith(valueB, StringComparison.InvariantCulture))
            {
                // b is a substring of a. a is more restrictive
                // e.g.: b = "A", a = "AA"
                return -1;
            }

            if (valueB.StartsWith(valueA, StringComparison.InvariantCulture))
            {
                // a is a substring of b. b is more restrictive
                // e.g.: a = "CA", a = "CAT"
                return 1;
            }

            // Take the lower string
            // e.g.: a = "ABC", b = "XYZ"
            return String.Compare(valueA, valueB, StringComparison.InvariantCulture);
        }

        /// <summary>
        /// Compare two keys. The keys cannot be null but their values can be.
        /// Null is treated as less than any other value.
        /// </summary>
        /// <param name="a">The first key.</param>
        /// <param name="b">The second key.</param>
        /// <returns>
        /// The result of the comparison.
        /// </returns>
        /// <remarks>
        /// Null keys have different semantics depending on whether we are
        /// comparing min or max keys. Null values can be treated uniformly
        /// though.
        /// </remarks>
        private static int CompareKeys(Key<T> a, Key<T> b)
        {
            if (Object.Equals(a.Value, b.Value))
            {
                return 0;
            }

            return a.Value.CompareTo(b.Value);
        }
    }
}