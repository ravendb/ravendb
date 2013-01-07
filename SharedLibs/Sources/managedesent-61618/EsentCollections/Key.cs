// --------------------------------------------------------------------------------------------------------------------
// <copyright file="Key.cs" company="Microsoft Corporation">
//   Copyright (c) Microsoft Corporation.
// </copyright>
// <summary>
//   Code to represent a generic key value.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Collections.Generic
{
    using System;
    using System.Diagnostics;
    using System.Globalization;

    /// <summary>
    /// Represents a generic key value.
    /// </summary>
    /// <typeparam name="T">The datatype of the key.</typeparam>
    internal sealed class Key<T> : IEquatable<Key<T>> where T : IComparable<T>
    {
        /// <summary>
        /// Initializes a new instance of the Key class.
        /// </summary>
        /// <param name="value">The value of the key.</param>
        /// <param name="isInclusive">True if this key is inclusive.</param>
        /// <param name="isPrefix">True if this key is a prefix.</param>
        private Key(T value, bool isInclusive, bool isPrefix)
        {
            this.Value = value;
            this.IsInclusive = isInclusive;
            this.IsPrefix = isPrefix;
            Debug.Assert(!this.IsPrefix || this.IsInclusive, "Cannot have exclusive prefix");
        }

        /// <summary>
        /// Gets a value indicating whether the key is inclusive.
        /// </summary>
        public bool IsInclusive { get; private set; }

        /// <summary>
        /// Gets a value indicating whether the key is a prefix.
        /// This only makes sense for string types.
        /// </summary>
        public bool IsPrefix { get; private set; }

        /// <summary>
        /// Gets the value of the key.
        /// </summary>
        public T Value { get; private set; }

        /// <summary>
        /// Create a new Key.
        /// </summary>
        /// <param name="value">The value of the key.</param>
        /// <param name="isInclusive">True if the key is to be inclusive.</param>
        /// <returns>The new key.</returns>
        public static Key<T> CreateKey(T value, bool isInclusive)
        {
            return new Key<T>(value, isInclusive, false);
        }

        /// <summary>
        /// Create a new prefix Key.
        /// </summary>
        /// <param name="value">The value of the key.</param>
        /// <returns>The new key.</returns>
        public static Key<T> CreatePrefixKey(T value)
        {
            return new Key<T>(value, true, true);
        }

        /// <summary>
        /// Gets a string representation of the key.
        /// </summary>
        /// <returns>A string representation of the key.</returns>
        public override string ToString()
        {
            return String.Format(
                CultureInfo.InvariantCulture,
                "{0} ({1})",
                this.Value,
                this.IsPrefix ? "prefix" : (this.IsInclusive ? "inclusive" : "exclusive"));
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

            return this.Equals((Key<T>)obj);
        }

        /// <summary>
        /// Gets a hash code for this object.
        /// </summary>
        /// <returns>
        /// A hash code for this object.
        /// </returns>
        public override int GetHashCode()
        {
            return this.Value.GetHashCode()
                   + (this.IsInclusive ? 1 : 2)
                   + (this.IsPrefix ? 3 : 4);
        }

        /// <summary>
        /// Determine if this Key matches another Key.
        /// </summary>
        /// <param name="other">The Key to compare with.</param>
        /// <returns>True if the keys are equal, false otherwise.</returns>
        public bool Equals(Key<T> other)
        {
            return this.ValueEquals(other)
                   && this.IsInclusive == other.IsInclusive
                   && this.IsPrefix == other.IsPrefix;
        }

        /// <summary>
        /// Compare the value of this Key to another key.
        /// </summary>
        /// <param name="other">The Key to compare with.</param>
        /// <returns>True if the values are equal, false otherwise.</returns>
        private bool ValueEquals(Key<T> other)
        {
            if (null == other)
            {
                return false;
            }

            if (Object.Equals(this.Value, other.Value))
            {
                return true;
            }
            
            return 0 == this.Value.CompareTo(other.Value);
        }
    }
}