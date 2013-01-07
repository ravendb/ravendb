// --------------------------------------------------------------------------------------------------------------------
// <copyright file="PersistentDictionaryLinqKeyEnumerable.cs" company="Microsoft Corporation">
//   Copyright (c) Microsoft Corporation.
// </copyright>
// <summary>
//   PersistentDictionary methods that deal with Linq methods.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Collections.Generic
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Linq;
    using System.Linq.Expressions;

    /// <summary>
    /// An object which can enumerate the specified key range in a PersistentDictionary and apply a filter.
    /// </summary>
    /// <typeparam name="TKey">The type of the key in the dictionary.</typeparam>
    /// <typeparam name="TValue">The type of the value in the dictionary.</typeparam>
    public sealed class PersistentDictionaryLinqKeyEnumerable<TKey, TValue> : IEnumerable<TKey> where TKey : IComparable<TKey>
    {
        /// <summary>
        /// The dictionary being iterated.
        /// </summary>
        private readonly PersistentDictionary<TKey, TValue> dictionary;

        /// <summary>
        /// The expression describing the key range to be iterated.
        /// </summary>
        private readonly Expression<Predicate<TKey>> expression;

        /// <summary>
        /// A predicate to apply to the return values. Only entries that match 
        /// the predicate are returned.
        /// </summary>
        private readonly Predicate<TKey> predicate;

        /// <summary>
        /// A value that controls whether enumerators produced by this enumerable 
        /// should be reversed.
        /// </summary>
        private readonly bool isReversed;

        /// <summary>
        /// Initializes a new instance of the PersistentDictionaryLinqKeyEnumerable class.
        /// </summary>
        /// <param name="dict">The dictionary to enumerate.</param>
        /// <param name="expression">The expression describing the range of keys to return.</param>
        /// <param name="predicate">Predicate to apply to the return values.</param>
        /// <param name="isReversed">
        /// A value that controls whether enumerators produced by this enumerable should be reversed.
        /// </param>
        public PersistentDictionaryLinqKeyEnumerable(
            PersistentDictionary<TKey, TValue> dict,
            Expression<Predicate<TKey>> expression,
            Predicate<TKey> predicate,
            bool isReversed)
        {
            this.dictionary = dict;
            this.expression = expression;
            this.predicate = predicate;
            this.isReversed = isReversed;
        }

        /// <summary>
        /// Returns an enumerator that iterates through the collection.
        /// </summary>
        /// <returns>
        /// A <see cref="T:System.Collections.Generic.IEnumerator`1"/> that can be used to iterate through the collection.
        /// </returns>
        public IEnumerator<TKey> GetEnumerator()
        {
            // Consider: we could get the enumeration of key ranges, sort them and union overlapping ranges.
            // Enumerating the data as several different ranges would be more efficient when the expression
            // specifies an OR and the ranges are highly disjoint.
            KeyRange<TKey> range = KeyExpressionEvaluator<TKey>.GetKeyRange(this.expression);
            this.dictionary.TraceWhere(range, this.isReversed);
            if (this.isReversed)
            {
                return new PersistentDictionaryReverseEnumerator<TKey, TValue, TKey>(
                    this.dictionary, range, c => c.RetrieveCurrentKey(), this.predicate);                
            }

            return new PersistentDictionaryEnumerator<TKey, TValue, TKey>(
                this.dictionary, range, c => c.RetrieveCurrentKey(), this.predicate);
        }

        /// <summary>
        /// Returns an enumerator that iterates through a collection.
        /// </summary>
        /// <returns>
        /// An <see cref="T:System.Collections.IEnumerator"/> object that can be used to iterate through the collection.
        /// </returns>
        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }

        /// <summary>
        /// Inverts the order of the elements in a sequence.
        /// </summary>
        /// <returns>
        /// A sequence whose elements correspond to those of the input sequence in reverse order.
        /// </returns>
        public PersistentDictionaryLinqKeyEnumerable<TKey, TValue> Reverse()
        {
            return new PersistentDictionaryLinqKeyEnumerable<TKey, TValue>(
                this.dictionary, this.expression, this.predicate, !this.isReversed);
        }

        /// <summary>
        /// Returns the last element in a sequence.
        /// </summary>
        /// <returns>
        /// The last element in a sequence.
        /// </returns>
        public TKey Last()
        {
            return this.Reverse().First();
        }

        /// <summary>
        /// Returns the last element in a sequence that satisfies a specified condition or a default
        /// value if no element exists.
        /// </summary>
        /// <returns>
        /// The last element in a sequence that satisfies a specified condition or a default value.
        /// </returns>
        public TKey LastOrDefault()
        {
            return this.Reverse().FirstOrDefault();
        }
    }
}