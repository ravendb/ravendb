// --------------------------------------------------------------------------------------------------------------------
// <copyright file="PersistentDictionaryKeyCollection.cs" company="Microsoft Corporation">
//   Copyright (c) Microsoft Corporation.
// </copyright>
// <summary>
//   Code that implements a collection of the keys in a PersistentDictionary.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Collections.Generic
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Linq.Expressions;

    /// <summary>
    /// Collection of the keys in a PersistentDictionary.
    /// </summary>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <typeparam name="TValue">The type of the value.</typeparam>
    public sealed class PersistentDictionaryKeyCollection<TKey, TValue> : PersistentDictionaryCollection<TKey, TValue, TKey> 
        where TKey : IComparable<TKey>
    {
        /// <summary>
        /// Initializes a new instance of the PersistentDictionaryKeyCollection class.
        /// </summary>
        /// <param name="dictionary">The dictionary containing the keys.</param>
        public PersistentDictionaryKeyCollection(PersistentDictionary<TKey, TValue> dictionary) :
            base(dictionary)
        {
        }

        /// <summary>
        /// Returns an enumerator that iterates through the collection.
        /// </summary>
        /// <returns>
        /// A <see cref="T:System.Collections.Generic.IEnumerator`1"/> that can be used to iterate through the collection.
        /// </returns>
        public override IEnumerator<TKey> GetEnumerator()
        {
            return new PersistentDictionaryEnumerator<TKey, TValue, TKey>(
                this.Dictionary, KeyRange<TKey>.OpenRange, c => c.RetrieveCurrentKey(), x => true);
        }

        /// <summary>
        /// Determines whether the <see cref="T:System.Collections.Generic.ICollection`1"/> contains a specific value.
        /// </summary>
        /// <returns>
        /// True if <paramref name="item"/> is found in the <see cref="T:System.Collections.Generic.ICollection`1"/>; otherwise, false.
        /// </returns>
        /// <param name="item">The object to locate in the <see cref="T:System.Collections.Generic.ICollection`1"/>.</param>
        public override bool Contains(TKey item)
        {
            return this.Dictionary.ContainsKey(item);
        }

        /// <summary>
        /// Inverts the order of the elements in the collection.
        /// </summary>
        /// <returns>
        /// A sequence whose elements correspond to those of the collection in reverse order.
        /// </returns>
        public PersistentDictionaryLinqKeyEnumerable<TKey, TValue> Reverse()
        {
            return this.Where(x => true).Reverse();
        }

        /// <summary>
        /// Optimize a where statement which uses this collection.
        /// </summary>
        /// <param name="expression">
        /// The predicate determining which items should be enumerated.
        /// </param>
        /// <returns>
        /// An enumerator matching only the records matched by the predicate.
        /// </returns>
        public PersistentDictionaryLinqKeyEnumerable<TKey, TValue> Where(Expression<Predicate<TKey>> expression)
        {
            if (null == expression)
            {
                throw new ArgumentNullException("expression");
            }

            Predicate<TKey> predicate = KeyExpressionEvaluator<TKey>.KeyRangeIsExact(expression) ? x => true : expression.Compile();
            return new PersistentDictionaryLinqKeyEnumerable<TKey, TValue>(
                this.Dictionary,
                expression,
                predicate,
                false);
        }

        /// <summary>
        /// Returns the minimum key.
        /// </summary>
        /// <returns>The minimum key.</returns>
        /// <exception cref="InvalidOperationException">
        /// Thrown if the key collection is empty.
        /// </exception>
        public TKey Min()
        {
            // The keys are sorted so the first element is the minimum
            return this.First();
        }

        /// <summary>
        /// Returns the maximum key.
        /// </summary>
        /// <returns>The maximum key.</returns>
        /// <exception cref="InvalidOperationException">
        /// Thrown if the key collection is empty.
        /// </exception>
        public TKey Max()
        {
            // The keys are sorted so the last element is the maximum
            return this.Last();
        }

        /// <summary>
        /// Determine whether any element of the collection satisfies a condition.
        /// </summary>
        /// <param name="expression">
        /// A function to test each element for a condition.
        /// </param>
        /// <returns>
        /// True if any elements match the predicate, false otherwise.
        /// </returns>
        public bool Any(Expression<Predicate<TKey>> expression)
        {
            return this.Where(expression).Any();
        }

        /// <summary>
        /// Returns the first element in the collection that satisfies a specified condition.
        /// </summary>
        /// <param name="expression">
        /// A function to test each element for a condition.
        /// </param>
        /// <returns>
        /// The first element in the collection that satisfies a specified condition.
        /// </returns>
        public TKey First(Expression<Predicate<TKey>> expression)
        {
            return this.Where(expression).First();
        }

        /// <summary>
        /// Returns the first element in the collection that satisfies a specified condition or a default
        /// value if no element exists.
        /// </summary>
        /// <param name="expression">
        /// A function to test each element for a condition.
        /// </param>
        /// <returns>
        /// The first element in the collection that satisfies a specified condition or a default value.
        /// </returns>
        public TKey FirstOrDefault(Expression<Predicate<TKey>> expression)
        {
            return this.Where(expression).FirstOrDefault();
        }

        /// <summary>
        /// Returns the last element in the collection that satisfies a specified condition.
        /// </summary>
        /// <param name="expression">
        /// A function to test each element for a condition.
        /// </param>
        /// <returns>
        /// The last element in the collection that satisfies a specified condition.
        /// </returns>
        public TKey Last(Expression<Predicate<TKey>> expression)
        {
            return this.Where(expression).Last();
        }

        /// <summary>
        /// Returns the last element in the collection that satisfies a specified condition or a default
        /// value if no element exists.
        /// </summary>
        /// <param name="expression">
        /// A function to test each element for a condition.
        /// </param>
        /// <returns>
        /// The last element in the collection that satisfies a specified condition or a default value.
        /// </returns>
        public TKey LastOrDefault(Expression<Predicate<TKey>> expression)
        {
            return this.Where(expression).LastOrDefault();
        }

        /// <summary>
        /// Returns the last element of the collection.
        /// </summary>
        /// <returns>The last element.</returns>
        /// <exception cref="InvalidOperationException">
        /// Thrown if the collection is empty.
        /// </exception>
        public TKey Last()
        {
            return this.Reverse().First();
        }

        /// <summary>
        /// Returns the last element of the collection or a default value.
        /// </summary>
        /// <returns>The last element.</returns>
        public TKey LastOrDefault()
        {
            return this.Reverse().FirstOrDefault();
        }

        /// <summary>
        /// Returns the only element in the collection that satisfies a specified condition and throws
        /// an exception if there is more than one element.
        /// </summary>
        /// <param name="expression">
        /// A function to test each element for a condition.
        /// </param>
        /// <returns>
        /// The last element in the collection that satisfies a specified condition.
        /// </returns>
        public TKey Single(Expression<Predicate<TKey>> expression)
        {
            return this.Where(expression).Single();
        }

        /// <summary>
        /// Returns the only element of the collection that satisfies a specified condition or a default
        /// value if no such element exists; this method throws an exception if more than one element
        /// satisfies the condition.
        /// </summary>
        /// <param name="expression">
        /// A function to test each element for a condition.
        /// </param>
        /// <returns>
        /// The last element in the collection that satisfies a specified condition or a default value.
        /// </returns>
        public TKey SingleOrDefault(Expression<Predicate<TKey>> expression)
        {
            return this.Where(expression).SingleOrDefault();
        }
    }
}