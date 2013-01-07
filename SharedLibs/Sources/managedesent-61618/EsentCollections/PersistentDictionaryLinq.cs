// --------------------------------------------------------------------------------------------------------------------
// <copyright file="PersistentDictionaryLinq.cs" company="Microsoft Corporation">
//   Copyright (c) Microsoft Corporation.
// </copyright>
// <summary>
//   PersistentDictionary methods that deal with Linq methods.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Collections.Generic
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Linq.Expressions;

    /// <content>
    /// Represents a collection of persistent keys and values.
    /// These are the methods that optimize LINQ extension methods
    /// on a dictionary.
    /// </content>
    public partial class PersistentDictionary<TKey, TValue>
    {
        /// <summary>
        /// Optimize a where statement which uses this dictionary.
        /// </summary>
        /// <param name="expression">
        /// The predicate determining which items should be enumerated.
        /// </param>
        /// <returns>
        /// An enumerator matching only the records matched by the predicate.
        /// </returns>
        public PersistentDictionaryLinqKeyValueEnumerable<TKey, TValue> Where(
            Expression<Predicate<KeyValuePair<TKey, TValue>>> expression)
        {
            if (null == expression)
            {
                throw new ArgumentNullException("expression");
            }

            Predicate<KeyValuePair<TKey, TValue>> predicate =
                KeyValueExpressionEvaluator<TKey, TValue>.KeyRangeIsExact(expression) ? x => true : expression.Compile();
            return new PersistentDictionaryLinqKeyValueEnumerable<TKey, TValue>(
                this,
                expression,
                predicate,
                false);
        }

        /// <summary>
        /// Inverts the order of the elements in the dictionary.
        /// </summary>
        /// <returns>
        /// A sequence whose elements correspond to those of the dictionary in reverse order.
        /// </returns>
        public PersistentDictionaryLinqKeyValueEnumerable<TKey, TValue> Reverse()
        {
            return this.Where(x => true).Reverse();
        }

        /// <summary>
        /// Determine whether any element of the dictionary satisfies a condition.
        /// </summary>
        /// <param name="expression">
        /// A function to test each element for a condition.
        /// </param>
        /// <returns>
        /// True if any elements match the predicate, false otherwise.
        /// </returns>
        public bool Any(Expression<Predicate<KeyValuePair<TKey, TValue>>> expression)
        {
            return this.Where(expression).Any();
        }

        /// <summary>
        /// Returns the first element in the dictionary that satisfies a specified condition.
        /// </summary>
        /// <param name="expression">
        /// A function to test each element for a condition.
        /// </param>
        /// <returns>
        /// The first element in the dictionary that satisfies a specified condition.
        /// </returns>
        public KeyValuePair<TKey, TValue> First(Expression<Predicate<KeyValuePair<TKey, TValue>>> expression)
        {
            return this.Where(expression).First();
        }

        /// <summary>
        /// Returns the first element in the dictionary that satisfies a specified condition or a default
        /// value if no element exists.
        /// </summary>
        /// <param name="expression">
        /// A function to test each element for a condition.
        /// </param>
        /// <returns>
        /// The first element in the dictionary that satisfies a specified condition or a default value.
        /// </returns>
        public KeyValuePair<TKey, TValue> FirstOrDefault(Expression<Predicate<KeyValuePair<TKey, TValue>>> expression)
        {
            return this.Where(expression).FirstOrDefault();
        }

        /// <summary>
        /// Returns the last element in the dictionary that satisfies a specified condition.
        /// </summary>
        /// <param name="expression">
        /// A function to test each element for a condition.
        /// </param>
        /// <returns>
        /// The last element in the dictionary that satisfies a specified condition.
        /// </returns>
        public KeyValuePair<TKey, TValue> Last(Expression<Predicate<KeyValuePair<TKey, TValue>>> expression)
        {
            return this.Where(expression).Last();
        }

        /// <summary>
        /// Returns the last element in the dictionary that satisfies a specified condition or a default
        /// value if no element exists.
        /// </summary>
        /// <param name="expression">
        /// A function to test each element for a condition.
        /// </param>
        /// <returns>
        /// The last element in the dictionary that satisfies a specified condition or a default value.
        /// </returns>
        public KeyValuePair<TKey, TValue> LastOrDefault(Expression<Predicate<KeyValuePair<TKey, TValue>>> expression)
        {
            return this.Where(expression).LastOrDefault();
        }

        /// <summary>
        /// Returns the only element in the dictionary that satisfies a specified condition and throws
        /// an exception if there is more than one element.
        /// </summary>
        /// <param name="expression">
        /// A function to test each element for a condition.
        /// </param>
        /// <returns>
        /// The last element in the dictionary that satisfies a specified condition.
        /// </returns>
        public KeyValuePair<TKey, TValue> Single(Expression<Predicate<KeyValuePair<TKey, TValue>>> expression)
        {
            return this.Where(expression).Single();
        }

        /// <summary>
        /// Returns the only element of the dictionary that satisfies a specified condition or a default
        /// value if no such element exists; this method throws an exception if more than one element
        /// satisfies the condition.
        /// </summary>
        /// <param name="expression">
        /// A function to test each element for a condition.
        /// </param>
        /// <returns>
        /// The last element in the dictionary that satisfies a specified condition or a default value.
        /// </returns>
        public KeyValuePair<TKey, TValue> SingleOrDefault(Expression<Predicate<KeyValuePair<TKey, TValue>>> expression)
        {
            return this.Where(expression).SingleOrDefault();
        }

        /// <summary>
        /// Returns the last element of the dictionary.
        /// </summary>
        /// <returns>The last element.</returns>
        /// <exception cref="InvalidOperationException">
        /// Thrown if the dictionary is empty.
        /// </exception>
        public KeyValuePair<TKey, TValue> Last()
        {
            return this.Reverse().First();
        }

        /// <summary>
        /// Returns the last element of the dictionary or a default value.
        /// </summary>
        /// <returns>The last element.</returns>
        public KeyValuePair<TKey, TValue> LastOrDefault()
        {
            return this.Reverse().FirstOrDefault();
        }
    }
}