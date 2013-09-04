// --------------------------------------------------------------------------------------------------------------------
// <copyright file="PersistentDictionaryLinqExtensions.cs" company="Microsoft Corporation">
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

    /// <summary>
    /// Extension methods for LINQ queries on a dictionary.
    /// </summary>
    public static class PersistentDictionaryLinqExtensions
    {
        /// <summary>
        /// Returns a number that represents how many elements in the key collection satisfy a condition.
        /// </summary>
        /// <typeparam name="TKey">The type of the dictionary's keys.</typeparam>
        /// <typeparam name="TValue">The type of the dictionary's values.</typeparam>
        /// <param name="source">The dictionary key collection.</param>
        /// <param name="expression">A function to test each element for a condition.</param>
        /// <returns>The number of elements that satisfy the condition.</returns>
        /// <remarks>
        /// This method cannot be defined on the PersistentDictionaryKeyCollection because it conflicts
        /// with the Count property.
        /// </remarks>
        public static int Count<TKey, TValue>(
            this PersistentDictionaryKeyCollection<TKey, TValue> source,
            Expression<Predicate<TKey>> expression) where TKey : IComparable<TKey>
        {
            return source.Where(expression).Count();
        }

        /// <summary>
        /// Returns a number that represents how many elements in the dictionary satisfy a condition.
        /// </summary>
        /// <typeparam name="TKey">The type of the dictionary's keys.</typeparam>
        /// <typeparam name="TValue">The type of the dictionary's values.</typeparam>
        /// <param name="source">The dictionary.</param>
        /// <param name="expression">A function to test each element for a condition.</param>
        /// <returns>The number of elements that satisfy the condition.</returns>
        /// <remarks>
        /// This method cannot be defined on the PersistentDictionary because it conflicts
        /// with the Count property.
        /// </remarks>
        public static int Count<TKey, TValue>(
            this PersistentDictionary<TKey, TValue> source,
            Expression<Predicate<KeyValuePair<TKey, TValue>>> expression) where TKey : IComparable<TKey>
        {
            return source.Where(expression).Count();
        }
    }
}