// --------------------------------------------------------------------------------------------------------------------
// <copyright file="KeyValueExpressionEvaluator.cs" company="Microsoft Corporation">
//   Copyright (c) Microsoft Corporation.
// </copyright>
// <summary>
//   Code to evaluate a predicate Expression and determine
//   a key range which contains all items matched by the predicate.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Collections.Generic
{
    using System;
    using System.Collections.Generic;
    using System.Linq.Expressions;
    using System.Reflection;

    /// <summary>
    /// Contains methods to evaluate a predicate Expression which operates
    /// on KeyValuePair types to determine a key range which
    /// contains all items matched by the predicate.
    /// </summary>
    /// <typeparam name="TKey">The key type.</typeparam>
    /// <typeparam name="TValue">The value type.</typeparam>
    internal static class KeyValueExpressionEvaluator<TKey, TValue> where TKey : IComparable<TKey>
    {
        /// <summary>
        /// The MemberInfo for KeyValuePair.Key. This is used to identify the key parameter when
        /// getting the key range.
        /// </summary>
        private static readonly MemberInfo KeyMemberInfo = typeof(KeyValuePair<TKey, TValue>).GetProperty("Key", typeof(TKey));

        /// <summary>
        /// Evaluate a predicate Expression and determine a key range which
        /// contains all items matched by the predicate.
        /// </summary>
        /// <param name="expression">The expression to evaluate.</param>
        /// <returns>
        /// A KeyRange that contains all items matched by the predicate. If no
        /// range can be determined the range will include all items.
        /// </returns>
        public static KeyRange<TKey> GetKeyRange(Expression<Predicate<KeyValuePair<TKey, TValue>>> expression)
        {
            if (null == expression)
            {
                throw new ArgumentNullException("expression");
            }

            return PredicateExpressionEvaluator<TKey>.GetKeyRange(expression.Body, KeyMemberInfo);
        }

        /// <summary>
        /// Evaluate a predicate Expression and determine whether a key range can
        /// be found that completely satisfies the expression. If this method returns
        /// true then the key range returned by <see cref="GetKeyRange"/> will return
        /// only records which match the expression.
        /// </summary>
        /// <param name="expression">The expression to evaluate.</param>
        /// <returns>
        /// True if the key range returned by <see cref="GetKeyRange"/> will perfectly
        /// match all records found by the expression.
        /// </returns>
        public static bool KeyRangeIsExact(Expression<Predicate<KeyValuePair<TKey, TValue>>> expression)
        {
            if (null == expression)
            {
                throw new ArgumentNullException("expression");
            }

            return PredicateExpressionEvaluator<TKey>.KeyRangeIsExact(expression.Body, KeyMemberInfo);
        }
    }
}