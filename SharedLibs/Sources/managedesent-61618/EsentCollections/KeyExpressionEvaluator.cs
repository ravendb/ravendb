// --------------------------------------------------------------------------------------------------------------------
// <copyright file="KeyExpressionEvaluator.cs" company="Microsoft Corporation">
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
    using System.Linq.Expressions;

    /// <summary>
    /// Contains methods to evaluate a predicate Expression which operates
    /// on Key types to determine a key range which contains all items
    /// matched by the predicate.
    /// </summary>
    /// <typeparam name="TKey">The key type.</typeparam>
    internal static class KeyExpressionEvaluator<TKey> where TKey : IComparable<TKey>
    {
        /// <summary>
        /// Evaluate a predicate Expression and determine a key range which
        /// contains all items matched by the predicate.
        /// </summary>
        /// <param name="expression">The expression to evaluate.</param>
        /// <returns>
        /// A KeyRange that contains all items matched by the predicate. If no
        /// range can be determined the range will include all items.
        /// </returns>
        public static KeyRange<TKey> GetKeyRange(Expression<Predicate<TKey>> expression)
        {
            if (null == expression)
            {
                throw new ArgumentNullException("expression");
            }

            return PredicateExpressionEvaluator<TKey>.GetKeyRange(expression.Body, null);
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
        public static bool KeyRangeIsExact(Expression<Predicate<TKey>> expression)
        {
            if (null == expression)
            {
                throw new ArgumentNullException("expression");
            }

            return PredicateExpressionEvaluator<TKey>.KeyRangeIsExact(expression.Body, null);
        }
    }
}