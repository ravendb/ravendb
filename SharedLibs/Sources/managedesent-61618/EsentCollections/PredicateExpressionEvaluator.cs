// --------------------------------------------------------------------------------------------------------------------
// <copyright file="PredicateExpressionEvaluator.cs" company="Microsoft Corporation">
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
    using System.Diagnostics;
    using System.Linq.Expressions;
    using System.Reflection;

    /// <summary>
    /// Contains methods to evaluate a predicate Expression and determine
    /// a key range which contains all items matched by the predicate.
    /// </summary>
    /// <typeparam name="TKey">The key type.</typeparam>
    internal static class PredicateExpressionEvaluator<TKey> where TKey : IComparable<TKey>
    {
        /// <summary>
        /// A MethodInfo describes TKey.CompareTo(TKey).
        /// </summary>
        private static readonly MethodInfo compareToMethod = typeof(TKey).GetMethod("CompareTo", new[] { typeof(TKey) });

        /// <summary>
        /// A MethodInfo describing TKey.Equals(TKey).
        /// </summary>
        private static readonly MethodInfo equalsMethod = typeof(TKey).GetMethod("Equals", new[] { typeof(TKey) });

        /// <summary>
        /// Evaluate a predicate Expression and determine a key range which
        /// contains all items matched by the predicate.
        /// </summary>
        /// <param name="expression">The expression to evaluate.</param>
        /// <param name="keyMemberInfo">The name of the parameter member that is the key.</param>
        /// <returns>
        /// A KeyRange that contains all items matched by the predicate. If no
        /// range can be determined the range will include all items.
        /// </returns>
        public static KeyRange<TKey> GetKeyRange(Expression expression, MemberInfo keyMemberInfo)
        {
            return GetKeyRangeOfSubtree(expression, keyMemberInfo);
        }

        /// <summary>
        /// Evaluate a predicate Expression and determine whether a key range can
        /// be found that completely satisfies the expression. If this method returns
        /// true then the key range returned by <see cref="GetKeyRange"/> will return
        /// only records which match the expression.
        /// </summary>
        /// <param name="expression">The expression to evaluate.</param>
        /// <param name="keyMemberInfo">The name of the parameter member that is the key.</param>
        /// <returns>
        /// True if the key range returned by <see cref="GetKeyRange"/> will perfectly
        /// match all records found by the expression.
        /// </returns>
        public static bool KeyRangeIsExact(Expression expression, MemberInfo keyMemberInfo)
        {
            // A KeyRange is exact if the expression is an AND of key comparisons
            switch (expression.NodeType)
            {
                case ExpressionType.AndAlso:
                    var andExpression = (BinaryExpression)expression;
                    return KeyRangeIsExact(andExpression.Left, keyMemberInfo) &&
                           KeyRangeIsExact(andExpression.Right, keyMemberInfo);

                case ExpressionType.Call:
                    KeyRange<TKey> ignored;
                    return IsComparisonMethod((MethodCallExpression)expression, keyMemberInfo, out ignored);

                case ExpressionType.Equal:
                case ExpressionType.LessThan:
                case ExpressionType.LessThanOrEqual:
                case ExpressionType.GreaterThan:
                case ExpressionType.GreaterThanOrEqual:
                    TKey value;
                    ExpressionType expressionType;
                    return IsConstantComparison((BinaryExpression)expression, keyMemberInfo, out value, out expressionType);
            }

            return false;
        }

        /// <summary>
        /// Evaluate a predicate Expression and determine key range which
        /// contains all items matched by the predicate.
        /// </summary>
        /// <param name="expression">The expression to evaluate.</param>
        /// <param name="keyMemberInfo">The name of the parameter member that is the key.</param>
        /// <returns>
        /// A KeyRange containing all items matched by the predicate. If no
        /// range can be determined the ranges will include all items.
        /// </returns>
        private static KeyRange<TKey> GetKeyRangeOfSubtree(Expression expression, MemberInfo keyMemberInfo)
        {
            switch (expression.NodeType)
            {
                case ExpressionType.AndAlso:
                {
                    // Intersect the left and right parts
                    var binaryExpression = (BinaryExpression)expression;
                    return GetKeyRangeOfSubtree(binaryExpression.Left, keyMemberInfo)
                           & GetKeyRangeOfSubtree(binaryExpression.Right, keyMemberInfo);
                }

                case ExpressionType.OrElse:
                {
                    // Union the left and right parts
                    var binaryExpression = (BinaryExpression)expression;
                    return GetKeyRangeOfSubtree(binaryExpression.Left, keyMemberInfo)
                           | GetKeyRangeOfSubtree(binaryExpression.Right, keyMemberInfo);
                }

                case ExpressionType.Not:
                {
                    var unaryExpression = (UnaryExpression)expression;
                    return GetNegationOf(unaryExpression.Operand, keyMemberInfo);
                }

                case ExpressionType.Call:
                    {
                        KeyRange<TKey> keyRange;
                        if (IsComparisonMethod((MethodCallExpression)expression, keyMemberInfo, out keyRange))
                        {
                            return keyRange;
                        }

                        break;
                    }

                case ExpressionType.Equal:
                case ExpressionType.LessThan:
                case ExpressionType.LessThanOrEqual:
                case ExpressionType.GreaterThan:
                case ExpressionType.GreaterThanOrEqual:
                {
                    // Return a range
                    var binaryExpression = (BinaryExpression)expression;
                    TKey value;
                    ExpressionType expressionType;
                    if (IsConstantComparison(binaryExpression, keyMemberInfo, out value, out expressionType))
                    {
                        switch (expressionType)
                        {
                            case ExpressionType.Equal:
                                var key = Key<TKey>.CreateKey(value, true);
                                return new KeyRange<TKey>(key, key);
                            case ExpressionType.LessThan:
                                return new KeyRange<TKey>(null, Key<TKey>.CreateKey(value, false));
                            case ExpressionType.LessThanOrEqual:
                                return new KeyRange<TKey>(null, Key<TKey>.CreateKey(value, true));
                            case ExpressionType.GreaterThan:
                                return new KeyRange<TKey>(Key<TKey>.CreateKey(value, false), null);
                            case ExpressionType.GreaterThanOrEqual:
                                return new KeyRange<TKey>(Key<TKey>.CreateKey(value, true), null);
                            default:
                                throw new InvalidOperationException(expressionType.ToString());
                        }
                    }

                    break;
                }

                default:
                    break;
            }

            return KeyRange<TKey>.OpenRange;
        }

        /// <summary>
        /// Determine if the MethodCallExpression is a key comparison method, and
        /// return the index range if it is.
        /// </summary>
        /// <param name="methodCall">The method call expression.</param>
        /// <param name="keyMemberInfo">The name of the parameter member that is the key.</param>
        /// <param name="keyRange">Returns the key range if this is a key comparison method.</param>
        /// <returns>True if the method is a key comparison method.</returns>
        private static bool IsComparisonMethod(MethodCallExpression methodCall, MemberInfo keyMemberInfo, out KeyRange<TKey> keyRange)
        {
            if (null != methodCall.Object && IsKeyAccess(methodCall.Object, keyMemberInfo))
            {
                TKey value;

                // TKey.Equals
                if ((equalsMethod == methodCall.Method)
                    && ConstantExpressionEvaluator<TKey>.TryGetConstantExpression(methodCall.Arguments[0], out value))
                {
                    keyRange = new KeyRange<TKey>(Key<TKey>.CreateKey(value, true), Key<TKey>.CreateKey(value, true));
                    return true;
                }
            }

            if (typeof(TKey) == typeof(string))
            {
                if (null != methodCall.Object
                    && IsKeyAccess(methodCall.Object, keyMemberInfo))
                {
                    TKey value;

                    // String.StartsWith
                    if (StringExpressionEvaluatorHelper.StringStartWithMethod == methodCall.Method
                        && ConstantExpressionEvaluator<TKey>.TryGetConstantExpression(methodCall.Arguments[0], out value))
                    {
                        // Lower range is just the string, upper range is the prefix
                        keyRange = new KeyRange<TKey>(Key<TKey>.CreateKey(value, true), Key<TKey>.CreatePrefixKey(value));
                        return true;
                    }
                }
                else if (null == methodCall.Object)
                {
                    // Static String.Equals
                    if (StringExpressionEvaluatorHelper.StringStaticEqualsMethod == methodCall.Method)
                    {
                        TKey value;
                        if ((IsKeyAccess(methodCall.Arguments[0], keyMemberInfo)
                            && ConstantExpressionEvaluator<TKey>.TryGetConstantExpression(methodCall.Arguments[1], out value))
                            || (IsKeyAccess(methodCall.Arguments[1], keyMemberInfo)
                            && ConstantExpressionEvaluator<TKey>.TryGetConstantExpression(methodCall.Arguments[0], out value)))
                        {
                            keyRange = new KeyRange<TKey>(Key<TKey>.CreateKey(value, true), Key<TKey>.CreateKey(value, true));
                            return true;
                        }
                    }                    
                }
            }

            keyRange = null;
            return false;
        }

        /// <summary>
        /// Get the negation of the given expression.
        /// </summary>
        /// <param name="expression">The expression.</param>
        /// <param name="keyMemberInfo">The name of the parameter member that is the key.</param>
        /// <returns>The negation of the given range.</returns>
        private static KeyRange<TKey> GetNegationOf(Expression expression, MemberInfo keyMemberInfo)
        {
            switch (expression.NodeType)
            {
                case ExpressionType.Not:
                {
                    // Negation of a not simply means evaluating the condition
                    UnaryExpression unaryExpression = (UnaryExpression)expression;
                    return GetKeyRangeOfSubtree(unaryExpression.Operand, keyMemberInfo);
                }

                case ExpressionType.AndAlso:
                {
                    // DeMorgan's Law: !(A && B) -> !A || !B
                    BinaryExpression binaryExpression = (BinaryExpression)expression;
                    return GetNegationOf(binaryExpression.Left, keyMemberInfo) | GetNegationOf(binaryExpression.Right, keyMemberInfo);
                }

                case ExpressionType.OrElse:
                {
                    // DeMorgan's Law: !(A || B) -> !A && !B
                    BinaryExpression binaryExpression = (BinaryExpression)expression;
                    return GetNegationOf(binaryExpression.Left, keyMemberInfo) & GetNegationOf(binaryExpression.Right, keyMemberInfo);
                }

                case ExpressionType.Equal:
                {
                    return KeyRange<TKey>.OpenRange;
                }

                case ExpressionType.NotEqual:
                {
                    BinaryExpression binaryExpression = (BinaryExpression)expression;
                    return GetKeyRangeOfSubtree(
                        Expression.Equal(binaryExpression.Left, binaryExpression.Right), keyMemberInfo);
                }

                case ExpressionType.LessThan:
                case ExpressionType.LessThanOrEqual:
                case ExpressionType.GreaterThan:
                case ExpressionType.GreaterThanOrEqual:
                    return GetKeyRangeOfSubtree(expression, keyMemberInfo).Invert();
                default:
                    break;
            }

            return KeyRange<TKey>.OpenRange;
        }

        /// <summary>
        /// Determine if the current binary expression involves the Key of the parameter
        /// and a constant value.
        /// </summary>
        /// <param name="expression">The expression to evaluate.</param>
        /// <param name="keyMemberInfo">The name of the parameter member that is the key.</param>
        /// <param name="value">Returns the value being compared to the key.</param>
        /// <param name="expressionType">Returns the type of the expression.</param>
        /// <returns>
        /// True if the expression involves the key of the parameter and a constant value.
        /// </returns>
        private static bool IsConstantComparison(BinaryExpression expression, MemberInfo keyMemberInfo, out TKey value, out ExpressionType expressionType)
        {
            // Look for expression of the form x.Key [comparison] value
            //   e.g. x.Key < 0 or x.Key > (3 + 7)
            if (IsSimpleComparisonExpression(expression, keyMemberInfo, out value, out expressionType))
            {
                return true;
            }

            // Look for expressions of the form x.Key.CompareTo(value) [comparison] 0
            //   e.g. x.Key.CompareTo("foo") <= 0 or 0 > x.Key.CompareTo(5.67)
            // TKey implements IComparable<TKey> so we should expect this on all key types.
            if (IsCompareToExpression(expression, keyMemberInfo, out value, out expressionType))
            {
                return true;
            }

            // For string keys look for expressions of the form String.Compare(Key, value) [comparison] 0
            //   e.g. String.Compare(Key, "foo") < 0 or 0 > String.Compare("bar", Key)
            if (typeof(string) == typeof(TKey)
                && IsStringComparisonExpression(expression, keyMemberInfo, out value, out expressionType))
            {
                return true;
            }

            value = default(TKey);
            expressionType = default(ExpressionType);
            return false;
        }

        /// <summary>
        /// Determine if the binary expression is comparing the key value against a constant.
        /// </summary>
        /// <param name="expression">The expression.</param>
        /// <param name="keyMemberInfo">The name of the parameter key.</param>
        /// <param name="value">Returns the constant being compared against.</param>
        /// <param name="expressionType">Returns the type of the comparison.</param>
        /// <returns>True if this expression is comparing the key value against a constant.</returns>
        private static bool IsSimpleComparisonExpression(BinaryExpression expression, MemberInfo keyMemberInfo, out TKey value, out ExpressionType expressionType)
        {
            if (IsKeyAccess(expression.Left, keyMemberInfo)
                && ConstantExpressionEvaluator<TKey>.TryGetConstantExpression(expression.Right, out value))
            {
                expressionType = expression.NodeType;
                return true;
            }

            if (IsKeyAccess(expression.Right, keyMemberInfo)
                && ConstantExpressionEvaluator<TKey>.TryGetConstantExpression(expression.Left, out value))
            {
                // The access is on the right so we have to switch the comparison type
                expressionType = GetReverseExpressionType(expression.NodeType);
                return true;
            }

            expressionType = ExpressionType.Equal;
            value = default(TKey);
            return false;
        }

        /// <summary>
        /// Determine if the binary expression is comparing the key value against a constant
        /// using CompareTo.
        /// </summary>
        /// <param name="expression">The expression.</param>
        /// <param name="keyMemberInfo">The name of the parameter key.</param>
        /// <param name="value">Returns the constant being compared against.</param>
        /// <param name="expressionType">Returns the type of the comparison.</param>
        /// <returns>True if this expression is comparing the key value against a constant.</returns>
        private static bool IsCompareToExpression(BinaryExpression expression, MemberInfo keyMemberInfo, out TKey value, out ExpressionType expressionType)
        {
            // CompareTo is only guaranteed to return <0, 0 or >0 so allowing for
            // comparisons with values other than 0 is complicated/subtle.
            // One way this could be expanded is by recognizing "< 1", and "> -1" as well.
            if (IsCompareTo(expression.Left, keyMemberInfo, out value))
            {
                int comparison;
                if (ConstantExpressionEvaluator<int>.TryGetConstantExpression(expression.Right, out comparison)
                    && 0 == comparison)
                {
                    expressionType = expression.NodeType;

                    return true;
                }
            }

            if (IsCompareTo(expression.Right, keyMemberInfo, out value))
            {
                int comparison;
                if (ConstantExpressionEvaluator<int>.TryGetConstantExpression(expression.Left, out comparison)
                    && 0 == comparison)
                {
                    expressionType = GetReverseExpressionType(expression.NodeType);

                    return true;
                }
            }

            expressionType = ExpressionType.Equal;
            value = default(TKey);
            return false;
        }

        /// <summary>
        /// Determine if the binary expression is comparing the key value against a string
        /// using the simplest (two-argument) form of String.Compare.
        /// </summary>
        /// <param name="expression">The expression.</param>
        /// <param name="keyMemberInfo">The name of the parameter key.</param>
        /// <param name="value">Returns the constant being compared against.</param>
        /// <param name="expressionType">Returns the type of the comparison.</param>
        /// <returns>True if this expression is comparing the key value against a constant string.</returns>
        private static bool IsStringComparisonExpression(BinaryExpression expression, MemberInfo keyMemberInfo, out TKey value, out ExpressionType expressionType)
        {
            Debug.Assert(typeof(string) == typeof(TKey), "This method should only be called for string keys");

            // CompareTo is only guaranteed to return <0, 0 or >0 so allowing for
            // comparisons with values other than 0 is complicated/subtle.
            // One way this could be expanded is by recognizing "< 1", and "> -1" as well.
            //
            // This code is tricky because there are 4 possibilities and we want
            // to turn them into a canonical form. In the first two cases we do
            // not reverse the sense of the comparison:
            //   1. String.Compare(Key, "m") < 0
            //   2. 0 < String.Compare("m", Key)
            // In the second two cases we do reverse the sense of the comparison:
            //   3. String.Compare("m", Key) > 0
            //   4. 0 > String.Compare(Key, "m")
            if (IsStringCompare(expression.Left, keyMemberInfo, out value))
            {
                int comparison;
                if (ConstantExpressionEvaluator<int>.TryGetConstantExpression(expression.Right, out comparison) && 0 == comparison)
                {
                    expressionType = expression.NodeType;
                    return true;
                }                
            }
            else if (IsStringCompareReversed(expression.Right, keyMemberInfo, out value))
            {
                int comparison;
                if (ConstantExpressionEvaluator<int>.TryGetConstantExpression(expression.Left, out comparison) && 0 == comparison)
                {
                    expressionType = expression.NodeType;
                    return true;
                }
            }
            else if (IsStringCompareReversed(expression.Left, keyMemberInfo, out value))
            {
                int comparison;
                if (ConstantExpressionEvaluator<int>.TryGetConstantExpression(expression.Right, out comparison) && 0 == comparison)
                {
                    expressionType = GetReverseExpressionType(expression.NodeType);
                    return true;
                }
            }
            else if (IsStringCompare(expression.Right, keyMemberInfo, out value))
            {
                int comparison;
                if (ConstantExpressionEvaluator<int>.TryGetConstantExpression(expression.Left, out comparison) && 0 == comparison)
                {
                    expressionType = GetReverseExpressionType(expression.NodeType);
                    return true;
                }
            }

            expressionType = ExpressionType.Equal;
            value = default(TKey);
            return false;
        }

        /// <summary>
        /// Reverse the type of the comparison. This is used when the key is on the right hand side
        /// of the comparison, so that 3 LT Key becomes Key GT 3.
        /// </summary>
        /// <param name="originalExpressionType">The original expression type.</param>
        /// <returns>The reverse of a comparison expression type or the original expression type.</returns>
        private static ExpressionType GetReverseExpressionType(ExpressionType originalExpressionType)
        {
            ExpressionType expressionType;
            switch (originalExpressionType)
            {
                case ExpressionType.LessThan:
                    expressionType = ExpressionType.GreaterThan;
                    break;
                case ExpressionType.LessThanOrEqual:
                    expressionType = ExpressionType.GreaterThanOrEqual;
                    break;
                case ExpressionType.GreaterThan:
                    expressionType = ExpressionType.LessThan;
                    break;
                case ExpressionType.GreaterThanOrEqual:
                    expressionType = ExpressionType.LessThanOrEqual;
                    break;
                default:
                    expressionType = originalExpressionType;
                    break;
            }

            return expressionType;
        }

        /// <summary>
        /// Determine if the expression is accessing the key of the paramter of the expression.
        /// parameter.
        /// </summary>
        /// <param name="expression">The expression to evaluate.</param>
        /// <param name="keyMemberInfo">The name of the parameter member that is the key.</param>
        /// <returns>True if the expression is accessing the key of the parameter.</returns>
        private static bool IsKeyAccess(Expression expression, MemberInfo keyMemberInfo)
        {
            if (ExpressionType.Convert == expression.NodeType || ExpressionType.ConvertChecked == expression.NodeType)
            {
                UnaryExpression convertExpression = (UnaryExpression)expression;
                return IsKeyAccess(convertExpression.Operand, keyMemberInfo);
            }

            // If keyMemberInfo is null then we are using the parameter directly
            if (null == keyMemberInfo && ExpressionType.Parameter == expression.NodeType)
            {
                return true;
            }

            // If keyMemberInfo is non-null then we are accessing this part of the parameter
            if (null != keyMemberInfo && ExpressionType.MemberAccess == expression.NodeType)
            {
                var member = (MemberExpression)expression;
                if (
                    null != member.Expression
                    && member.Expression.NodeType == ExpressionType.Parameter
                    && member.Member == keyMemberInfo)
                {
                    return true;
                }
            }

            return false;
        }

        /// <summary>
        /// Determine if the expression is a call to [param].[member].CompareTo(value).
        /// </summary>
        /// <param name="expression">The expression to examine.</param>
        /// <param name="keyMemberInfo">The name of the key member.</param>
        /// <param name="value">Returns the string value being compared against.</param>
        /// <returns>
        /// True if the expression is a call to parameter.keyMember.CompareTo(value).
        /// </returns>
        private static bool IsCompareTo(Expression expression, MemberInfo keyMemberInfo, out TKey value)
        {
            if (expression is MethodCallExpression)
            {
                MethodCallExpression methodCall = (MethodCallExpression)expression;
                if (methodCall.Method == compareToMethod
                    && null != methodCall.Object
                    && IsKeyAccess(methodCall.Object, keyMemberInfo))
                {
                    return ConstantExpressionEvaluator<TKey>.TryGetConstantExpression(methodCall.Arguments[0], out value);
                }
            }

            value = default(TKey);
            return false;
        }

        /// <summary>
        /// Determine if the expression is a call to String.Compare(key, value).
        /// </summary>
        /// <param name="expression">The expression to examine.</param>
        /// <param name="keyMemberInfo">The name of the key member.</param>
        /// <param name="value">Returns the string value being compared against.</param>
        /// <returns>
        /// True if the expression is a call to String.Compare(key, value).
        /// </returns>
        private static bool IsStringCompare(Expression expression, MemberInfo keyMemberInfo, out TKey value)
        {
            if (expression is MethodCallExpression)
            {
                MethodCallExpression methodCall = (MethodCallExpression)expression;
                if (methodCall.Method == StringExpressionEvaluatorHelper.StringStaticCompareMethod
                    && IsKeyAccess(methodCall.Arguments[0], keyMemberInfo))
                {
                    return ConstantExpressionEvaluator<TKey>.TryGetConstantExpression(methodCall.Arguments[1], out value);
                }
            }

            value = default(TKey);
            return false;
        }

        /// <summary>
        /// Determine if the expression is a call to String.Compare(value, key).
        /// </summary>
        /// <param name="expression">The expression to examine.</param>
        /// <param name="keyMemberInfo">The name of the key member.</param>
        /// <param name="value">Returns the string value being compared against.</param>
        /// <returns>
        /// True if the expression is a call to String.Compare(value, key).
        /// </returns>
        private static bool IsStringCompareReversed(Expression expression, MemberInfo keyMemberInfo, out TKey value)
        {
            if (expression is MethodCallExpression)
            {
                MethodCallExpression methodCall = (MethodCallExpression)expression;
                if (methodCall.Method == StringExpressionEvaluatorHelper.StringStaticCompareMethod
                    && IsKeyAccess(methodCall.Arguments[1], keyMemberInfo))
                {
                    return ConstantExpressionEvaluator<TKey>.TryGetConstantExpression(methodCall.Arguments[0], out value);
                }
            }

            value = default(TKey);
            return false;
        }
    }
}