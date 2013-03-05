// --------------------------------------------------------------------------------------------------------------------
// <copyright file="ConstantExpressionEvaluator.cs" company="Microsoft Corporation">
//   Copyright (c) Microsoft Corporation.
// </copyright>
// <summary>
//   Methods to evaluate an expression which returns a T.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Collections.Generic
{
    using System;
    using System.Globalization;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;

    /// <summary>
    /// Methods to evaluate an expression which returns a T.
    /// </summary>
    /// <typeparam name="T">The type returned by the expression.</typeparam>
    internal static class ConstantExpressionEvaluator<T>
    {
        /// <summary>
        /// Determine if the given expression is a constant expression, and
        /// return the value of the expression.
        /// </summary>
        /// <param name="expression">The expression to evaluate.</param>
        /// <param name="value">The value of the expression.</param>
        /// <returns>True if the expression was a constant, false otherwise.</returns>
        public static bool TryGetConstantExpression(Expression expression, out T value)
        {
            if (HasNoParameterAccess(expression))
            {
                object obj = GetExpressionValue(expression);
                if (null != obj)
                {
                    try
                    {
                        value = (T)Convert.ChangeType(obj, typeof(T), CultureInfo.InvariantCulture);
                        return true;
                    }
                    catch (InvalidCastException)
                    {
                        // Expected
                    }
                }
            }

            value = default(T);
            return false;
        }

        /// <summary>
        /// Get the value of the expression.
        /// </summary>
        /// <param name="expression">The expression to get the value of.</param>
        /// <returns>The value of the expression.</returns>
        private static object GetExpressionValue(Expression expression)
        {
            switch (expression.NodeType)
            {
                case ExpressionType.Convert:
                {
                    UnaryExpression unaryExpression = (UnaryExpression)expression;
                    object value = GetExpressionValue(unaryExpression.Operand);
                    if (null == value)
                    {
                        return null;
                    }

                    try
                    {
                        return Convert.ChangeType(value, unaryExpression.Type, CultureInfo.InvariantCulture);
                    }
                    catch (InvalidCastException)
                    {
                        // If conversion fails here then pass the buck by returning
                        // the unconverted value.
                        return value;
                    }
                }

                case ExpressionType.Constant:
                {
                    ConstantExpression constantExpression = (ConstantExpression)expression;
                    return constantExpression.Value;
                }

                case ExpressionType.MemberAccess:
                {
                    MemberExpression memberExpression = (MemberExpression)expression;
                    object obj;
                    if (null == memberExpression.Expression)
                    {
                        obj = null;
                    }
                    else if (ExpressionType.Constant == memberExpression.Expression.NodeType)
                    {
                        ConstantExpression constantExpression = (ConstantExpression)memberExpression.Expression;
                        obj = constantExpression.Value;                        
                    }
                    else
                    {
                        break;
                    }

                    if (memberExpression.Member is FieldInfo)
                    {
                        return ((FieldInfo)memberExpression.Member).GetValue(obj);
                    }

                    break;
                }
            }

            if (typeof(T) != expression.Type)
            {
                // Jump through some hoops to create a lambda expression
                // of the correct type.
                return ConstantExpressionCompiler.Compile(expression);
            }

            return Expression.Lambda<Func<T>>(expression).Compile()();
        }

        /// <summary>
        /// Determine if there are any parameter access calls in the expression.
        /// </summary>
        /// <param name="expression">The expression.</param>
        /// <returns>
        /// True if there are no parameter accesses in the expression.
        /// </returns>
        /// <remarks>
        /// This method is conservative. We only return true if we end up with
        /// null, a constant or parameter access. Unknown expressions return false.
        /// </remarks>
        private static bool HasNoParameterAccess(Expression expression)
        {
            if (null == expression)
            {
                return true;
            }

            switch (expression.NodeType)
            {
                case ExpressionType.Constant:
                    return true;

                case ExpressionType.Parameter:
                    return false;
            }

            if (expression is UnaryExpression)
            {
                UnaryExpression unaryExpression = (UnaryExpression)expression;
                return HasNoParameterAccess(unaryExpression.Operand);
            }
            else if (expression is BinaryExpression)
            {
                BinaryExpression binaryExpression = (BinaryExpression)expression;
                return HasNoParameterAccess(binaryExpression.Left) && HasNoParameterAccess(binaryExpression.Right);
            }
            else if (expression is ConditionalExpression)
            {
                ConditionalExpression conditionalExpression = (ConditionalExpression)expression;
                return HasNoParameterAccess(conditionalExpression.Test)
                       && HasNoParameterAccess(conditionalExpression.IfFalse)
                       && HasNoParameterAccess(conditionalExpression.IfTrue);
            }
            else if (expression is MethodCallExpression)
            {
                MethodCallExpression callExpression = (MethodCallExpression)expression;
                return callExpression.Arguments.All(HasNoParameterAccess) && HasNoParameterAccess(callExpression.Object);
            }
            else if (expression is InvocationExpression)
            {
                InvocationExpression invocationExpression = (InvocationExpression)expression;
                return invocationExpression.Arguments.All(HasNoParameterAccess) &&
                       HasNoParameterAccess(invocationExpression.Expression);
            }
            else if (expression is MemberExpression)
            {
                MemberExpression memberExpression = (MemberExpression)expression;
                return HasNoParameterAccess(memberExpression.Expression);
            }
            else if (expression is NewArrayExpression)
            {
                NewArrayExpression newArrayExpression = (NewArrayExpression)expression;
                return newArrayExpression.Expressions.All(HasNoParameterAccess);
            }

            return false;
        }
    }
}