// --------------------------------------------------------------------------------------------------------------------
// <copyright file="ConstantExpressionCompiler.cs" company="Microsoft Corporation">
//   Copyright (c) Microsoft Corporation.
// </copyright>
// <summary>
//   Methods to compile and evaluate an expression which returns a T.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Collections.Generic
{
    using System;
    using System.Collections.Generic;
    using System.Linq.Expressions;

    /// <summary>
    /// Compile and evaluate expressions.
    /// </summary>
    internal static class ConstantExpressionCompiler
    {
        /// <summary>
        /// Dictionary of type => compiler mapping. The compiler functions
        /// take an expression and return its value.
        /// </summary>
        private static readonly Dictionary<Type, Func<Expression, object>> compilers = new Dictionary<Type, Func<Expression, object>>
        {
            { typeof(bool), GetValue<bool> },
            { typeof(bool?), GetValue<bool?> },
            { typeof(byte), GetValue<byte> },
            { typeof(byte?), GetValue<byte?> },
            { typeof(short), GetValue<short> },
            { typeof(short?), GetValue<short?> },
            { typeof(ushort), GetValue<ushort> },
            { typeof(ushort?), GetValue<ushort?> },
            { typeof(int), GetValue<int> },
            { typeof(int?), GetValue<int?> },
            { typeof(uint), GetValue<uint> },
            { typeof(uint?), GetValue<uint?> },
            { typeof(long), GetValue<long> },
            { typeof(long?), GetValue<long?> },
            { typeof(ulong), GetValue<ulong> },
            { typeof(ulong?), GetValue<ulong?> },
            { typeof(float), GetValue<float> },
            { typeof(float?), GetValue<float?> },
            { typeof(double), GetValue<double> },
            { typeof(double?), GetValue<double?> },
            { typeof(DateTime), GetValue<DateTime> },
            { typeof(DateTime?), GetValue<DateTime?> },
            { typeof(TimeSpan), GetValue<TimeSpan> },
            { typeof(TimeSpan?), GetValue<TimeSpan?> },
            { typeof(Guid), GetValue<Guid> },
            { typeof(Guid?), GetValue<Guid?> },
        };

        /// <summary>
        /// Compile an expression and return its value.
        /// </summary>
        /// <param name="expression">The expression to compile.</param>
        /// <returns>The value of the expression or null if no value can be determined.</returns>
        public static object Compile(Expression expression)
        {
            Func<Expression, object> f;
            if (compilers.TryGetValue(expression.Type, out f))
            {
                return f(expression);
            }

            // Can't evaluate
            return null;
        }

        /// <summary>
        /// Compile an expression of a specific type and return its value.
        /// </summary>
        /// <typeparam name="T">The return type of the expression.</typeparam>
        /// <param name="expression">The expression.</param>
        /// <returns>The value of the compiled expression.</returns>
        private static object GetValue<T>(Expression expression)
        {
            return Expression.Lambda<Func<T>>(expression).Compile()();            
        }        
    }
}