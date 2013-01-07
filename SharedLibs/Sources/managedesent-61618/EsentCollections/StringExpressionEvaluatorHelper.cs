// --------------------------------------------------------------------------------------------------------------------
// <copyright file="StringExpressionEvaluatorHelper.cs" company="Microsoft Corporation">
//   Copyright (c) Microsoft Corporation.
// </copyright>
// <summary>
//   Code to evaluate a predicate Expression and determine
//   a key range which contains all items matched by the predicate.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Collections.Generic
{
    using System.Reflection;

    /// <summary>
    /// Methods for dealing with string expressions.
    /// </summary>
    internal static class StringExpressionEvaluatorHelper
    {
        /// <summary>
        /// A MethodInfo describing the static String.Compare(string, string).
        /// </summary>
        private static readonly MethodInfo stringStaticCompareMethod = typeof(string).GetMethod("Compare", new[] { typeof(string), typeof(string) });

        /// <summary>
        /// A MethodInfo describing the static String.Equals(string, string).
        /// </summary>
        private static readonly MethodInfo stringStaticEqualsMethod = typeof(string).GetMethod("Equals", new[] { typeof(string), typeof(string) });

        /// <summary>
        /// A MethodInfo describing String.StartsWith(string).
        /// </summary>
        private static readonly MethodInfo stringStartsWithMethod = typeof(string).GetMethod("StartsWith", new[] { typeof(string) }); 
       
        /// <summary>
        /// Gets a MethodInfo describing the static String.Compare(string, string).
        /// </summary>
        public static MethodInfo StringStaticCompareMethod
        {
            get
            {
                return stringStaticCompareMethod;
            }
        }

        /// <summary>
        /// Gets a MethodInfo describing the static String.Equals(string, string).
        /// </summary>
        public static MethodInfo StringStaticEqualsMethod
        {
            get
            {
                return stringStaticEqualsMethod;
            }
        }

        /// <summary>
        /// Gets a MethodInfo describing String.StartsWith(string, string).
        /// </summary>
        public static MethodInfo StringStartWithMethod
        {
            get
            {
                return stringStartsWithMethod;
            }
        }
    }
}