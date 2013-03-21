// --------------------------------------------------------------------------------------------------------------------
// <copyright file="Compare.cs" company="Microsoft Corporation">
//   Copyright (c) Microsoft Corporation.
// </copyright>
// <summary>
//   Provides generic comparison methods.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Collections.Generic
{
    /// <summary>
    /// Provides generic comparison methods.
    /// </summary>
    internal static class Compare
    {
        /// <summary>
        /// Compare two objects to determine if they are equal.
        /// </summary>
        /// <typeparam name="T">The type to compare.</typeparam>
        /// <param name="value1">The first object.</param>
        /// <param name="value2">The second object.</param>
        /// <returns>True if they are equal.</returns>
        public static bool AreEqual<T>(T value1, T value2)
        {
            if (((null == value1) && (null == value2))
                || ((null != value1) && value1.Equals(value2)))
            {
                return true;
            }

            return false;
        }
    }
}