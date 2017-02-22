//-----------------------------------------------------------------------
// <copyright file="NumberUtil.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Globalization;
using System.Runtime.CompilerServices;

namespace Raven.Client.Documents.Indexes
{
    /// <summary>
    /// Helper function for numeric to indexed string and vice versa
    /// </summary>
    internal static class NumberUtil
    {
        /// <summary>
        /// Translate a number to an indexable string
        /// </summary>
        public static string NumberToString(long number)
        {
            return number.ToString("G", CultureInfo.InvariantCulture);
        }

        /// <summary>
        /// Translate a number to an indexable string
        /// </summary>
        public static string NumberToString(double number)
        {
            return number.ToString("G", CultureInfo.InvariantCulture);
        }

        /// <summary>
        /// Translate an indexable string to a nullable long
        /// </summary>
        public static long? StringToLong(string number)
        {
            if (number == null)
                return null;

            if (IsNull(number))
                return null;

            if (number.Length == 0)
                throw new ArgumentException("String must be greater than 0 characters");

            return long.Parse(number, CultureInfo.InvariantCulture);
        }

        /// <summary>
        /// Translate an indexable string to a nullable double
        /// </summary>
        public static double? StringToDouble(string number)
        {
            if (number == null)
                return null;

            if (IsNull(number))
                return null;

            if (number.Length == 0)
                throw new ArgumentException("String must be greater than 0 characters");

            return double.Parse(number, CultureInfo.InvariantCulture);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsNull(string value)
        {
            return "NULL".Equals(value, StringComparison.OrdinalIgnoreCase) || "*".Equals(value, StringComparison.OrdinalIgnoreCase);
        }
    }
}
