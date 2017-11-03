//-----------------------------------------------------------------------
// <copyright file="NumberUtil.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Globalization;

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
        /// Translate an indexable string to a nullable double
        /// </summary>
        public static bool TryStringToDouble(string number, out double value)
        {
            value = 0;

            if (number == null)
                return false;

            if (number.Length == 0)
                throw new ArgumentException("String must be greater than 0 characters");

            return double.TryParse(number, NumberStyles.None, CultureInfo.InvariantCulture, out value);
        }
    }
}
