//-----------------------------------------------------------------------
// <copyright file="NumberUtil.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

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

        public static string NumberToString(float number)
        {
            return number.ToString("G", CultureInfo.InvariantCulture);
        }
    }
}
