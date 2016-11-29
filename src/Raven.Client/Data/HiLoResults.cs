//-----------------------------------------------------------------------
// <copyright file="HiLoResult.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

namespace Raven.Client.Data
{
    /// <summary>
    /// The result of a NextHiLo operation
    /// </summary>   
    public class HiLoResults
    {
        public HiLoResults(long low, long high, string prefix)
        {
            Low = low;
            High = high;
            Prefix = prefix;
        }
        /// <summary>
        /// long of the low value.
        /// </summary>
        public long Low { get; set; }

        /// <summary>
        /// long of the high value.
        /// </summary>
        public long High { get; set; }

        /// <summary>
        /// prefix string, including the server prefix (if exists)
        /// </summary>
        public string Prefix { get; set; }


    }
}
