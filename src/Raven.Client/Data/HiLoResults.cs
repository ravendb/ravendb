//-----------------------------------------------------------------------
// <copyright file="HiLoResult.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

namespace Raven.Abstractions.Data
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

        public long Low { get; set; }

        public long High { get; set; }

        /// <summary>
        /// prefix string, including the server prefix (if exists)
        /// </summary>
        public string Prefix { get; set; }


    }
}
