//-----------------------------------------------------------------------
// <copyright file="HiLoResult.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;

namespace Raven.NewClient.Abstractions.Data
{
    /// <summary>
    /// The result of a NextHiLo operation
    /// </summary>   
    public class HiLoResult
    {
        public HiLoResult()
        {

        }

        public HiLoResult(long low, long high, string prefix, long lastSize, DateTime lastRangeAt)
        {
            Low = low;
            High = high;
            Prefix = prefix;
            LastSize = lastSize;
            LastRangeAt = lastRangeAt;
        }

        public long Low { get; set; }

        public long High { get; set; }

        /// <summary>
        /// prefix string, including the server prefix (if exists)
        /// </summary>
        public string Prefix { get; set; }

        public long LastSize { get; set; }

        public DateTime LastRangeAt { get; set; }

    }
}
