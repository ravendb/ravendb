//-----------------------------------------------------------------------
// <copyright file="HiLoResult.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;

namespace Raven.Client.Documents.Identity
{
    /// <summary>
    /// The result of a NextHiLo operation
    /// </summary>   
    public class HiLoResult
    {
        /// <summary>
        /// prefix string, including the server prefix (if exists)
        /// </summary>
        public string Prefix { get; set; }

        public long Low { get; set; }

        public long High { get; set; }

        public long LastSize { get; set; }

        public DateTime LastRangeAt { get; set; }

    }
}
