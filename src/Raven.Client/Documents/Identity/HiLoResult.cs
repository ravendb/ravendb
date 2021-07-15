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

        public string Prefix { get; set; }

        public long Low { get; set; }

        public long High { get; set; }

        public long LastSize { get; set; }

        /// <summary>
        /// The tag of the server that allocated this range.
        /// </summary>
        public string ServerTag { get; set; }

        public DateTime LastRangeAt { get; set; }

        public int? ShardIndex { get; set; }

    }

    public class HiloDocument
    {
        public long Max { get; set; }
    }
}
