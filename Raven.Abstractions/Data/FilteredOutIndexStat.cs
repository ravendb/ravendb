// -----------------------------------------------------------------------
//  <copyright file="FilteredOutIndexStat.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;

namespace Raven.Abstractions.Data
{
    public class FilteredOutIndexStat
    {
        /// <summary>
        /// Time of event
        /// </summary>
        public DateTime Timestamp { get; set; }

        /// <summary>
        /// Index name
        /// </summary>
        public string IndexName { get; set; }
    }
}
