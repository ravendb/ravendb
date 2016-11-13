// -----------------------------------------------------------------------
//  <copyright file="IndexToAdd.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;

namespace Raven.Abstractions.Data
{
    public class SideBySideIndexes
    {
        /// <summary>
        /// Side-by-side indexes definitions
        /// </summary>
        public IndexToAdd[] IndexesToAdd { get; set; }

        /// <summary>
        /// Minimum etag before replacement
        /// </summary>
        public long? MinimumEtagBeforeReplace { get; set; }

        /// <summary>
        /// UTC time of replacement
        /// </summary>
        public DateTime? ReplaceTimeUtc { get; set; }
    }
}
