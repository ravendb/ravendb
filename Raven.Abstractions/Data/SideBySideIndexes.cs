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
        /// The name of an index that will be added
        /// </summary>
        public IndexToAdd[] IndexesToAdd { get; set; }

        /// <summary>
        /// Definition of an index
        /// </summary>
        public Etag MinimumEtagBeforeReplace { get; set; }

        /// <summary>
        /// Priority of an index
        /// </summary>
        public DateTime? ReplaceTimeUtc { get; set; }
    }
}
