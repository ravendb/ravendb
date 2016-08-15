// -----------------------------------------------------------------------
//  <copyright file="IndexToAdd.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Raven.Abstractions.Indexing;

namespace Raven.Abstractions.Data
{
    public class IndexToAdd
    {
        /// <summary>
        /// The name of an index that will be added
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Definition of an index
        /// </summary>
        public IndexDefinition Definition { get; set; }

        /// <summary>
        /// Priority of an index
        /// </summary>
        public IndexingPriority Priority { get; set; }

        /// <summary>
        /// Minimum etag before replacement
        /// </summary>
        public Etag MinimumEtagBeforeReplace { get; set; }

        /// <summary>
        /// UTC time of replacement
        /// </summary>
        public DateTime? ReplaceTimeUtc { get; set; }
    }
}
