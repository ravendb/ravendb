// -----------------------------------------------------------------------
//  <copyright file="IndexToAdd.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Indexing;
using Raven.NewClient.Client.Data.Indexes;
using Raven.NewClient.Client.Indexing;

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
    }
}
