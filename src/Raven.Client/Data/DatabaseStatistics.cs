//-----------------------------------------------------------------------
// <copyright file="DatabaseStatistics.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;

using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Data.Indexes;

namespace Raven.Client.Data
{
    public class DatabaseStatistics
    {
        /// <summary>
        /// Last document etag in database.
        /// </summary>
        public long? LastDocEtag { get; set; }

        /// <summary>
        /// Total number of indexes in database.
        /// </summary>
        public int CountOfIndexes { get; set; }

        /// <summary>
        /// Total number of transformers in database.
        /// </summary>
        public int CountOfTransformers { get; set; }

        /// <summary>
        /// Indicates how many tasks (approximately) are running currently in database.
        /// </summary>
        public long ApproximateTaskCount { get; set; }

        /// <summary>
        /// Total number of documents in database.
        /// </summary>
        public long CountOfDocuments { get; set; }

        /// <summary>
        /// Total number of revision documents in database.
        /// </summary>
        public long? CountOfRevisionDocuments { get; set; }

        /// <summary>
        /// List of stale index names in database..
        /// </summary>
        public string[] StaleIndexes => Indexes?.Where(x => x.IsStale).Select(x => x.Name).ToArray();

        /// <summary>
        /// The concurrency level that RavenDB is currently using
        /// </summary>
        public int CurrentNumberOfParallelTasks { get; set; }

        /// <summary>
        /// Current value of items that will be processed by index (map) in single batch.
        /// </summary>
        public int CurrentNumberOfItemsToIndexInSingleBatch { get; set; }

        /// <summary>
        /// Current value of items that will be processed by index (reduce) in single batch.
        /// </summary>
        public int CurrentNumberOfItemsToReduceInSingleBatch { get; set; }

        /// <summary>
        /// Statistics for each index in database.
        /// </summary>
        public IndexInformation[] Indexes { get; set; }

        /// <summary>
        /// Database identifier.
        /// </summary>
        public Guid DatabaseId { get; set; }

        /// <summary>
        /// Indicates if process is 64-bit
        /// </summary>
        public bool Is64Bit { get; set; }
    }

    public class IndexInformation
    {
        public int IndexId { get; set; }

        public string Name { get; set; }

        public bool IsStale { get; set; }

        public IndexingPriority Priority { get; set; }

        public IndexLockMode LockMode { get; set; }

        public IndexType Type { get; set; }
    }

    public class TriggerInfo
    {
        public string Type { get; set; }

        public string Name { get; set; }
    }

    public class PluginsInfo
    {
        public List<ExtensionsLog> Extensions { get; set; }
        public List<TriggerInfo> Triggers { get; set; }
        public List<string> CustomBundles { get; set; }
    }
}
