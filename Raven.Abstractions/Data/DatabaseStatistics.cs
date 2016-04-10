//-----------------------------------------------------------------------
// <copyright file="DatabaseStatistics.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;

namespace Raven.Abstractions.Data
{
    public class DatabaseStatistics
    {
        /// <summary>
        /// Storage engine used by database (esent, voron).
        /// </summary>
        public string StorageEngine { get; set; }

        /// <summary>
        /// Last document etag in database.
        /// </summary>
        public Etag LastDocEtag { get; set; }

        /// <summary>
        /// Last attachment etag in database.
        /// </summary>
        [Obsolete("Use RavenFS instead.")]
        public Etag LastAttachmentEtag { get; set; }

        /// <summary>
        /// Total number of indexes in database.
        /// </summary>
        public int CountOfIndexes { get; set; }

        /// <summary>
        /// Total number of indexes in database excluding disabled and abandoned
        /// </summary>
        public int CountOfIndexesExcludingDisabledAndAbandoned { get; set; }

        /// <summary>
        /// Total number of transformers in database.
        /// </summary>
        public int CountOfResultTransformers { get; set; }

        /// <summary>
        /// Indicates how many elements are currently kept in queue for all indexing prefetchers available.
        /// </summary>
        public int[] InMemoryIndexingQueueSizes { get; set; }

        /// <summary>
        /// Indicates how many tasks (approximately) are running currently in database.
        /// </summary>
        public long ApproximateTaskCount { get; set; }

        /// <summary>
        /// Total number of documents in database.
        /// </summary>
        public long CountOfDocuments { get; set; }

        /// <summary>
        /// Total number of attachments in database.
        /// </summary>
        [Obsolete("Use RavenFS instead.")]
        public long CountOfAttachments { get; set; }

        /// <summary>
        /// List of stale index names in database..
        /// </summary>
        public string[] StaleIndexes { get; set; }

        /// <summary>
        /// Total number of stale indexes excluding disabled and abandoned
        /// </summary>
        public int CountOfStaleIndexesExcludingDisabledAndAbandoned { get; set; }

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
        /// Transaction version size in megabytes for database.
        /// </summary>
        public decimal DatabaseTransactionVersionSizeInMB { get; set; }

        /// <summary>
        /// Statistics for each index in database.
        /// </summary>
        public IndexStats[] Indexes { get; set; }

        /// <summary>
        /// Array of indexing errors that occured in database.
        /// </summary>
        public IndexingError[] Errors { get; set; }

        /// <summary>
        /// Information about future indexing batches.
        /// </summary>
        public FutureBatchStats[] Prefetches { get; set; }

        /// <summary>
        /// Database identifier.
        /// </summary>
        public Guid DatabaseId { get; set; }

        /// <summary>
        /// Indicates if database supports DTC transactions.
        /// </summary>
        public bool SupportsDtc { get; set; }

        /// <summary>
        /// Indicates if process is 64-bit
        /// </summary>
        public bool Is64Bit { get; set; }

        /// <summary>
        /// Indicates if the low memory thread is running
        /// </summary>
        public bool IsMemoryStatisticThreadRuning { get; set; }
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
