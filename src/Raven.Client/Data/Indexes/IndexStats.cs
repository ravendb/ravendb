//-----------------------------------------------------------------------
// <copyright file="IndexStats.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;

using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;

namespace Raven.Client.Data.Indexes
{
    public class IndexStats
    {
        /// <summary>
        /// Index identifier.
        /// </summary>
        public int Id { get; set; }

        /// <summary>
        /// Index name.
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Indicates how many times database tried to index documents (map) using this index.
        /// </summary>
        public int MapAttempts { get; set; }

        /// <summary>
        /// Indicates how many indexing attempts succeeded.
        /// </summary>
        public int MapSuccesses { get; set; }

        /// <summary>
        /// Indicates how many indexing attempts failed.
        /// </summary>
        public int MapErrors { get; set; }

        /// <summary>
        /// Indicates how many times database tried to index documents (reduce) using this index.
        /// </summary>
        public int? ReduceAttempts { get; set; }

        /// <summary>
        /// Indicates how many reducing attempts succeeded.
        /// </summary>
        public int? ReduceSuccesses { get; set; }

        /// <summary>
        /// Indicates how many reducing attempts failed.
        /// </summary>
        public int? ReduceErrors { get; set; }

        /// <summary>
        /// The duration of the current batch
        /// </summary>
        public TimeSpan CurrentBatchDuration { get; set; }

        /// <summary>
        /// The number of documents that have been processed in the current batch
        /// </summary>
        public int CurrentBatchDocuments { get; set; }

        /// <summary>
        /// The value of docs/sec rate for the index over the last minute
        /// </summary>
        public double MappedPerSecondRate { get; set; }

        /// <summary>
        /// The value of reduces/sec rate for the index over the last minute
        /// </summary>
        public double ReducedPerSecondRate { get; set; }


        public Dictionary<string, CollectionStats> Collections { get; set; }

        /// <summary>
        /// Time of last query for this index.
        /// </summary>
        public DateTime? LastQueryingTime { get; set; }

        /// <summary>
        /// Index priority (Normal, Disabled, Idle, Abandoned, Error)
        /// </summary>
        public IndexingPriority Priority { get; set; }

        /// <summary>
        /// Date of index creation.
        /// </summary>
        public DateTime CreatedTimestamp { get; set; }

        /// <summary>
        /// Time of last indexing (map or reduce) for this index.
        /// </summary>
        public DateTime? LastIndexingTime { get; set; }

        public bool IsStale { get; set; }

        /// <summary>
        /// Indicates current lock mode:
        /// <para>- Unlock - all index definition changes acceptable</para>
        /// <para>- LockedIgnore - all index definition changes will be ignored, only log entry will be created</para>
        /// <para>- LockedError - all index definition changes will raise exception</para>
        /// </summary>
        public IndexLockMode LockMode { get; set; }

        /// <summary>
        /// Indicates index type.
        /// </summary>
        public IndexType Type { get; set; }

        public IndexRunningStatus Status { get; set; }

        /// <summary>
        /// Total number of entries in this index.
        /// </summary>
        public int EntriesCount { get; set; }

        public int ErrorsCount { get; set; }

        /// <summary>
        /// Indicates if this is a test index (works on a limited data set - for testing purposes only)
        /// </summary>
        public bool IsTestIndex { get; set; }

        /// <summary>
        /// Determines if index is invalid. If more than 15% of attemps (map or reduce) are errors then value will be <c>true</c>.
        /// </summary>
        public bool IsInvalidIndex
        {
            get
            {
                return IndexFailureInformation.CheckIndexInvalid(MapAttempts, MapErrors, ReduceAttempts, ReduceErrors);
            }
        }

        public MemoryStats Memory { get; set; }

        public class MemoryStats
        {
            public MemoryStats()
            {
                DiskSize = new Size();
                ThreadAllocations = new Size();
            }

            public bool InMemory { get; set; }
            public Size DiskSize { get; set; }
            public Size ThreadAllocations { get; set; }
        }

        public class CollectionStats
        {
            public CollectionStats()
            {
                DocumentLag = -1;
                TombstoneLag = -1;
            }

            public long LastProcessedDocumentEtag { get; set; }

            public long LastProcessedTombstoneEtag { get; set; }

            public long DocumentLag { get; set; }

            public long TombstoneLag { get; set; }
        }
    }

    public enum IndexRunningStatus
    {
        Running,
        Paused,
        Disabled
    }

    [Flags]
    public enum IndexingPriority
    {
        None = 0,

        Normal = 1,

        Disabled = 2,

        Idle = 4,

        Error = 16,

        Forced = 512,
    }

    public enum IndexingOperation
    {
        // ReSharper disable InconsistentNaming
        LoadDocument,

        Linq_MapExecution,
        Linq_ReduceLinqExecution,

        Lucene_DeleteExistingDocument,
        Lucene_ConvertToLuceneDocument,
        Lucene_AddDocument,
        Lucene_FlushToDisk,
        Lucene_RecreateSearcher,

        Map_DeleteMappedResults,
        Map_ConvertToRavenJObject,
        Map_PutMappedResults,
        Map_ScheduleReductions,

        Reduce_GetItemsToReduce,
        Reduce_DeleteScheduledReductions,
        Reduce_ScheduleReductions,
        Reduce_GetMappedResults,
        Reduce_RemoveReduceResults,

        UpdateDocumentReferences,

        Extension_Suggestions,

        StorageCommit,
        // ReSharper restore InconsistentNaming
    }

    public abstract class BasePerformanceStats
    {
        public long DurationMs { get; set; }
    }

    public class PerformanceStats : BasePerformanceStats
    {
        public IndexingOperation Name { get; set; }


        public static PerformanceStats From(IndexingOperation name, long durationMs)
        {
            return new PerformanceStats
            {
                Name = name,
                DurationMs = durationMs
            };
        }
    }

    public class ParallelPerformanceStats : BasePerformanceStats
    {
        public ParallelPerformanceStats()
        {
            BatchedOperations = new List<ParallelBatchStats>();
        }
        public long NumberOfThreads { get; set; }

        public List<ParallelBatchStats> BatchedOperations { get; set; }
    }

    public class ParallelBatchStats
    {

        public ParallelBatchStats()
        {
            Operations = new List<PerformanceStats>();
        }
        public long StartDelay { get; set; }
        public List<PerformanceStats> Operations { get; set; }
    }
}
