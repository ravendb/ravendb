//-----------------------------------------------------------------------
// <copyright file="IndexStats.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using Raven.Client.Documents.DataArchival;
using Raven.Client.Documents.Operations.DataArchival;
using Raven.Client.Util;

namespace Raven.Client.Documents.Indexes
{
    public sealed class IndexStats
    {
        /// <summary>
        /// Index name.
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Indicates how many times the database tried to index documents (map) using this index.
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
        /// Indicates how many times the database tried to index referenced documents (map) using this index.
        /// </summary>
        public int? MapReferenceAttempts { get; set; }

        /// <summary>
        /// Indicates how many indexing attempts of referenced documents succeeded.
        /// </summary>
        public int? MapReferenceSuccesses { get; set; }

        /// <summary>
        /// Indicates how many indexing attempts of referenced documents failed.
        /// </summary>
        public int? MapReferenceErrors { get; set; }

        /// <summary>
        /// Indicates how many times database tried to index documents (reduce) using this index.
        /// </summary>
        public long? ReduceAttempts { get; set; }

        /// <summary>
        /// Indicates how many reducing attempts succeeded.
        /// </summary>
        public long? ReduceSuccesses { get; set; }

        /// <summary>
        /// Indicates how many reducing attempts failed.
        /// </summary>
        public long? ReduceErrors { get; set; }
        
        /// <summary>
        /// The reduce output collection.
        /// </summary>
        public string ReduceOutputCollection { get; set; }
        
        /// <summary>
        /// Pattern for creating IDs for the reduce output reference-collection 
        /// </summary>
        public string ReduceOutputReferencePattern { get; set; }
        
        /// <summary>
        /// Collection name for reduce output reference-collection 
        /// </summary>
        public string PatternReferencesCollectionName { get; set; }

        /// <summary>
        /// The value of docs/sec rate for the index over the last minute
        /// </summary>
        public double MappedPerSecondRate { get; set; }

        /// <summary>
        /// The value of reduces/sec rate for the index over the last minute
        /// </summary>
        public double ReducedPerSecondRate { get; set; }

        /// <summary>
        /// Indicates the maximum number of produced indexing outputs from a single document
        /// </summary>
        public int MaxNumberOfOutputsPerDocument { get; set; }

        public Dictionary<string, CollectionStats> Collections { get; set; }

        /// <summary>
        /// Time of last query for this index.
        /// </summary>
        public DateTime? LastQueryingTime { get; set; }

        /// <summary>
        /// Index state (Normal, Disabled, Idle, Abandoned, Error)
        /// </summary>
        public IndexState State { get; set; }

        /// <summary>
        /// Index priority (Low, Normal, High)
        /// </summary>
        public IndexPriority Priority { get; set; }

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
        
        /// <summary>
        /// Indicates search engine.
        /// </summary>
        public SearchEngineType SearchEngineType { get; set; }
        
        /// <summary>
        /// Indicates whether archived, unarchived or all documents will be indexed
        /// </summary>
        public ArchivedDataProcessingBehavior? ArchivedDataProcessingBehavior { get; set; }

        public IndexRunningStatus Status { get; set; }

        /// <summary>
        /// Total number of entries in this index.
        /// </summary>
        public long EntriesCount { get; set; }

        public int ErrorsCount { get; set; }
        
        public IndexSourceType SourceType { get; set; }
        
        /// <summary>
        /// Returns the names of referenced collections.
        /// </summary>
        public HashSet<string> ReferencedCollections { get; set; }

#if FEATURE_TEST_INDEX
        /// <summary>
        /// Indicates if this is a test index (works on a limited data set - for testing purposes only)
        /// </summary>
        public bool IsTestIndex { get; set; }
#endif

        /// <summary>
        /// Determines if index is invalid. If more than 15% of attempts (map or reduce) are errors then value will be <c>true</c>.
        /// </summary>
        public bool IsInvalidIndex => IndexFailureInformation.CheckIndexInvalid(MapAttempts, MapErrors, MapReferenceAttempts, MapReferenceErrors, ReduceAttempts, ReduceErrors, IsStale);

        public MemoryStats Memory { get; set; }

        public IndexingPerformanceBasicStats LastBatchStats { get; set; }

        public sealed class MemoryStats
        {
            public MemoryStats()
            {
                DiskSize = new Size();
                ThreadAllocations = new Size();
                MemoryBudget = new Size();
            }

            public Size DiskSize { get; set; }
            public Size ThreadAllocations { get; set; }
            public Size MemoryBudget { get; set; }
        }

        public sealed class CollectionStats
        {
            public CollectionStats()
            {
                DocumentLag = -1;
                TombstoneLag = -1;
            }

            public long LastProcessedDocumentEtag { get; set; }

            public long LastProcessedTombstoneEtag { get; set; }

            public long LastProcessedTimeSeriesDeletedRangeEtag { get; set; }

            public long DocumentLag { get; set; }

            public long TombstoneLag { get; set; }
        }
    }
}
