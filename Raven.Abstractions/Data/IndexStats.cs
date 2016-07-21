//-----------------------------------------------------------------------
// <copyright file="IndexStats.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

using Raven.Abstractions.Indexing;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Abstractions.Data
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
        public int IndexingAttempts { get; set; }

        /// <summary>
        /// Indicates how many indexing attempts succeeded.
        /// </summary>
        public int IndexingSuccesses { get; set; }

        /// <summary>
        /// Indicates how many indexing attempts failed.
        /// </summary>
        public int IndexingErrors { get; set; }

        /// <summary>
        /// This value represents etag of last document indexed (using map) by this index.
        /// </summary>
        public Etag LastIndexedEtag { get; set; }

        /// <summary>
        /// Shows the difference between last document etag available in database and last indexed etag.
        /// </summary>
        public int? IndexingLag { get; set; }

        /// <summary>
        /// Time of last indexing for this index.
        /// </summary>
        public DateTime LastIndexedTimestamp { get; set; }

        /// <summary>
        /// Time of last query for this index.
        /// </summary>
        public DateTime? LastQueryTimestamp { get; set; }

        public int TouchCount { get; set; }

        /// <summary>
        /// Index priority (Normal, Disabled, Idle, Abandoned, Error)
        /// </summary>
        public IndexingPriority Priority { get; set; }

        /// <summary>
        /// Indicates how many times database tried to index documents (reduce) using this index.
        /// </summary>
        public int? ReduceIndexingAttempts { get; set; }

        /// <summary>
        /// Indicates how many reducing attempts succeeded.
        /// </summary>
        public int? ReduceIndexingSuccesses { get; set; }

        /// <summary>
        /// Indicates how many reducing attempts failed.
        /// </summary>
        public int? ReduceIndexingErrors { get; set; }

        /// <summary>
        /// This value represents etag of last document indexed (using reduce) by this index.
        /// </summary>
        public Etag LastReducedEtag { get; set; }

        /// <summary>
        /// Time of last reduce for this index.
        /// </summary>
        public DateTime? LastReducedTimestamp { get; set; }

        /// <summary>
        /// Date of index creation.
        /// </summary>
        public DateTime CreatedTimestamp { get; set; }

        /// <summary>
        /// Time of last indexing (map or reduce) for this index.
        /// </summary>
        public DateTime LastIndexingTime { get; set; }

        /// <summary>
        /// Indicates if index is in-memory only.
        /// </summary>
        public string IsOnRam { get; set; }

        /// <summary>
        /// Indicates current lock mode:
        /// <para>- Unlock - all index definition changes acceptable</para>
        /// <para>- LockedIgnore - all index definition changes will be ignored, only log entry will be created</para>
        /// <para>- LockedError - all index definition changes will raise exception</para>
        /// </summary>
        public IndexLockMode LockMode { get; set; }

        /// <summary>
        /// Indicates if it is a MapReduce index.
        /// </summary>
        public bool IsMapReduce { get; set; }

        /// <summary>
        /// List of all entity names (collections) for which this index is working.
        /// </summary>
        public string[] ForEntityName { get; set; } 

        /// <summary>
        /// Total number of entries in this index.
        /// </summary>
        public int DocsCount { get; set; }

        /// <summary>
        /// Indicates if this is a test index (works on a limited data set - for testing purposes only)
        /// </summary>
        public bool IsTestIndex { get; set; }

        /// <summary>
        /// Determines if index is invalid. If more thant 15% of attemps (map or reduce) are errors then value will be <c>true</c>.
        /// </summary>
        public bool IsInvalidIndex
        {
            get
            {
                return IndexFailureInformation.CheckIndexInvalid(IndexingAttempts, IndexingErrors, ReduceIndexingAttempts, ReduceIndexingErrors);
            }
        }

        public override string ToString()
        {
            return Id.ToString(CultureInfo.InvariantCulture);
        }

        public void SetLastDocumentEtag(Etag lastDocEtag)
        {
            if (lastDocEtag == null)
                return;

            IndexingLag = (int) (lastDocEtag.Changes - LastIndexedEtag.Changes);

            if (lastDocEtag.Restarts != LastIndexedEtag.Restarts)
            {
                IndexingLag *= -1;
            }
        }
    }

    [Flags]
    public enum IndexingPriority
    {
        None = 0,

        Normal = 1,
        
        Disabled = 2,
        
        Idle = 4,
        
        Abandoned = 8,

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

        Delete_DeleteMappedResultsForDocumentId,
        Delete_Documents,
        Delete_IndexUpdateTriggers,

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

    public class ReducingPerformanceStats
    {
        public ReducingPerformanceStats(ReduceType reduceType)
        {
            ReduceType = reduceType;
            LevelStats = new List<ReduceLevelPeformanceStats>();
        }

        public ReduceType ReduceType { get; private set; }
        public List<ReduceLevelPeformanceStats> LevelStats { get; set; }
    }

    public class ReduceLevelPeformanceStats
    {
        public int Level { get; set; }
        public int ItemsCount { get; set; }
        public int InputCount { get; set; }
        public int OutputCount { get; set; }
        public DateTime Started { get; set; }
        public DateTime Completed { get; set; }
        public TimeSpan Duration { get; set; }
        public double DurationMs{ get { return Math.Round(Duration.TotalMilliseconds, 2); } }
        [JsonProperty(ItemTypeNameHandling = TypeNameHandling.Objects)]
        public List<BasePerformanceStats> Operations { get; set; }

        public ReduceLevelPeformanceStats()
        {
            Operations = new List<BasePerformanceStats>();
        }

        public void Add(IndexingPerformanceStats other)
        {
            ItemsCount += other.ItemsCount;
            InputCount += other.InputCount;
            OutputCount += other.OutputCount;
            foreach (var stats in other.Operations)
            {
                var performanceStats = stats as PerformanceStats;
                if (performanceStats != null)
                {
                    var existingStat = Operations.OfType<PerformanceStats>().FirstOrDefault(x => x.Name == performanceStats.Name);
                    if (existingStat != null)
                    {
                        existingStat.DurationMs += performanceStats.DurationMs;
                    }
                    else
                    {
                        Operations.Add(new PerformanceStats
                        {
                            Name = performanceStats.Name,
                            DurationMs = performanceStats.DurationMs
                        });
                    }
                }
            }
        }
    }
}
