//-----------------------------------------------------------------------
// <copyright file="IndexStats.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Globalization;
using Raven.Abstractions.Indexing;

namespace Raven.Abstractions.Data
{
	public class IndexStats
	{
		public int Id { get; set; }
		public string Name { get; set; }
		public int IndexingAttempts { get; set; }
		public int IndexingSuccesses { get; set; }
		public int IndexingErrors { get; set; }
		public Etag LastIndexedEtag { get; set; }
		public int? IndexingLag { get; set; }
        public DateTime LastIndexedTimestamp { get; set; }
		public DateTime? LastQueryTimestamp { get; set; }
		public int TouchCount { get; set; }
        public IndexingPriority Priority { get; set; }
		public int? ReduceIndexingAttempts { get; set; }
		public int? ReduceIndexingSuccesses { get; set; }
		public int? ReduceIndexingErrors { get; set; }
		public Etag LastReducedEtag { get; set; }
		public DateTime? LastReducedTimestamp { get; set; }
        public DateTime CreatedTimestamp { get; set; }
		public DateTime LastIndexingTime { get; set; }
		public string IsOnRam { get; set; }
		public IndexLockMode LockMode { get; set; }
		public List<string> ForEntityName { get; set; } 

		public IndexingPerformanceStats[] Performance { get; set; }
		public int DocsCount { get; set; }
        public bool IsTestIndex { get; set; }

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

    public class IndexingPerformanceStats
	{
	    public IndexingPerformanceStats()
	    {
		    LoadDocumentPerformance = new LoadDocumentPerformanceStats();
			LinqExecutionPerformance = new LinqExecutionPerformanceStats();
			LucenePerformance = new LucenePerformanceStats();
			MapStoragePerformance = new MapStoragePerformanceStats();
	    }

	    public string Operation { get; set; }
	    public int ItemsCount { get; set; }
	    public int InputCount { get; set; }
	    public int OutputCount { get; set; }
		public DateTime Started { get; set; }
		public DateTime Completed { get; set; }
	    public TimeSpan Duration { get; set; }
	    public double DurationMilliseconds { get { return Math.Round(Duration.TotalMilliseconds, 2); } }
		public LoadDocumentPerformanceStats LoadDocumentPerformance { get; set; }
		public LinqExecutionPerformanceStats LinqExecutionPerformance { get; set; }
		public LucenePerformanceStats LucenePerformance { get; set; }
		public MapStoragePerformanceStats MapStoragePerformance { get; set; }
	    public TimeSpan WaitingTimeSinceLastBatchCompleted { get; set; }
	}

	public class LinqExecutionPerformanceStats
	{
		public long MapLinqExecutionDurationMs { get; set; }
		public long ReduceLinqExecutionDurationMs { get; set; }
	}

	public class LoadDocumentPerformanceStats
	{
		public int LoadDocumentCount { get; set; }
		public long LoadDocumentDurationMs { get; set; }
	}

	public class MapStoragePerformanceStats
	{
		public MapStoragePerformanceStats()
		{
			DeleteMappedResultsDurationMs = -1;
			PutMappedResultsDurationMs = -1;
			StorageCommitDurationMs = -1;
		}

		public long DeleteMappedResultsDurationMs { get; set; }
		public long PutMappedResultsDurationMs { get; set; }
		public long StorageCommitDurationMs { get; set; }
	}

	public class LucenePerformanceStats
	{
		public long WriteDocumentsDurationMs { get; set; }
		public long FlushToDiskDurationMs { get; set; }
	}

	public class ReducingPerformanceStats
	{
		public ReduceType ReduceType { get; set; }
		public List<ReduceLevelPeformanceStats> LevelStats { get; set; } 
	}

	public class ReduceLevelPeformanceStats
	{
		public ReduceLevelPeformanceStats()
		{
			LinqExecutionPerformance = new LinqExecutionPerformanceStats()
			{
				MapLinqExecutionDurationMs = -1
			};
			LucenePerformance = new LucenePerformanceStats();
			ReduceStoragePerformance = new ReduceStoragePerformanceStats();
		}

		public int Level { get; set; }
		public int ItemsCount { get; set; }
		public int InputCount { get; set; }
		public int OutputCount { get; set; }
		public DateTime Started { get; set; }
		public DateTime Completed { get; set; }
		public TimeSpan Duration { get; set; }
		public double DurationMs{ get { return Math.Round(Duration.TotalMilliseconds, 2); } }
		public LinqExecutionPerformanceStats LinqExecutionPerformance { get; set; }
		public LucenePerformanceStats LucenePerformance { get; set; }
		public ReduceStoragePerformanceStats ReduceStoragePerformance { get; set; }

		public void Add(IndexingPerformanceStats other)
		{
			ItemsCount += other.ItemsCount;
			InputCount += other.InputCount;
			OutputCount += other.OutputCount;

			LinqExecutionPerformance.ReduceLinqExecutionDurationMs += other.LinqExecutionPerformance.ReduceLinqExecutionDurationMs;

			LucenePerformance.WriteDocumentsDurationMs += other.LucenePerformance.WriteDocumentsDurationMs;
			LucenePerformance.FlushToDiskDurationMs += other.LucenePerformance.FlushToDiskDurationMs;
		}
	}

	public class ReduceStoragePerformanceStats
	{
		public long GetItemsToReduceDurationMs { get; set; }
	}
}
