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

	    public override bool Equals(object obj)
	    {
		    if (ReferenceEquals(null, obj)) return false;
		    if (ReferenceEquals(this, obj)) return true;
		    if (obj.GetType() != this.GetType()) return false;
		    return Equals((IndexingPerformanceStats) obj);
	    }

	    public override string ToString()
	    {
		    return string.Format(@"
Operation:         {0}
Input:              {1:#,#}
Output:              {2:#,#}
Duration:          {3}
Duration in ms: {4:#,#}
", Operation,
		                         InputCount,
		                         OutputCount,
		                         Duration,
		                         DurationMilliseconds);

	    }

		protected bool Equals(IndexingPerformanceStats other)
		{
			return string.Equals(Operation, other.Operation) && ItemsCount == other.ItemsCount && InputCount == other.InputCount && OutputCount == other.OutputCount && Started.Equals(other.Started) && Completed.Equals(other.Completed) && Duration.Equals(other.Duration) && Equals(LoadDocumentPerformance, other.LoadDocumentPerformance) && Equals(LinqExecutionPerformance, other.LinqExecutionPerformance) && Equals(LucenePerformance, other.LucenePerformance) && Equals(MapStoragePerformance, other.MapStoragePerformance) && WaitingTimeSinceLastBatchCompleted.Equals(other.WaitingTimeSinceLastBatchCompleted);
		}

		public override int GetHashCode()
		{
			unchecked
			{
				var hashCode = (Operation != null ? Operation.GetHashCode() : 0);
				hashCode = (hashCode * 397) ^ ItemsCount;
				hashCode = (hashCode * 397) ^ InputCount;
				hashCode = (hashCode * 397) ^ OutputCount;
				hashCode = (hashCode * 397) ^ Started.GetHashCode();
				hashCode = (hashCode * 397) ^ Completed.GetHashCode();
				hashCode = (hashCode * 397) ^ Duration.GetHashCode();
				hashCode = (hashCode * 397) ^ (LoadDocumentPerformance != null ? LoadDocumentPerformance.GetHashCode() : 0);
				hashCode = (hashCode * 397) ^ (LinqExecutionPerformance != null ? LinqExecutionPerformance.GetHashCode() : 0);
				hashCode = (hashCode * 397) ^ (LucenePerformance != null ? LucenePerformance.GetHashCode() : 0);
				hashCode = (hashCode * 397) ^ (MapStoragePerformance != null ? MapStoragePerformance.GetHashCode() : 0);
				hashCode = (hashCode * 397) ^ WaitingTimeSinceLastBatchCompleted.GetHashCode();
				return hashCode;
			}
		}
	}

	public class LinqExecutionPerformanceStats
	{
		public long MapLinqExecutionDurationMs { get; set; }
		public long ReduceLinqExecutionDurationMs { get; set; }

		protected bool Equals(LinqExecutionPerformanceStats other)
		{
			return MapLinqExecutionDurationMs == other.MapLinqExecutionDurationMs && ReduceLinqExecutionDurationMs == other.ReduceLinqExecutionDurationMs;
		}

		public override int GetHashCode()
		{
			unchecked
			{
				return (MapLinqExecutionDurationMs.GetHashCode()*397) ^ ReduceLinqExecutionDurationMs.GetHashCode();
			}
		}

		public override bool Equals(object obj)
		{
			if (ReferenceEquals(null, obj)) return false;
			if (ReferenceEquals(this, obj)) return true;
			if (obj.GetType() != this.GetType()) return false;
			return Equals((LinqExecutionPerformanceStats) obj);
		}
	}

	public class LoadDocumentPerformanceStats
	{
		public int LoadDocumentCount { get; set; }
		public long LoadDocumentDurationMs { get; set; }

		protected bool Equals(LoadDocumentPerformanceStats other)
		{
			return LoadDocumentCount == other.LoadDocumentCount && LoadDocumentDurationMs == other.LoadDocumentDurationMs;
		}

		public override int GetHashCode()
		{
			unchecked
			{
				return (LoadDocumentCount*397) ^ LoadDocumentDurationMs.GetHashCode();
			}
		}

		public override bool Equals(object obj)
		{
			if (ReferenceEquals(null, obj)) return false;
			if (ReferenceEquals(this, obj)) return true;
			if (obj.GetType() != this.GetType()) return false;
			return Equals((LoadDocumentPerformanceStats) obj);
		}
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

		protected bool Equals(MapStoragePerformanceStats other)
		{
			return DeleteMappedResultsDurationMs == other.DeleteMappedResultsDurationMs && PutMappedResultsDurationMs == other.PutMappedResultsDurationMs;
		}

		public override int GetHashCode()
		{
			unchecked
			{
				return (DeleteMappedResultsDurationMs.GetHashCode()*397) ^ PutMappedResultsDurationMs.GetHashCode();
			}
		}

		public override bool Equals(object obj)
		{
			if (ReferenceEquals(null, obj)) return false;
			if (ReferenceEquals(this, obj)) return true;
			if (obj.GetType() != this.GetType()) return false;
			return Equals((MapStoragePerformanceStats) obj);
		}
	}

	public class LucenePerformanceStats
	{
		public long WriteDocumentsDurationMs { get; set; }
		public long FlushToDiskDurationMs { get; set; }

		protected bool Equals(LucenePerformanceStats other)
		{
			return WriteDocumentsDurationMs == other.WriteDocumentsDurationMs && FlushToDiskDurationMs == other.FlushToDiskDurationMs;
		}

		public override int GetHashCode()
		{
			unchecked
			{
				return (WriteDocumentsDurationMs.GetHashCode()*397) ^ FlushToDiskDurationMs.GetHashCode();
			}
		}

		public override bool Equals(object obj)
		{
			if (ReferenceEquals(null, obj)) return false;
			if (ReferenceEquals(this, obj)) return true;
			if (obj.GetType() != this.GetType()) return false;
			return Equals((LucenePerformanceStats) obj);
		}	
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
