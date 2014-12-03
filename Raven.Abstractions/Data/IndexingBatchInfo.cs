using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace Raven.Abstractions.Data
{
	public class IndexingBatchInfo : IEquatable<IndexingBatchInfo>
	{
		public BatchType BatchType { get; set; }

		public List<string> IndexesToWorkOn { get; set; }

		public int TotalDocumentCount { get; set; }

		public long TotalDocumentSize { get; set; }

		public DateTime StartedAt { get; set; }

		public TimeSpan TotalDuration { get; set; }

		public TimeSpan TimeSinceFirstIndexInBatchCompleted { get; set; }

		public ConcurrentDictionary<string, IndexingPerformanceStats> PerformanceStats { get; set; }

		public void BatchCompleted()
		{
			var now = SystemTime.UtcNow;
			TotalDuration = now - StartedAt;

			if (PerformanceStats.Count > 0)
				TimeSinceFirstIndexInBatchCompleted = now - PerformanceStats.Min(x => x.Value.Completed);
		}

		public override bool Equals(object obj)
		{
			if (ReferenceEquals(null, obj)) return false;
			if (ReferenceEquals(this, obj)) return true;
			if (obj.GetType() != GetType()) return false;
			return Equals((IndexingBatchInfo) obj);
		}

		public bool Equals(IndexingBatchInfo other)
		{
			if (ReferenceEquals(null, other))
				return false;
			if (ReferenceEquals(this, other))
				return true;
			return BatchType == other.BatchType && Equals(IndexesToWorkOn, other.IndexesToWorkOn) && TotalDocumentCount == other.TotalDocumentCount && TotalDocumentSize == other.TotalDocumentSize && StartedAt.Equals(other.StartedAt) && TotalDuration.Equals(other.TotalDuration) && TimeSinceFirstIndexInBatchCompleted.Equals(other.TimeSinceFirstIndexInBatchCompleted) && Equals(PerformanceStats, other.PerformanceStats);
		}

		public override int GetHashCode()
		{
			unchecked
			{
				int hashCode = (int)BatchType;
				hashCode = (hashCode * 397) ^ (IndexesToWorkOn != null ? IndexesToWorkOn.GetHashCode() : 0);
				hashCode = (hashCode * 397) ^ TotalDocumentCount;
				hashCode = (hashCode * 397) ^ TotalDocumentSize.GetHashCode();
				hashCode = (hashCode * 397) ^ StartedAt.GetHashCode();
				hashCode = (hashCode * 397) ^ TotalDuration.GetHashCode();
				hashCode = (hashCode * 397) ^ TimeSinceFirstIndexInBatchCompleted.GetHashCode();
				hashCode = (hashCode * 397) ^ (PerformanceStats != null ? PerformanceStats.GetHashCode() : 0);
				return hashCode;
			}
		}
	}

	public enum BatchType
	{
		Standard,
		Precomputed
	}
}