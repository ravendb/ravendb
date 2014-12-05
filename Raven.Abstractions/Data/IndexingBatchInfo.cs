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

        public double TotalDurationMs { get; set; }

		public double TimeSinceFirstIndexInBatchCompletedMs { get; set; }

		public ConcurrentDictionary<string, IndexingPerformanceStats> PerformanceStats { get; set; }

		public void BatchCompleted()
		{
			var now = SystemTime.UtcNow;
            TotalDurationMs = (now - StartedAt).TotalMilliseconds;

			if (PerformanceStats.Count > 0)
				TimeSinceFirstIndexInBatchCompletedMs = (now - PerformanceStats.Min(x => x.Value.Completed)).TotalMilliseconds;
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
			return BatchType == other.BatchType && Equals(IndexesToWorkOn, other.IndexesToWorkOn) && TotalDocumentCount == other.TotalDocumentCount && TotalDocumentSize == other.TotalDocumentSize && StartedAt.Equals(other.StartedAt) && TotalDurationMs.Equals(other.TotalDurationMs) && TimeSinceFirstIndexInBatchCompletedMs.Equals(other.TimeSinceFirstIndexInBatchCompletedMs) && Equals(PerformanceStats, other.PerformanceStats);
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
				hashCode = (hashCode * 397) ^ TotalDurationMs.GetHashCode();
				hashCode = (hashCode * 397) ^ TimeSinceFirstIndexInBatchCompletedMs.GetHashCode();
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