// -----------------------------------------------------------------------
//  <copyright file="ReducingBatchInfo.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace Raven.Abstractions.Data
{
	public class ReducingBatchInfo : IEquatable<ReducingBatchInfo>
	{
		public List<string> IndexesToWorkOn { get; set; }

		public DateTime StartedAt { get; set; }

		public double TotalDurationMs { get; set; }

		public double TimeSinceFirstReduceInBatchCompletedMs { get; set; }

		public ConcurrentDictionary<string, IndexingPerformanceStats> PerformanceStats { get; set; }

		public void BatchCompleted()
		{
			var now = SystemTime.UtcNow;
			TotalDurationMs = (now - StartedAt).TotalMilliseconds;

			if (PerformanceStats.Count > 0)
				TimeSinceFirstReduceInBatchCompletedMs = (now - PerformanceStats.Min(x => x.Value.Completed)).TotalMilliseconds;
		}

		public override bool Equals(object obj)
		{
			if (ReferenceEquals(null, obj)) return false;
			if (ReferenceEquals(this, obj)) return true;
			if (obj.GetType() != this.GetType()) return false;
			return Equals((ReducingBatchInfo) obj);
		}

		public bool Equals(ReducingBatchInfo other)
		{
			if (ReferenceEquals(null, other))
				return false;
			if (ReferenceEquals(this, other))
				return true;
			return Equals(IndexesToWorkOn, other.IndexesToWorkOn) && StartedAt.Equals(other.StartedAt) && TotalDurationMs.Equals(other.TotalDurationMs) && TimeSinceFirstReduceInBatchCompletedMs.Equals(other.TimeSinceFirstReduceInBatchCompletedMs) && Equals(PerformanceStats, other.PerformanceStats);
		}

		public override int GetHashCode()
		{
			unchecked
			{
				var hashCode = (IndexesToWorkOn != null ? IndexesToWorkOn.GetHashCode() : 0);
				hashCode = (hashCode * 397) ^ StartedAt.GetHashCode();
				hashCode = (hashCode * 397) ^ TotalDurationMs.GetHashCode();
				hashCode = (hashCode * 397) ^ TimeSinceFirstReduceInBatchCompletedMs.GetHashCode();
				hashCode = (hashCode * 397) ^ (PerformanceStats != null ? PerformanceStats.GetHashCode() : 0);
				return hashCode;
			}
		}
	}
}