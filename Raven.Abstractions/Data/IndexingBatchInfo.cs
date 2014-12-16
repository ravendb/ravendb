using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace Raven.Abstractions.Data
{
	public class IndexingBatchInfo
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
	}

	public enum BatchType
	{
		Standard,
		Precomputed
	}
}