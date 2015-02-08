using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace Raven.Abstractions.Data
{
	public class IndexingBatchInfo
	{
		/// <summary>
		/// Type of batch (Standard, Precomputed).
		/// </summary>
		public BatchType BatchType { get; set; }

		/// <summary>
		/// List of indexes (names) that processed this batch.
		/// </summary>
		public List<string> IndexesToWorkOn { get; set; }

		/// <summary>
		/// Total count of documents in batch.
		/// </summary>
		public int TotalDocumentCount { get; set; }

		/// <summary>
		/// Total size of documents in batch (in bytes).
		/// </summary>
		public long TotalDocumentSize { get; set; }
		
		/// <summary>
		/// Batch processing start time.
		/// </summary>
		public DateTime StartedAt { get; set; }

		/// <summary>
		/// Total batch processing time in milliseconds.
		/// </summary>
        public double TotalDurationMs { get; set; }

		/// <summary>
		/// Time (in milliseconds) that passed since first index completed the batch to full batch completion.
		/// </summary>
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