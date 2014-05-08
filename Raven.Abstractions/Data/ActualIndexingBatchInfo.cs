using System;

namespace Raven.Abstractions.Data
{
	public class ActualIndexingBatchInfo
	{
		public int TotalDocumentCount { get; set; }

		public long TotalDocumentSize { get; set; }

		public DateTime Timestamp { get; set; }
	}
}