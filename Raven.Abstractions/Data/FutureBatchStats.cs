using System;

namespace Raven.Abstractions.Data
{
	public class FutureBatchStats
	{
		public DateTime Timestamp { get; set; }
		public TimeSpan? Duration { get; set; }
		public int? Size { get; set; }
		public int Retries { get; set; }
	}
}