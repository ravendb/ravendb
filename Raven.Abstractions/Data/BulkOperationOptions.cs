using System;
using System.Threading;


namespace Raven.Abstractions.Data
{
	/// <summary>
	/// Holds different setting options for base operations.
	/// </summary>
	public class BulkOperationOptions
	{
		/// <summary>
		/// Indicates whether operations are allowed on stale indexes.
		/// </summary>
		public bool AllowStale { get; set; }

		public TimeSpan? StaleTimeout { get; set; }

		/// <summary>
		/// Limits the amount of base operation per second allowed.
		/// </summary>
		public int? MaxOpsPerSec { get; set; }

		/// <summary>
		/// Determines whether operation details about each document should be returned by server.
		/// </summary>
		public bool RetrieveDetails { get; set; }
	}
}
