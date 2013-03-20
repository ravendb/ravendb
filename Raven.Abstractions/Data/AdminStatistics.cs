using System;
using System.Collections.Generic;

namespace Raven.Abstractions.Data
{
	public class AdminStatistics
	{
		public string ServerName { get; set; }
		public string ClusterName { get; set; }

		public int TotalNumberOfRequests { get; set; }
		public TimeSpan Uptime { get; set; }
		public AdminMemoryStatistics Memory { get; set; }

		public IEnumerable<LoadedDatabaseStatistics> LoadedDatabases { get; set; }
	}

	public class AdminMemoryStatistics
	{
		public decimal DatabaseCacheSizeInMB { get; set; }
		public decimal ManagedMemorySizeInMB { get; set; }
		public decimal TotalProcessMemorySizeInMB { get; set; }
	}

	public class LoadedDatabaseStatistics
	{
		public string Name { get; set; }
		public DateTime LastActivity { get; set; }
		public long TransactionalStorageSize { get; set; }
		public string TransactionalStorageSizeHumaneSize { get; set; }
		public long IndexStorageSize { get; set; }
		public string IndexStorageHumaneSize { get; set; }
		public long TotalDatabaseSize { get; set; }
		public string TotalDatabaseHumaneSize { get; set; }
		public long CountOfDocuments { get; set; }
		public double RequestsPerSecond { get; set; }
		public int ConcurrentRequests { get; set; }
		public decimal DatabaseTransactionVersionSizeInMB { get; set; }
	}
}