using System;
using System.Collections.Generic;
using Raven.Abstractions.RavenFS;

namespace Raven.Abstractions.Data
{
    public class AdminStatistics
    {
        public string ServerName { get; set; }
        public int TotalNumberOfRequests { get; set; }
        public TimeSpan Uptime { get; set; }
        public AdminMemoryStatistics Memory { get; set; }

        public IEnumerable<LoadedDatabaseStatistics> LoadedDatabases { get; set; }
        public IEnumerable<FileSystemStats> LoadedFileSystems { get; set; }
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
        public long TransactionalStorageAllocatedSize { get; set; }
        public string TransactionalStorageAllocatedSizeHumaneSize { get; set; }
        public long TransactionalStorageUsedSize { get; set; }
        public string TransactionalStorageUsedSizeHumaneSize { get; set; }
        public long IndexStorageSize { get; set; }
        public string IndexStorageHumaneSize { get; set; }
        public long TotalDatabaseSize { get; set; }
        public string TotalDatabaseHumaneSize { get; set; }
        public long CountOfDocuments { get; set; }
        public long CountOfAttachments { get; set; }
        public decimal DatabaseTransactionVersionSizeInMB { get; set; }
        public DatabaseMetrics Metrics { get; set; }
    }
}