using System;
using System.Collections.Generic;
using Raven.Abstractions.FileSystem;

namespace Raven.Abstractions.Data
{
    /// <summary>
    /// Server-wide statistics that contain memory usage and loaded resources information.
    /// </summary>
    public class AdminStatistics
    {
        /// <summary>
        /// Name of a server.
        /// </summary>
        public string ServerName { get; set; }

        /// <summary>
        /// Total number of requests received since server startup.
        /// </summary>
        public int TotalNumberOfRequests { get; set; }

        /// <summary>
        /// Server uptime.
        /// </summary>
        public TimeSpan Uptime { get; set; }

        /// <summary>
        /// Current memory statistics.
        /// </summary>
        public AdminMemoryStatistics Memory { get; set; }

        /// <summary>
        /// List of loaded databases with their statistics.
        /// </summary>
        public IEnumerable<LoadedDatabaseStatistics> LoadedDatabases { get; set; }

        /// <summary>
        /// List of loaded filesystems with their statistics.
        /// </summary>
        public IEnumerable<FileSystemStats> LoadedFileSystems { get; set; }
    }

    public class AdminMemoryStatistics
    {
        /// <summary>
        /// Size of database cache in megabytes.
        /// </summary>
        public decimal DatabaseCacheSizeInMB { get; set; }

        /// <summary>
        /// Size (in megabytes) of managed memory held by server.
        /// </summary>
        public decimal ManagedMemorySizeInMB { get; set; }

        /// <summary>
        /// Total size of memory held by server.
        /// </summary>
        public decimal TotalProcessMemorySizeInMB { get; set; }
    }

    public class LoadedDatabaseStatistics
    {
        /// <summary>
        /// Name of database
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Time of last activity on this database
        /// </summary>
        public DateTime LastActivity { get; set; }

        /// <summary>
        /// Size (allocated) of a transactional storage in bytes.
        /// </summary>
        public long TransactionalStorageAllocatedSize { get; set; }

        /// <summary>
        /// Size (allocated) of a transactional storage in a more human readable format.
        /// </summary>
        public string TransactionalStorageAllocatedSizeHumaneSize { get; set; }

        /// <summary>
        /// Size (used) of a transactional storage in bytes.
        /// </summary>
        public long TransactionalStorageUsedSize { get; set; }

        /// <summary>
        /// Size (used) of a transactional storage in a more human readable format.
        /// </summary>
        public string TransactionalStorageUsedSizeHumaneSize { get; set; }

        /// <summary>
        /// Size of a index storage in bytes.
        /// </summary>
        public long IndexStorageSize { get; set; }

        /// <summary>
        /// Size of a index storage in a more human readable format.
        /// </summary>
        public string IndexStorageHumaneSize { get; set; }

        /// <summary>
        /// Total database size in bytes.
        /// </summary>
        public long TotalDatabaseSize { get; set; }

        /// <summary>
        /// Total database size in a more human readable format.
        /// </summary>
        public string TotalDatabaseHumaneSize { get; set; }

        /// <summary>
        /// Total count of documents in database.
        /// </summary>
        public long CountOfDocuments { get; set; }

        /// <summary>
        /// Transaction version size in megabytes for database.
        /// </summary>
        public decimal DatabaseTransactionVersionSizeInMB { get; set; }

        /// <summary>
        /// Database metrics.
        /// </summary>
        public DatabaseMetrics Metrics { get; set; }

        /// <summary>
        /// Database storage statistics.
        /// </summary>
        public StorageStats StorageStats { get; set; }
    }
}
