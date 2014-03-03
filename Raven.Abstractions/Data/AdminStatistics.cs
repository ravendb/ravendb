using System;
using System.Collections.Generic;

namespace Raven.Abstractions.Data
{
    public class AdminStatistics
    {
        public string ServerName { get; set; }
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
<<<<<<< HEAD

        public double DocsWritesPerSecond { get; set; }

        public double IndexedPerSecond { get; set; }

        public double ReducedPerSecond { get; set; }
        public double RequestsPerSecond { get; set; }

        public MeterData Requests { get; set; }

        public class HistogramData
        {
            public long Counter { get; set; }
            public double Max { get; set; }
            public double Min { get; set; }
            public double Mean { get; set; }
            public double Stdev { get; set; }
            public double Var { get; set; }
            public Dictionary<string, double> Percentiles { get; set; }

            public HistogramData()
            {
                Percentiles = new Dictionary<string, double>();
            }
        }

        public class MeterData
        {
            public long Count { get; set; }
            public double MeanRate { get; set; }
            public double OneMinuteRate { get; set; }
            public double FiveMinuteRate { get; set; }
            public double FifteenMinuteRate { get; set; }

        }
=======
        public DatabaseMetrics Metrics { get; set; }
>>>>>>> upstream/new3
    }
}