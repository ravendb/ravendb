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
	//	public double RequestsPerSecond { get; set; }
		//public int ConcurrentRequests { get; set; }
		public decimal DatabaseTransactionVersionSizeInMB { get; set; }

        //public double MeanRate { get; set; }
        //public double OneMinuteRate { get; set; }
        //public long Count { get; set; }

        //public double FiveMinuteRate { get; set; }
        //public double FifteenMinuteRate { get; set; }

        //public double CounterRequestsPerSecond { get; set; }

        //public long DocsPerSecondCounter { get;  set; }

        //public long IndexedPerSecondCounter { get;  set; }

        //public long ReducedPerSecondCounter { get;  set; }

        //public MeterMetric ConcurrentRequests { get; private set; }  //!!

        //public MeterMetric RequestsMeter { get; private set; }
        //public PerSecondCounterMetric RequestsPerSecondCounter { get; private set; }
        //public HistogramMetric RequestsPerSecondHistogram { get; private set; }
        public FullMetricsData MetricsData { get; set; }

	}

    public class FullMetricsData
    {
        public double MeanRate { get; set; }
        public double OneMinuteRate { get; set; }
        public long Count { get; set; }

        public double FiveMinuteRate { get; set; }
        public double FifteenMinuteRate { get; set; }

        public double CounterRequestsPerSecond { get; set; }

        public long DocsPerSecondCounter { get; set; }

        public long IndexedPerSecondCounter { get; set; }

        public long ReducedPerSecondCounter { get; set; }

      //  public HistogramMetric RequestsPerSecondHistogram { get; set; }
        //Histogram data
        public long HistCounter { get; set; }
        public double HistMax { get; set; }
        public double HistMin { get; set; }
        public double HistMean { get; set; }
        public double HistStdev { get; set; }
        public double HistVar { get; set; }
        public double[] HistPercentiles { get; set; }

         
	       
         
              
   
    }

}