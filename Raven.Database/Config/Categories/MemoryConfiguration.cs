using System.ComponentModel;
using System.Runtime.Caching;
using Raven.Database.Config.Attributes;
using Raven.Database.Config.Settings;

namespace Raven.Database.Config.Categories
{
    public class MemoryConfiguration : ConfigurationCategory
    {
        public MemoryConfiguration()
        {
            // we allow 1 GB by default, or up to 75% of available memory on startup, if less than that is available
            LimitForProcessing = Size.Min(new Size(1024, SizeUnit.Megabytes), MemoryStatistics.AvailableMemory * 0.75);

            LowMemoryForLinuxDetection = Size.Min(new Size(16, SizeUnit.Megabytes), MemoryStatistics.AvailableMemory * 0.10);

            MemoryCacheLimit = GetDefaultMemoryCacheLimit();

            MemoryCacheLimitCheckInterval = new TimeSetting((long)MemoryCache.Default.PollingInterval.TotalSeconds, TimeUnit.Seconds);

            AvailableMemoryForRaisingBatchSizeLimit = Size.Min(new Size(768, SizeUnit.Megabytes), MemoryStatistics.TotalPhysicalMemory / 2);
        }

        [Description("Maximum number of megabytes that can be used by database to control the maximum size of the processing batches.\r\n" +
                     "Default: 1024 or 75% percent of available memory if 1GB is not available.")]
        [DefaultValue(DefaultValueSetInConstructor)]
        [SizeUnit(SizeUnit.Megabytes)]
        [ConfigurationEntry("Raven/Memory/LimitForProcessingInMB")]
        [ConfigurationEntry("Raven/MemoryLimitForProcessing")]
        [ConfigurationEntry("Raven/MemoryLimitForIndexing")]
        public Size LimitForProcessing { get; set; }

        public Size DynamicLimitForProcessing
        {
            get
            {
                var availableMemory = MemoryStatistics.AvailableMemory;
                var minFreeMemory = LimitForProcessing * 2L;
                // we have more memory than the twice the limit, we can use the default limit
                if (availableMemory > minFreeMemory)
                    return LimitForProcessing;

                // we don't have enough room to play with, if two databases will request the max memory limit
                // at the same time, we'll start paging because we'll run out of free memory. 
                // Because of that, we'll dynamically adjust the amount
                // of memory available for processing based on the amount of memory we actually have available,
                // assuming that we have multiple concurrent users of memory at the same time.
                // we limit that at 16 MB, if we have less memory than that, we can't really do much anyway
                return Size.Min(availableMemory / 4, new Size(16, SizeUnit.Megabytes));
            }
        }

        [Description("Limit for low mem detection in linux")]
        [DefaultValue(DefaultValueSetInConstructor)]
        [SizeUnit(SizeUnit.Megabytes)]
        [ConfigurationEntry("Raven/Memory/LowMemoryLimitForLinuxDetectionInMB")]
        [ConfigurationEntry("Raven/LowMemoryLimitForLinuxDetectionInMB")]
        public Size LowMemoryForLinuxDetection { get; set; }

        [Description("An integer value that specifies the maximum allowable size, in megabytes, that caching document instances will use")]
        [DefaultValue(DefaultValueSetInConstructor)]
        [SizeUnit(SizeUnit.Megabytes)]
        [ConfigurationEntry("Raven/Memory/MemoryCacheLimitInMB")]
        [ConfigurationEntry("Raven/MemoryCacheLimitMegabytes")]
        public Size MemoryCacheLimit { get; set; }

        [Description("The expiration value for documents in the internal managed cache")]
        [DefaultValue(360)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Raven/Memory/MemoryCacheExpirationInSec")]
        [ConfigurationEntry("Raven/MemoryCacheExpiration")]
        public TimeSetting MemoryCacheExpiration { get; set; }

        [Description("Percentage of physical memory used for caching. Allowed values: 0-99 (0 = autosize)")]
        [DefaultValue(0 /* auto size */)]
        [ConfigurationEntry("Raven/Memory/MemoryCacheLimitPercentage")]
        [ConfigurationEntry("Raven/MemoryCacheLimitPercentage")]
        public int MemoryCacheLimitPercentage { get; set; }

        [Description("Interval for checking the memory cache limits. Allowed values: max precision is 1 second\r\n" +
                     "Default: 00:02:00 (or value provided by system.runtime.caching app config)")]
        [DefaultValue(DefaultValueSetInConstructor)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Raven/Memory/MemoryCacheLimitCheckIntervalInSec")]
        [ConfigurationEntry("Raven/MemoryCacheLimitCheckInterval")]
        public TimeSetting MemoryCacheLimitCheckInterval { get; set; }

        [Description("The minimum amount of memory available for us to double the size of Raven/InitialNumberOfItemsToProcessInSingleBatch if we need to.")]
        [DefaultValue(DefaultValueSetInConstructor)]
        [SizeUnit(SizeUnit.Megabytes)]
        [ConfigurationEntry("Raven/Memory/AvailableMemoryForRaisingBatchSizeLimitInMB")]
        [ConfigurationEntry("Raven/AvailableMemoryForRaisingBatchSizeLimit")]
        [ConfigurationEntry("Raven/AvailableMemoryForRaisingIndexBatchSizeLimit")]
        public Size AvailableMemoryForRaisingBatchSizeLimit { get; set; }

        private Size GetDefaultMemoryCacheLimit()
        {
            if (MemoryStatistics.TotalPhysicalMemory < new Size(1024, SizeUnit.Megabytes))
                return new Size(128, SizeUnit.Megabytes); // if machine has less than 1024 MB, then only use 128 MB 

            // we need to leave ( a lot ) of room for other things as well, so we min the cache size
            return MemoryStatistics.TotalPhysicalMemory / 2;
        }
    }
}