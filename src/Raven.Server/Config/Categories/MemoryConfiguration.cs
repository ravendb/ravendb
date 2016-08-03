using System.ComponentModel;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;
using Raven.Server.ServerWide.LowMemoryNotification;
using Sparrow.Logging;

namespace Raven.Server.Config.Categories
{
    public class MemoryConfiguration : ConfigurationCategory
    {
        public MemoryConfiguration(RavenConfiguration configuration)
        {
            var memoryInfo = MemoryInformation.GetMemoryInfo(configuration);

            // we allow 1 GB by default, or up to 75% of available memory on startup, if less than that is available
            LimitForProcessing = Size.Min(new Size(1024, SizeUnit.Megabytes), memoryInfo.AvailableMemory * 0.75);

            LowMemoryForLinuxDetection = Size.Min(new Size(16, SizeUnit.Megabytes), memoryInfo.AvailableMemory * 0.10);

            MemoryCacheLimit = GetDefaultMemoryCacheLimit(memoryInfo.TotalPhysicalMemory);

            AvailableMemoryForRaisingBatchSizeLimit = Size.Min(new Size(768, SizeUnit.Megabytes), memoryInfo.TotalPhysicalMemory / 2);
        }

        [Description("Maximum number of megabytes that can be used by database to control the maximum size of the processing batches.\r\n" +
                     "Default: 1024 or 75% percent of available memory if 1GB is not available.")]
        [DefaultValue(DefaultValueSetInConstructor)]
        [SizeUnit(SizeUnit.Megabytes)]
        [ConfigurationEntry("Raven/Memory/LimitForProcessingInMB")]
        [ConfigurationEntry("Raven/MemoryLimitForProcessing")]
        [ConfigurationEntry("Raven/MemoryLimitForIndexing")]
        public Size LimitForProcessing { get; set; }

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

        [Description("The minimum amount of memory available for us to double the size of Raven/InitialNumberOfItemsToProcessInSingleBatch if we need to.")]
        [DefaultValue(DefaultValueSetInConstructor)]
        [SizeUnit(SizeUnit.Megabytes)]
        [ConfigurationEntry("Raven/Memory/AvailableMemoryForRaisingBatchSizeLimitInMB")]
        [ConfigurationEntry("Raven/AvailableMemoryForRaisingBatchSizeLimit")]
        [ConfigurationEntry("Raven/AvailableMemoryForRaisingIndexBatchSizeLimit")]
        public Size AvailableMemoryForRaisingBatchSizeLimit { get; set; }

        private Size GetDefaultMemoryCacheLimit(Size totalPhysicalMemory)
        {
            if (totalPhysicalMemory < new Size(1024, SizeUnit.Megabytes))
                return new Size(128, SizeUnit.Megabytes); // if machine has less than 1024 MB, then only use 128 MB 

            // we need to leave ( a lot ) of room for other things as well, so we min the cache size
            return totalPhysicalMemory / 2;
        }
    }
}