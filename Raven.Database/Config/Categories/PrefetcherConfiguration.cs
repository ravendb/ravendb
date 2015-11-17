using System.ComponentModel;
using Raven.Database.Config.Attributes;
using Raven.Database.Config.Settings;

namespace Raven.Database.Config.Categories
{
    public class PrefetcherConfiguration : ConfigurationCategory
    {
        public PrefetcherConfiguration()
        {
            MaxNumberOfItemsToPreFetch = CoreConfiguration.DefaultMaxNumberOfItemsToProcessInSingleBatch;
        }

        [DefaultValue(5000)]
        [TimeUnit(TimeUnit.Milliseconds)]
        [ConfigurationEntry("Raven/Prefetching/DurationLimitInMs")]
        [ConfigurationEntry("Raven/Prefetching/DurationLimit")]
        public TimeSetting DurationLimit { get; set; }

        [Description("Disable document prefetching")]
        [DefaultValue(false)]
        [ConfigurationEntry("Raven/Prefetching/Disable")]
        [ConfigurationEntry("Raven/DisableDocumentPreFetching")]
        [ConfigurationEntry("Raven/DisableDocumentPreFetchingForIndexing")]
        public bool Disabled { get; set; }

        [DefaultValue(DefaultValueSetInConstructor)]
        [MinValue(128)]
        [ConfigurationEntry("Raven/Prefetching/MaxNumberOfItemsToPreFetch")]
        [ConfigurationEntry("Raven/MaxNumberOfItemsToPreFetch")]
        [ConfigurationEntry("Raven/MaxNumberOfItemsToPreFetchForIndexing")]
        public int MaxNumberOfItemsToPreFetch { get; set; }

        [Description("Number of seconds after which prefetcher will stop reading documents from disk")]
        [DefaultValue(5)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Raven/Prefetching/FetchingDocumentsFromDiskTimeoutInSec")]
        [ConfigurationEntry("Raven/Prefetcher/FetchingDocumentsFromDiskTimeout")]
        public TimeSetting FetchingDocumentsFromDiskTimeout { get; set; }

        [Description("Maximum number of megabytes after which prefetcher will stop reading documents from disk")]
        [DefaultValue(256)]
        [SizeUnit(SizeUnit.Megabytes)]
        [ConfigurationEntry("Raven/Prefetching/MaximumSizeAllowedToFetchFromStorageInMB")]
        [ConfigurationEntry("Raven/Prefetcher/MaximumSizeAllowedToFetchFromStorage")]
        public Size MaximumSizeAllowedToFetchFromStorageInMb { get; set; }
    }
}