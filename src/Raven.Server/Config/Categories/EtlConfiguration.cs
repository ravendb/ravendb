using System;
using System.Collections.Generic;
using System.ComponentModel;
using Microsoft.Extensions.Configuration;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;
using Raven.Server.Documents.ETL;
using Raven.Server.ServerWide;
using Sparrow;

namespace Raven.Server.Config.Categories
{
    [ConfigurationCategory(ConfigurationCategoryType.Etl)]
    public sealed class EtlConfiguration : ConfigurationCategory
    {
        [Description("Number of seconds after which Raven ETL load request will time out. Default: 300 seconds. Can be overridden by setting the LoadRequestTimeoutInSec property in the Raven ETL configuration.")]
        [DefaultValue(300)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("ETL.Raven.LoadRequestTimeoutInSec", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public TimeSetting RavenLoadRequestTimeout { get; set; }

        [Description("Number of seconds after which SQL command will time out. Default: null (use provider default). Can be overridden by setting the CommandTimeout property in the SQL ETL configuration.")]
        [DefaultValue(null)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("ETL.SQL.CommandTimeoutInSec", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public TimeSetting? SqlCommandTimeout { get; set; }

        [Description("Number of seconds after which extraction and transformation will end and loading will start.")]
        [DefaultValue(30)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("ETL.ExtractAndTransformTimeoutInSec", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public TimeSetting ExtractAndTransformTimeout { get; set; }

        [Description("Max number of extracted documents in ETL batch")]
        [DefaultValue(8192)]
        [ConfigurationEntry("ETL.MaxNumberOfExtractedDocuments", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public int? MaxNumberOfExtractedDocuments { get; set; }

        [Description("Max number of extracted items in ETL batch")]
        [DefaultValue(8192)]
        [ConfigurationEntry("ETL.MaxNumberOfExtractedItems", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public int? MaxNumberOfExtractedItems { get; set; }

        [Description("Maximum number of seconds ETL process will be in a fallback mode after a load connection failure to a destination. The fallback mode means suspending the process.")]
        [DefaultValue(60 * 15)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("ETL.MaxFallbackTimeInSec", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public TimeSetting MaxFallbackTime { get; set; }

        [Description("Maximum batch size of data (documents and attachments) in megabytes after transformation that will be sent to the destination in a single batch")]
        [DefaultValue(64)]
        [SizeUnit(SizeUnit.Megabytes)]
        [ConfigurationEntry("ETL.MaxBatchSizeInMb", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public Size? MaxBatchSize { get; set; }

        [Description("Max number of extracted documents in OLAP ETL batch")]
        [DefaultValue(64 * 1024)]
        [ConfigurationEntry("ETL.OLAP.MaxNumberOfExtractedDocuments", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public int? OlapMaxNumberOfExtractedDocuments { get; set; }

        [Description("Timeout to initialize transactions for the Kafka producer")]
        [DefaultValue(60)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("ETL.Queue.Kafka.InitTransactionsTimeoutInSec", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public TimeSetting KafkaInitTransactionsTimeout { get; set; }
        
        [Description("Lifespan of a message in the queue")]
        [DefaultValue(604800)] // 7 days (Azure default)
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("ETL.Queue.AzureQueueStorage.TimeToLiveInSec", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public TimeSetting AzureQueueStorageTimeToLive{ get; set; }
        
        [Description("How long a message is hidden after being retrieved but not deleted")]
        [DefaultValue(0)] // Azure default
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("ETL.Queue.AzureQueueStorage.VisibilityTimeoutInSec", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public TimeSetting AzureQueueStorageVisibilityTimeout{ get; set; }
        
        [Description($"Weighted EWMA ratio threshold of errored items to successfully processed items above which the process health status will be set to '{nameof(EtlProcessHealthStatus.Failed)}'")]
        [DefaultValue(0.9f)]
        [ConfigurationEntry("ETL.ProcessHealthStatusFailedThreshold", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public float ProcessHealthStatusFailedThreshold { get; set; }
        
        [Description($"Weighted ratio threshold of errored items to successfully processed items above which the process health status will be set to '{nameof(EtlProcessHealthStatus.Impaired)}'")]
        [DefaultValue(0.1f)]
        [ConfigurationEntry("ETL.ProcessHealthStatusImpairedThreshold", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public float ProcessHealthStatusImpairedThreshold { get; set; }

        public override void Initialize(IConfigurationRoot settings, HashSet<string> settingsNames, IConfigurationRoot serverWideSettings, HashSet<string> serverWideSettingsNames, ResourceType type, string resourceName)
        {
            base.Initialize(settings, settingsNames, serverWideSettings, serverWideSettingsNames, type, resourceName);

            if (ProcessHealthStatusFailedThreshold is < 0f or > 1f)
            {
                throw new InvalidOperationException(
                    $"The value of '{RavenConfiguration.GetKey(x => x.Etl.ProcessHealthStatusFailedThreshold)}' ({ProcessHealthStatusFailedThreshold}) must be between 0 and 1.");
            }

            if (ProcessHealthStatusImpairedThreshold is < 0f or > 1f)
            {
                throw new InvalidOperationException(
                    $"The value of '{RavenConfiguration.GetKey(x => x.Etl.ProcessHealthStatusImpairedThreshold)}' ({ProcessHealthStatusImpairedThreshold}) must be between 0 and 1.");
            }

            if (ProcessHealthStatusFailedThreshold <= ProcessHealthStatusImpairedThreshold)
            {
                throw new InvalidOperationException(
                    $"The value of '{RavenConfiguration.GetKey(x => x.Etl.ProcessHealthStatusFailedThreshold)}' ({ProcessHealthStatusFailedThreshold}) must be greater than " +
                    $"the value of '{RavenConfiguration.GetKey(x => x.Etl.ProcessHealthStatusImpairedThreshold)}' ({ProcessHealthStatusImpairedThreshold}).");
            }
        }
    }
}
