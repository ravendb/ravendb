using System.ComponentModel;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;
using Sparrow;

namespace Raven.Server.Config.Categories
{
    [ConfigurationCategory(ConfigurationCategoryType.Etl)]
    public class EtlConfiguration : ConfigurationCategory
    {
        [Description("Number of seconds after which SQL command will timeout. Default: null (use provider default). Can be overriden by setting CommandTimeout property value in SQL ETL configuration.")]
        [DefaultValue(null)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("ETL.SQL.CommandTimeoutInSec", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public TimeSetting? SqlCommandTimeout { get; set; }

        [Description("Number of seconds after which extraction and transformation will end and loading will start.")]
        [DefaultValue(30)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("ETL.ExtractAndTransformTimeoutInSec", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public TimeSetting ExtractAndTransformTimeout { get; protected set; }

        [Description("Max number of extracted documents in ETL batch")]
        [DefaultValue(8192)]
        [ConfigurationEntry("ETL.MaxNumberOfExtractedDocuments", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public int? MaxNumberOfExtractedDocuments { get; protected set; }

        [Description("Max number of extracted items in ETL batch")]
        [DefaultValue(8192)]
        [ConfigurationEntry("ETL.MaxNumberOfExtractedItems", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public int? MaxNumberOfExtractedItems { get; protected set; }

        [Description("Maximum number of seconds ETL process will be in a fallback mode after a load connection failure to a destination. The fallback mode means suspending the process.")]
        [DefaultValue(60 * 15)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("ETL.MaxFallbackTimeInSec", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public TimeSetting MaxFallbackTime { get; set; }

        [Description("Maximum batch size of data (documents and attachments) in megabytes after transformation that will be sent to the destination in a single batch")]
        [DefaultValue(64)]
        [SizeUnit(SizeUnit.Megabytes)]
        [ConfigurationEntry("ETL.MaxBatchSizeInMb", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public Size? MaxBatchSize { get; protected set; }

        [Description("Max number of extracted documents in OLAP ETL batch")]
        [DefaultValue(65536)]
        [ConfigurationEntry("ETL.OLAP.MaxNumberOfExtractedDocuments", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public int? OlapMaxNumberOfExtractedDocuments { get; protected set; }
    }
}
