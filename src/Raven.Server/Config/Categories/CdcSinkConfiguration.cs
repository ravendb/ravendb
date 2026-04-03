using System.ComponentModel;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;

namespace Raven.Server.Config.Categories
{
    [ConfigurationCategory(ConfigurationCategoryType.CdcSink)]
    public class CdcSinkConfiguration : ConfigurationCategory
    {
        [Description("Target number of change rows processed in a single batch before writing to the database. A batch may exceed this size when a source database transaction contains more rows, since transactions are never split across batches.")]
        [DefaultValue(1024)]
        [ConfigurationEntry("CdcSink.MaxBatchSize", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public int MaxBatchSize { get; protected set; }

        [Description("Maximum number of seconds the CDC Sink process will stay in fallback mode after a failure before retrying. Fallback duration doubles on each consecutive failure, up to this cap.")]
        [DefaultValue(60 * 15)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("CdcSink.MaxFallbackTimeInSec", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public TimeSetting MaxFallbackTime { get; protected set; }

        [Description("How frequently (in seconds) the SQL Server CDC Sink polls for new change rows. PostgreSQL uses streaming replication and ignores this setting.")]
        [DefaultValue(1)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("CdcSink.PollIntervalInSec", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public TimeSetting PollInterval { get; protected set; }
    }
}
