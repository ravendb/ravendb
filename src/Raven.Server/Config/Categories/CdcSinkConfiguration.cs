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

        [Description("How frequently (in seconds) the SQL Server CDC Sink polls for new change rows. Lower values reduce latency but increase load on the source database.")]
        [DefaultValue(1)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("CdcSink.SqlServer.PollIntervalInSec", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public TimeSetting PollInterval { get; protected set; }

        [Description("Timeout (in seconds) for the PostgreSQL replication connection. Controls both the server-side wal_sender_timeout (keepalives arrive at roughly half this interval) and the client-side WalReceiverTimeout. Lower values detect dead connections faster but increase keepalive traffic. SQL Server and MySQL ignore this setting.")]
        [DefaultValue(10)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("CdcSink.Postgres.ReplicationTimeoutInSec", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public TimeSetting PostgresReplicationTimeout { get; protected set; }
    }
}
