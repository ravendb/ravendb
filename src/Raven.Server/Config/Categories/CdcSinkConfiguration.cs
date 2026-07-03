using Raven.Server.Documents.TasksErrors;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using Microsoft.Extensions.Configuration;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;
using Raven.Server.Documents.ETL;
using Raven.Server.ServerWide;

namespace Raven.Server.Config.Categories
{
    [ConfigurationCategory(ConfigurationCategoryType.CdcSink)]
    public class CdcSinkConfiguration : ConfigurationCategory
    {
        [Description($"Weighted ratio threshold of errored items to successfully processed items above which the process health status will be set to '{nameof(OngoingTaskHealthStatus.Failed)}'")]
        [DefaultValue(0.9f)]
        [ConfigurationEntry("CdcSink.ProcessHealthStatusFailedThreshold", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public float ProcessHealthStatusFailedThreshold { get; protected set; }

        [Description($"Weighted ratio threshold of errored items to successfully processed items above which the process health status will be set to '{nameof(OngoingTaskHealthStatus.Impaired)}'")]
        [DefaultValue(0.1f)]
        [ConfigurationEntry("CdcSink.ProcessHealthStatusImpairedThreshold", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public float ProcessHealthStatusImpairedThreshold { get; protected set; }

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

        public override void Initialize(IConfigurationRoot settings, HashSet<string> settingsNames, IConfigurationRoot serverWideSettings, HashSet<string> serverWideSettingsNames, ResourceType type, string resourceName)
        {
            base.Initialize(settings, settingsNames, serverWideSettings, serverWideSettingsNames, type, resourceName);

            if (ProcessHealthStatusFailedThreshold is < 0f or > 1f)
            {
                throw new InvalidOperationException(
                    $"The value of '{RavenConfiguration.GetKey(x => x.CdcSink.ProcessHealthStatusFailedThreshold)}' ({ProcessHealthStatusFailedThreshold}) must be between 0 and 1.");
            }

            if (ProcessHealthStatusImpairedThreshold is < 0f or > 1f)
            {
                throw new InvalidOperationException(
                    $"The value of '{RavenConfiguration.GetKey(x => x.CdcSink.ProcessHealthStatusImpairedThreshold)}' ({ProcessHealthStatusImpairedThreshold}) must be between 0 and 1.");
            }

            if (ProcessHealthStatusFailedThreshold <= ProcessHealthStatusImpairedThreshold)
            {
                throw new InvalidOperationException(
                    $"The value of '{RavenConfiguration.GetKey(x => x.CdcSink.ProcessHealthStatusFailedThreshold)}' ({ProcessHealthStatusFailedThreshold}) must be greater than " +
                    $"the value of '{RavenConfiguration.GetKey(x => x.CdcSink.ProcessHealthStatusImpairedThreshold)}' ({ProcessHealthStatusImpairedThreshold}).");
            }
        }
    }
}
