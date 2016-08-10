using System.ComponentModel;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;

namespace Raven.Server.Config.Categories
{
    public class ReplicationConfiguration : ConfigurationCategory
    {
        [Description("Threshold under which an incoming replication connection is considered active. If an incoming connection receives messages within this time-span, new connection coming from the same source would be rejected (as the existing connection is considered active)")]
        [DefaultValue(30)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Raven/Replication/ActiveConnectionTimeout")]
        public TimeSetting ActiveConnectionTimeout { get; set; }

        [DefaultValue(600)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Raven/Replication/IndexAndTransformerReplicationLatencyInSec")]
        [LegacyConfigurationEntry("Raven/Replication/IndexAndTransformerReplicationLatency")]
        public TimeSetting IndexAndTransformerReplicationLatency { get; set; }

        [Description("Number of seconds after which replication will stop reading documents from disk")]
        [DefaultValue(30)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Raven/Replication/FetchingFromDiskTimeoutInSec")]
        [LegacyConfigurationEntry("Raven/Replication/FetchingFromDiskTimeout")]
        public TimeSetting FetchingFromDiskTimeoutInSeconds { get; set; }

        [Description("Number of milliseconds before replication requests will timeout")]
        [DefaultValue(60 * 1000)]
        [TimeUnit(TimeUnit.Milliseconds)]
        [ConfigurationEntry("Raven/Replication/ReplicationRequestTimeoutInMs")]
        [LegacyConfigurationEntry("Raven/Replication/ReplicationRequestTimeout")]
        public TimeSetting ReplicationRequestTimeout { get; set; }

        [Description("Force us to buffer replication requests (useful if using windows auth under certain scenarios)")]
        [DefaultValue(false)]
        [ConfigurationEntry("Raven/Replication/ForceReplicationRequestBuffering")]
        public bool ForceReplicationRequestBuffering { get; set; }

        [Description("Maximum number of items replication will receive in single batch, null means let source server decide")]
        [DefaultValue(null)]
        [MinValue(512)]
        [ConfigurationEntry("Raven/Replication/MaxNumberOfItemsToReceiveInSingleBatch")]
        public int? MaxNumberOfItemsToReceiveInSingleBatch { get; set; }
    }
}