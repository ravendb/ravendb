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

        [Description("Minimal time in milliseconds before sending another heartbeat")]
        [DefaultValue(15 * 1000)]
        [TimeUnit(TimeUnit.Milliseconds)]
        [ConfigurationEntry("Raven/Replication/ReplicationMinimalHeartbeat")]
        public TimeSetting ReplicationMinimalHeartbeat { get; set; }

        [Description("Number of seconds before replication topology discovery requests will timeout")]
        [DefaultValue(60)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Raven/Replication/ReplicationTopologyDiscoveryTimeoutInSec")]
        public TimeSetting ReplicationTopologyDiscoveryTimeout { get; set; }

        [Description("Force us to buffer replication requests (useful if using windows auth under certain scenarios)")]
        [DefaultValue(false)]
        [ConfigurationEntry("Raven/Replication/ForceReplicationRequestBuffering")]
        public bool ForceReplicationRequestBuffering { get; set; }

        [Description("Maximum number of items replication will receive in single batch, null means let source server decide")]
        [DefaultValue(null)]
        [MinValue(512)]
        [ConfigurationEntry("Raven/Replication/MaxNumberOfItemsToReceiveInSingleBatch")]
        public int? MaxNumberOfItemsToReceiveInSingleBatch { get; set; }

        [Description("Maximum number of items replication will send in single batch, null means we will not cut the batch by number of items")]
        [DefaultValue(1024)]
        [ConfigurationEntry("Raven/Replication/MaxItemsCount")]
        public int? MaxItemsCount { get; set; }

        [Description("Maximum number of data size replication will send in single batch, null means we will not cut the batch by the size")]
        [DefaultValue(16 * 1024 * 1024)]
        [ConfigurationEntry("Raven/Replication/MaxSizeToSend")]
        public int? MaxSizeToSend { get; set; }
    }
}