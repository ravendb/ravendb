using System.ComponentModel;
using Raven.Database.Config.Attributes;
using Raven.Database.Config.Settings;

namespace Raven.Database.Config.Categories
{
    public class ReplicationConfiguration : ConfigurationCategory
    {
        [DefaultValue(600)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Raven/Replication/IndexAndTransformerReplicationLatencyInSec")]
        [ConfigurationEntry("Raven/Replication/IndexAndTransformerReplicationLatency")]
        public TimeSetting IndexAndTransformerReplicationLatency { get; set; }

        [Description("Number of seconds after which replication will stop reading documents from disk")]
        [DefaultValue(30)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Raven/Replication/FetchingFromDiskTimeoutInSec")]
        [ConfigurationEntry("Raven/Replication/FetchingFromDiskTimeout")]
        public TimeSetting FetchingFromDiskTimeoutInSeconds { get; set; }

        [Description("Number of milliseconds before replication requests will timeout")]
        [DefaultValue(60 * 1000)]
        [TimeUnit(TimeUnit.Milliseconds)]
        [ConfigurationEntry("Raven/Replication/ReplicationRequestTimeoutInMs")]
        [ConfigurationEntry("Raven/Replication/ReplicationRequestTimeout")]
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