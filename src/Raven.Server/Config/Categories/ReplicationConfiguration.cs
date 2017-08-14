using System.ComponentModel;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;
using Sparrow;

namespace Raven.Server.Config.Categories
{
    public class ReplicationConfiguration : ConfigurationCategory
    {
        [Description("Threshold under which an incoming replication connection is considered active. If an incoming connection receives messages within this time-span, new connection coming from the same source would be rejected (as the existing connection is considered active)")]
        [DefaultValue(30)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Replication.ActiveConnectionTimeoutInSec")]
        public TimeSetting ActiveConnectionTimeout { get; set; }
        
        [Description("Minimal time in milliseconds before sending another heartbeat")]
        [DefaultValue(15)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Replication.ReplicationMinimalHeartbeatInSec")]
        public TimeSetting ReplicationMinimalHeartbeat { get; set; }
        
        [Description("Maximum number of items replication will send in single batch, null means we will not cut the batch by number of items")]
        [DefaultValue(16*1024)]
        [ConfigurationEntry("Replication.MaxItemsCount")]
        public int? MaxItemsCount { get; set; }

        [Description("Maximum number of data size replication will send in single batch, null means we will not cut the batch by the size")]
        [DefaultValue(64)]
        [SizeUnit(SizeUnit.Megabytes)]
        [ConfigurationEntry("Replication.MaxSizeToSendInMb")]
        public Size? MaxSizeToSend { get; set; }
    }
}