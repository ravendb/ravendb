using System.ComponentModel;
using Rachis;
using Raven.Database.Config.Attributes;
using Raven.Database.Config.Settings;

namespace Raven.Database.Config.Categories
{
    public class ClusterConfiguration : ConfigurationCategory
    {
        public ClusterConfiguration()
        {
            MaxStepDownDrainTime = new TimeSetting((long)RaftEngineOptions.DefaultMaxStepDownDrainTime.TotalSeconds, TimeUnit.Seconds);
        }

        [DefaultValue(RaftEngineOptions.DefaultElectionTimeout * 5)] // 6000ms
        [TimeUnit(TimeUnit.Milliseconds)]
        [ConfigurationEntry("Raven/Cluster/ElectionTimeoutInMs")]
        [ConfigurationEntry("Raven/Cluster/ElectionTimeout")]
        public TimeSetting ElectionTimeout { get; set; }

        [DefaultValue(RaftEngineOptions.DefaultHeartbeatTimeout * 5)] // 1500ms
        [TimeUnit(TimeUnit.Milliseconds)]
        [ConfigurationEntry("Raven/Cluster/HeartbeatTimeoutInMs")]
        [ConfigurationEntry("Raven/Cluster/HeartbeatTimeout")]
        public TimeSetting HeartbeatTimeout { get; set; }

        [DefaultValue(RaftEngineOptions.DefaultMaxLogLengthBeforeCompaction)]
        [ConfigurationEntry("Raven/Cluster/MaxLogLengthBeforeCompaction")]
        public int MaxLogLengthBeforeCompaction { get; set; }

        [DefaultValue(DefaultValueSetInConstructor)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Raven/Cluster/MaxStepDownDrainTime")]
        public TimeSetting MaxStepDownDrainTime { get; set; }

        [DefaultValue(RaftEngineOptions.DefaultMaxEntiresPerRequest)]
        [ConfigurationEntry("Raven/Cluster/MaxEntriesPerRequest")]
        public int MaxEntriesPerRequest { get; set; }
    }
}