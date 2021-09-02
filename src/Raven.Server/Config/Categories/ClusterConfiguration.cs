using System.ComponentModel;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;
using Sparrow;

namespace Raven.Server.Config.Categories
{
    [ConfigurationCategory(ConfigurationCategoryType.Cluster)]
    public class ClusterConfiguration : ConfigurationCategory
    {
        [Description("Timeout in which the node expects to receive a heartbeat from the leader")]
        [DefaultValue(300)]
        [TimeUnit(TimeUnit.Milliseconds)]
        [ConfigurationEntry("Cluster.ElectionTimeoutInMs", ConfigurationEntryScope.ServerWideOnly)]
        public TimeSetting ElectionTimeout { get; set; }

        [Description("How frequently we sample the information about the databases and send it to the maintenance supervisor.")]
        [DefaultValue(250)]
        [TimeUnit(TimeUnit.Milliseconds)]
        [ConfigurationEntry("Cluster.WorkerSamplePeriodInMs", ConfigurationEntryScope.ServerWideOnly)]
        public TimeSetting WorkerSamplePeriod { get; set; }

        [Description("As the maintenance supervisor, how frequent we sample the information received from the nodes.")]
        [DefaultValue(500)]
        [TimeUnit(TimeUnit.Milliseconds)]
        [ConfigurationEntry("Cluster.SupervisorSamplePeriodInMs", ConfigurationEntryScope.ServerWideOnly)]
        public TimeSetting SupervisorSamplePeriod { get; set; }

        [Description("As the maintenance supervisor, how long we wait to hear from a worker before it is time out.")]
        [DefaultValue(5000)]
        [TimeUnit(TimeUnit.Milliseconds)]
        [ConfigurationEntry("Cluster.ReceiveFromWorkerTimeoutInMs", ConfigurationEntryScope.ServerWideOnly)]
        public TimeSetting ReceiveFromWorkerTimeout { get; set; }

        [Description("As the maintenance supervisor, how long we wait after we received an exception from a worker. Before we retry.")]
        [DefaultValue(5000)]
        [TimeUnit(TimeUnit.Milliseconds)]
        [ConfigurationEntry("Cluster.OnErrorDelayTimeInMs", ConfigurationEntryScope.ServerWideOnly)]
        public TimeSetting OnErrorDelayTime { get; set; }

        [Description("As a cluster node, how long it takes to timeout operation between two cluster nodes.")]
        [DefaultValue(15)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Cluster.OperationTimeoutInSec", ConfigurationEntryScope.ServerWideOnly)]
        public TimeSetting OperationTimeout { get; set; }

        [Description("The time we give to the cluster stats to stabilize after a database topology change.")]
        [DefaultValue(5)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Cluster.StatsStabilizationTimeInSec", ConfigurationEntryScope.ServerWideOnly)]
        public TimeSetting StabilizationTime { get; set; }

        [Description("The time we give to a database instance to be in a good and responsive state, before we adding a replica to match the replication factor.")]
        [DefaultValue(15 * 60)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Cluster.TimeBeforeAddingReplicaInSec", ConfigurationEntryScope.ServerWideOnly)]
        public TimeSetting AddReplicaTimeout{ get; set; }

        [Description("The grace time we give to a node before it will be moved to rehab.")]
        [DefaultValue(60)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Cluster.TimeBeforeMovingToRehabInSec", ConfigurationEntryScope.ServerWideOnly)]
        public TimeSetting MoveToRehabGraceTime{ get; set; }

        [Description("The grace time we give to the preferred node before we move him to the end of the members list.")]
        [DefaultValue(5)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Cluster.TimeBeforeRotatingPreferredNodeInSec", ConfigurationEntryScope.ServerWideOnly)]
        public TimeSetting RotatePreferredNodeGraceTime { get; set; }

        [Description("Tcp connection read/write timeout.")]
        [DefaultValue(15 * 1000)]
        [TimeUnit(TimeUnit.Milliseconds)]
        [ConfigurationEntry("Cluster.TcpTimeoutInMs", ConfigurationEntryScope.ServerWideOnly)]
        public TimeSetting TcpConnectionTimeout { get; set; }

        [Description("Tcp connection send buffer size in bytes")]
        [DefaultValue(32 * 1024)]
        [SizeUnit(SizeUnit.Bytes)]
        [ConfigurationEntry("Cluster.TcpSendBufferSizeInBytes", ConfigurationEntryScope.ServerWideOnly)]
        public Size TcpSendBufferSize { get; set; }

        [Description("Tcp connection receive buffer size in bytes")]
        [DefaultValue(32 * 1024)]
        [SizeUnit(SizeUnit.Bytes)]
        [ConfigurationEntry("Cluster.TcpReceiveBufferSizeInBytes", ConfigurationEntryScope.ServerWideOnly)]
        public Size TcpReceiveBufferSize { get; set; }

        [Description("Set hard/soft delete for a database that was removed by the observer form the cluster topology in order to maintain the replication factor.")]
        [DefaultValue(true)]
        [ConfigurationEntry("Cluster.HardDeleteOnReplacement", ConfigurationEntryScope.ServerWideOnly)]
        public bool HardDeleteOnReplacement { get; set; }

        [Description("EXPERT: If exceeded, clamp the cluster to the specified version.")]
        [DefaultValue(null)]
        [ConfigurationEntry("Cluster.MaximalAllowedClusterVersion", ConfigurationEntryScope.ServerWideOnly)]
        public int? MaximalAllowedClusterVersion { get; set; }

        [Description("Time (in minutes) between compare exchange tombstones cleanup")]
        [DefaultValue(10)]
        [TimeUnit(TimeUnit.Minutes)]
        [ConfigurationEntry("Cluster.CompareExchangeTombstonesCleanupIntervalInMin", ConfigurationEntryScope.ServerWideOnly)]
        public TimeSetting CompareExchangeTombstonesCleanupInterval { get; set; }

        [Description("Maximum number of log entires to keep in the history log table.")]
        [DefaultValue(2048)]
        [ConfigurationEntry("Cluster.LogHistoryMaxEntries", ConfigurationEntryScope.ServerWideOnly)]
        public int LogHistoryMaxEntries { get; set; }

        [Description("Time (in seconds) between expired compare exchange cleanup")]
        [DefaultValue(60)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Cluster.CompareExchangeExpiredDeleteFrequencyInSec", ConfigurationEntryScope.ServerWideOnly)]
        public TimeSetting CompareExchangeExpiredCleanupInterval { get; set; }

        [Description("Excceding the allowed change vector distance between two nodes, will move the lagged node to rehab.")]
        [DefaultValue(10_000)]
        [ConfigurationEntry("Cluster.MaxChangeVectorDistance", ConfigurationEntryScope.ServerWideOnly)]
        public long MaxChangeVectorDistance { get; set; }
        
        [Description("EXPERT: Disable automatic atomic writes with cluster write transactions. If set to 'true', will only consider explicitly added compare exchange values to validate cluster wide transactions.")]
        [DefaultValue(false)]
        [ConfigurationEntry("Cluster.DisableAtomicDocumentWrites", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public bool DisableAtomicDocumentWrites { get; set; }
    }
}
