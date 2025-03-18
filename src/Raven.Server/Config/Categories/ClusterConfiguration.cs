using System.ComponentModel;
using Raven.Server.Config.Attributes;
using Raven.Server.Config.Settings;
using Sparrow;

namespace Raven.Server.Config.Categories
{
    [ConfigurationCategory(ConfigurationCategoryType.Cluster)]
    public sealed class ClusterConfiguration : ConfigurationCategory
    {
        [Description("Timeout (in milliseconds) within which the node expects to receive a heartbeat from the leader.")]
        [DefaultValue(300)]
        [TimeUnit(TimeUnit.Milliseconds)]
        [ConfigurationEntry("Cluster.ElectionTimeoutInMs", ConfigurationEntryScope.ServerWideOnly)]
        public TimeSetting ElectionTimeout { get; set; }

        [Description("The time (in milliseconds) between sampling database information and sending it to the maintenance supervisor.")]
        [DefaultValue(500)]
        [TimeUnit(TimeUnit.Milliseconds)]
        [ConfigurationEntry("Cluster.WorkerSamplePeriodInMs", ConfigurationEntryScope.ServerWideOnly)]
        public TimeSetting WorkerSamplePeriod { get; set; }

        [Description("How long the maintenance supervisor waits (in milliseconds) between sampling the information received from the nodes.")]
        [DefaultValue(1000)]
        [TimeUnit(TimeUnit.Milliseconds)]
        [ConfigurationEntry("Cluster.SupervisorSamplePeriodInMs", ConfigurationEntryScope.ServerWideOnly)]
        public TimeSetting SupervisorSamplePeriod { get; set; }

        [Description("How long the maintenance supervisor waits (in milliseconds) for a response from a worker before timing out.")]
        [DefaultValue(5000)]
        [TimeUnit(TimeUnit.Milliseconds)]
        [ConfigurationEntry("Cluster.ReceiveFromWorkerTimeoutInMs", ConfigurationEntryScope.ServerWideOnly)]
        public TimeSetting ReceiveFromWorkerTimeout { get; set; }

        [Description("How long the maintenance supervisor waits (in milliseconds) after receiving an exception from a worker before retrying.")]
        [DefaultValue(5000)]
        [TimeUnit(TimeUnit.Milliseconds)]
        [ConfigurationEntry("Cluster.OnErrorDelayTimeInMs", ConfigurationEntryScope.ServerWideOnly)]
        public TimeSetting OnErrorDelayTime { get; set; }

        [Description("As a cluster node, how long (in seconds) to wait before timing out an operation between two cluster nodes.")]
        [DefaultValue(15)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Cluster.OperationTimeoutInSec", ConfigurationEntryScope.ServerWideOnly)]
        public TimeSetting OperationTimeout { get; set; }

        [Description("How long to wait (in seconds) for cluster stats to stabilize after a database topology change.")]
        [DefaultValue(5)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Cluster.StatsStabilizationTimeInSec", ConfigurationEntryScope.ServerWideOnly)]
        public TimeSetting StabilizationTime { get; set; }

        [Description("The time (in seconds) a database instance must be in a good and responsive state before we add a replica to match the replication factor.")]
        [DefaultValue(15 * 60)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Cluster.TimeBeforeAddingReplicaInSec", ConfigurationEntryScope.ServerWideOnly)]
        public TimeSetting AddReplicaTimeout{ get; set; }

        [Description("The grace period (in seconds) we give a node before it is moved to rehab.")]
        [DefaultValue(60)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Cluster.TimeBeforeMovingToRehabInSec", ConfigurationEntryScope.ServerWideOnly)]
        public TimeSetting MoveToRehabGraceTime{ get; set; }

        [Description("The grace period (in seconds) we give the preferred node before moving it to the end of the members list.")]
        [DefaultValue(5)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Cluster.TimeBeforeRotatingPreferredNodeInSec", ConfigurationEntryScope.ServerWideOnly)]
        public TimeSetting RotatePreferredNodeGraceTime { get; set; }

        [Description("TCP connection read/write timeout (in milliseconds).")]
        [DefaultValue(15 * 1000)]
        [TimeUnit(TimeUnit.Milliseconds)]
        [ConfigurationEntry("Cluster.TcpTimeoutInMs", ConfigurationEntryScope.ServerWideOnly)]
        public TimeSetting TcpConnectionTimeout { get; set; }

        [Description("The size (in bytes) of the TCP connection send buffer.")]
        [DefaultValue(32 * 1024)]
        [SizeUnit(SizeUnit.Bytes)]
        [ConfigurationEntry("Cluster.TcpSendBufferSizeInBytes", ConfigurationEntryScope.ServerWideOnly)]
        public Size TcpSendBufferSize { get; set; }

        [Description("The size (in bytes) of the TCP connection receive buffer.")]
        [DefaultValue(32 * 1024)]
        [SizeUnit(SizeUnit.Bytes)]
        [ConfigurationEntry("Cluster.TcpReceiveBufferSizeInBytes", ConfigurationEntryScope.ServerWideOnly)]
        public Size TcpReceiveBufferSize { get; set; }

        [Description("Set hard/soft delete for a database that was removed by the observer form the cluster topology in order to maintain the replication factor.")]
        [DefaultValue(true)]
        [ConfigurationEntry("Cluster.HardDeleteOnReplacement", ConfigurationEntryScope.ServerWideOnly)]
        public bool HardDeleteOnReplacement { get; set; }

        [Description("EXPERT: If exceeded, restrict the cluster to the specified version.")]
        [DefaultValue(null)]
        [ConfigurationEntry("Cluster.MaximalAllowedClusterVersion", ConfigurationEntryScope.ServerWideOnly)]
        public int? MaximalAllowedClusterVersion { get; set; }

        [Description("Time (in minutes) between cleanup of compare exchange tombstones.")]
        [DefaultValue(10)]
        [TimeUnit(TimeUnit.Minutes)]
        [ConfigurationEntry("Cluster.CompareExchangeTombstonesCleanupIntervalInMin", ConfigurationEntryScope.ServerWideOnly)]
        public TimeSetting CompareExchangeTombstonesCleanupInterval { get; set; }
        
        [Description("The maximum interval (in minutes) between checks for compare exchange tombstones that are performed by the cluster-wide transaction mechanism.")]
        [DefaultValue(5)]
        [TimeUnit(TimeUnit.Minutes)]
        [ConfigurationEntry("Cluster.MaxClusterTransactionCompareExchangeTombstoneCheckIntervalInMin", ConfigurationEntryScope.ServerWideOnly)]
        public TimeSetting MaxClusterTransactionCompareExchangeTombstoneCheckInterval { get; set; }
                           
        [Description("Maximum number of log entires to keep in the history log table.")]
        [DefaultValue(2048)]
        [ConfigurationEntry("Cluster.LogHistoryMaxEntries", ConfigurationEntryScope.ServerWideOnly)]
        public int LogHistoryMaxEntries { get; set; }

        [Description("Time (in seconds) between cleanup of expired compare exchange items.")]
        [DefaultValue(60)]
        [TimeUnit(TimeUnit.Seconds)]
        [ConfigurationEntry("Cluster.CompareExchangeExpiredDeleteFrequencyInSec", ConfigurationEntryScope.ServerWideOnly)]
        public TimeSetting CompareExchangeExpiredCleanupInterval { get; set; }

        [Description("Excceding the allowed change vector distance between two nodes will move the lagged node to rehab.")]
        [DefaultValue(65536)]
        [ConfigurationEntry("Cluster.MaxChangeVectorDistance", ConfigurationEntryScope.ServerWideOnly)]
        public long MaxChangeVectorDistance { get; set; }
        
        [Description("EXPERT: Disable automatic atomic writes with cluster write transactions. If set to 'true', will only consider explicitly added compare exchange values to validate cluster wide transactions.")]
        [DefaultValue(false)]
        [ConfigurationEntry("Cluster.DisableAtomicDocumentWrites", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public bool DisableAtomicDocumentWrites { get; set; }

        [Description("EXPERT: The maximum allowed size (in megabytes) for a single raft command.")]
        [DefaultValue(128)]
        [SizeUnit(SizeUnit.Megabytes)]
        [ConfigurationEntry("Cluster.MaxSizeOfSingleRaftCommandInMb", ConfigurationEntryScope.ServerWideOnly)]
        public Size? MaxSizeOfSingleRaftCommand { get; set; }
        
        [Description("EXPERT: Specifies the maximum size of the cluster transaction batch to be executed on the database at once.")]
        [DefaultValue(256)]
        [ConfigurationEntry("Cluster.MaxClusterTransactionBatchSize", ConfigurationEntryScope.ServerWideOrPerDatabase)]
        public int MaxClusterTransactionsBatchSize { get; set; }
    }
}
