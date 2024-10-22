using System;
using System.Collections.Generic;
using Raven.Client.Documents.DataArchival;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.ElasticSearch;
using Raven.Client.Documents.Operations.ETL.OLAP;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Documents.Operations.QueueSink;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.ServerWide.Operations;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.OngoingTasks
{
    /// <summary>
    /// Specifies ongoing task type.
    /// </summary>
    public enum OngoingTaskType
    {
        Replication,
        RavenEtl,
        SqlEtl,
        OlapEtl,
        ElasticSearchEtl,
        QueueEtl,
        Backup,
        Subscription,
        PullReplicationAsHub,
        PullReplicationAsSink,
        QueueSink
    }

    public enum OngoingTaskState
    {
        None,
        Enabled,
        Disabled,
        PartiallyEnabled
    }

    public enum OngoingTaskConnectionStatus
    {
        None,
        Active,
        NotActive,
        Reconnect,
        NotOnThisNode
    }

    public abstract class OngoingTask : IDynamicJson // Common info for all tasks types - used for Ongoing Tasks List View in studio
    {
        public long TaskId { get; set; }
        public OngoingTaskType TaskType { get; protected set; }
        public NodeId ResponsibleNode { get; set; }
        public OngoingTaskState TaskState { get; set; }
        public OngoingTaskConnectionStatus TaskConnectionStatus { get; set; }
        public string TaskName { get; set; }
        public string Error { get; set; }
        public string MentorNode { get; set; }
        public bool PinToMentorNode { get; set; }

        public virtual DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(TaskId)] = TaskId,
                [nameof(TaskType)] = TaskType,
                [nameof(ResponsibleNode)] = ResponsibleNode?.ToJson(),
                [nameof(TaskState)] = TaskState,
                [nameof(TaskConnectionStatus)] = TaskConnectionStatus,
                [nameof(TaskName)] = TaskName,
                [nameof(MentorNode)] = MentorNode,
                [nameof(PinToMentorNode)] = PinToMentorNode,
                [nameof(Error)] = Error
            };
        }
    }

    public sealed class OngoingTaskSubscription : OngoingTask
    {
        public OngoingTaskSubscription()
        {
            TaskType = OngoingTaskType.Subscription;
        }

        public string Query { get; set; }
        public string SubscriptionName { get; set; }
        public long SubscriptionId { get; set; }
        public ArchivedDataProcessingBehavior? ArchivedDataProcessingBehavior { get; set; }
        public string ChangeVectorForNextBatchStartingPoint { get; set; }
        public Dictionary<string, string> ChangeVectorForNextBatchStartingPointPerShard { get; set; }
        public DateTime? LastBatchAckTime { get; set; }
        public bool Disabled { get; set; }
        public DateTime? LastClientConnectionTime { get; set; }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(Query)] = Query;
            json[nameof(SubscriptionName)] = SubscriptionName;
            json[nameof(SubscriptionId)] = SubscriptionId;
            json[nameof(ArchivedDataProcessingBehavior)] = ArchivedDataProcessingBehavior;
            json[nameof(ChangeVectorForNextBatchStartingPoint)] = ChangeVectorForNextBatchStartingPoint;
            json[nameof(ChangeVectorForNextBatchStartingPointPerShard)] = ChangeVectorForNextBatchStartingPointPerShard?.ToJson();
            json[nameof(LastBatchAckTime)] = LastBatchAckTime;
            json[nameof(Disabled)] = Disabled;
            json[nameof(LastClientConnectionTime)] = LastClientConnectionTime;
            return json;
        }

        internal static OngoingTaskSubscription From(SubscriptionState state, OngoingTaskConnectionStatus connectionStatus, ClusterTopology clusterTopology, string responsibleNodeTag)
        {
            if (state == null)
                throw new ArgumentNullException(nameof(state));
            if (clusterTopology == null)
                throw new ArgumentNullException(nameof(clusterTopology));

            return new OngoingTaskSubscription
            {
                TaskName = state.SubscriptionName,
                TaskId = state.SubscriptionId,
                TaskState = state.Disabled ? OngoingTaskState.Disabled : OngoingTaskState.Enabled,
                Query = state.Query,
                ChangeVectorForNextBatchStartingPoint = state.ChangeVectorForNextBatchStartingPoint,
                ChangeVectorForNextBatchStartingPointPerShard = state.ShardingState?.ChangeVectorForNextBatchStartingPointPerShard,
                SubscriptionId = state.SubscriptionId,
                SubscriptionName = state.SubscriptionName,
                ArchivedDataProcessingBehavior = state.ArchivedDataProcessingBehavior,
                LastBatchAckTime = state.LastBatchAckTime,
                Disabled = state.Disabled,
                LastClientConnectionTime = state.LastClientConnectionTime,
                MentorNode = state.MentorNode,
                PinToMentorNode = state.PinToMentorNode,
                ResponsibleNode = new NodeId
                {
                    NodeTag = responsibleNodeTag,
                    NodeUrl = clusterTopology.GetUrlFromTag(responsibleNodeTag)
                },
                TaskConnectionStatus = connectionStatus
            };
        }
    }

    public sealed class OngoingTaskReplication : OngoingTask
    {
        public OngoingTaskReplication()
        {
            TaskType = OngoingTaskType.Replication;
        }

        public string FromToString { get; set; }
        public string DestinationUrl { get; set; }
        public string[] TopologyDiscoveryUrls { get; set; }
        public string DestinationDatabase { get; set; }
        public string ConnectionStringName { get; set; }
        public TimeSpan DelayReplicationFor { get; set; }
        public string LastAcceptedChangeVectorFromDestination { get; set; }
        public string SourceDatabaseChangeVector { get; set; }
        public long LastSentEtag { get; set; }
        //TODO: last etag

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(FromToString)] = FromToString;
            json[nameof(DestinationUrl)] = DestinationUrl;
            json[nameof(TopologyDiscoveryUrls)] = TopologyDiscoveryUrls;
            json[nameof(DestinationDatabase)] = DestinationDatabase;
            json[nameof(ConnectionStringName)] = ConnectionStringName;
            json[nameof(DelayReplicationFor)] = DelayReplicationFor;
            json[nameof(LastAcceptedChangeVectorFromDestination)] = LastAcceptedChangeVectorFromDestination;
            json[nameof(SourceDatabaseChangeVector)] = SourceDatabaseChangeVector;
            json[nameof(LastSentEtag)] = LastSentEtag;
            return json;
        }
    }

    public sealed class OngoingTaskPullReplicationAsHub : OngoingTask
    {
        public OngoingTaskPullReplicationAsHub()
        {
            TaskType = OngoingTaskType.PullReplicationAsHub;
        }

        public string DestinationUrl { get; set; }
        public string DestinationDatabase { get; set; }
        public TimeSpan DelayReplicationFor { get; set; }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(DestinationUrl)] = DestinationUrl;
            json[nameof(DestinationDatabase)] = DestinationDatabase;
            json[nameof(DelayReplicationFor)] = DelayReplicationFor;
            return json;
        }
    }

    public sealed class OngoingTaskPullReplicationAsSink : OngoingTask
    {
        public OngoingTaskPullReplicationAsSink()
        {
            TaskType = OngoingTaskType.PullReplicationAsSink;
        }

        public string HubName { get; set; }
        public PullReplicationMode Mode { get; set; }

        public string DestinationUrl { get; set; }
        public string[] TopologyDiscoveryUrls { get; set; }
        public string DestinationDatabase { get; set; }
        public string ConnectionStringName { get; set; }

        public string CertificatePublicKey { get; set; }

        public string AccessName { get; set; }
        public string[] AllowedHubToSinkPaths { get; set; }
        public string[] AllowedSinkToHubPaths { get; set; }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(DestinationUrl)] = DestinationUrl;
            json[nameof(TopologyDiscoveryUrls)] = TopologyDiscoveryUrls;
            json[nameof(DestinationDatabase)] = DestinationDatabase;
            json[nameof(HubName)] = HubName;
            json[nameof(Mode)] = Mode;
            json[nameof(ConnectionStringName)] = ConnectionStringName;
            json[nameof(CertificatePublicKey)] = CertificatePublicKey;
            json[nameof(AccessName)] = AccessName;
            json[nameof(AllowedHubToSinkPaths)] = AllowedHubToSinkPaths;
            json[nameof(AllowedSinkToHubPaths)] = AllowedSinkToHubPaths;
            json[nameof(MentorNode)] = MentorNode;
            json[nameof(PinToMentorNode)] = PinToMentorNode;
            return json;
        }
    }

    public sealed class OngoingTaskRavenEtl : OngoingTask
    {
        public OngoingTaskRavenEtl()
        {
            TaskType = OngoingTaskType.RavenEtl;
        }

        public string DestinationUrl { get; set; }
        public string DestinationDatabase { get; set; }
        public string ConnectionStringName { get; set; }
        public string[] TopologyDiscoveryUrls { get; set; }

        public RavenEtlConfiguration Configuration { get; set; }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(DestinationUrl)] = DestinationUrl;
            json[nameof(DestinationDatabase)] = DestinationDatabase;
            json[nameof(ConnectionStringName)] = ConnectionStringName;
            json[nameof(TopologyDiscoveryUrls)] = TopologyDiscoveryUrls;
            json[nameof(Configuration)] = Configuration?.ToJson();

            return json;
        }
    }

    public sealed class OngoingTaskSqlEtl : OngoingTask
    {
        public OngoingTaskSqlEtl()
        {
            TaskType = OngoingTaskType.SqlEtl;
        }

        public string DestinationServer { get; set; }

        public string DestinationDatabase { get; set; }

        public string ConnectionStringName { get; set; }

        public bool ConnectionStringDefined { get; set; }

        public SqlEtlConfiguration Configuration { get; set; }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();

            json[nameof(DestinationServer)] = DestinationServer;
            json[nameof(DestinationDatabase)] = DestinationDatabase;
            json[nameof(ConnectionStringName)] = ConnectionStringName;
            json[nameof(ConnectionStringDefined)] = ConnectionStringDefined;
            json[nameof(Configuration)] = Configuration?.ToJson();

            return json;
        }
    }

    public sealed class OngoingTaskOlapEtl : OngoingTask
    {
        public OngoingTaskOlapEtl()
        {
            TaskType = OngoingTaskType.OlapEtl;
        }

        public string ConnectionStringName { get; set; }
        public string Destination { get; set; }
        public OlapEtlConfiguration Configuration { get; set; }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();

            json[nameof(ConnectionStringName)] = ConnectionStringName;
            json[nameof(Destination)] = Destination;
            json[nameof(Configuration)] = Configuration?.ToJson();

            return json;
        }
    }

    public sealed class OngoingTaskElasticSearchEtl : OngoingTask
    {
        public OngoingTaskElasticSearchEtl()
        {
            TaskType = OngoingTaskType.ElasticSearchEtl;
        }

        public string ConnectionStringName { get; set; }
        public string[] NodesUrls { get; set; }

        public ElasticSearchEtlConfiguration Configuration { get; set; }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();

            json[nameof(ConnectionStringName)] = ConnectionStringName;
            json[nameof(NodesUrls)] = NodesUrls;
            json[nameof(Configuration)] = Configuration?.ToJson();

            return json;
        }
    }

    public sealed class OngoingTaskQueueEtl : OngoingTask
    {
        public OngoingTaskQueueEtl()
        {
            TaskType = OngoingTaskType.QueueEtl;
        }

        public QueueBrokerType BrokerType { get; set; }
        public string ConnectionStringName { get; set; }
        public string Url { get; set; }

        public QueueEtlConfiguration Configuration { get; set; }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();

            json[nameof(BrokerType)] = BrokerType;
            json[nameof(ConnectionStringName)] = ConnectionStringName;
            json[nameof(Url)] = Url;
            json[nameof(Configuration)] = Configuration?.ToJson();

            return json;
        }
    }

    public sealed class OngoingTaskBackup : OngoingTask
    {
        public BackupType BackupType { get; set; }
        public List<string> BackupDestinations { get; set; }
        public DateTime? LastFullBackup { get; set; }
        public DateTime? LastIncrementalBackup { get; set; }
        public RunningBackup OnGoingBackup { get; set; }
        public NextBackup NextBackup { get; set; }
        public RetentionPolicy RetentionPolicy { get; set; }
        public bool IsEncrypted { get; set; }
        public string LastExecutingNodeTag { get; set; }

        public OngoingTaskBackup()
        {
            TaskType = OngoingTaskType.Backup;
        }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(BackupType)] = BackupType;
            json[nameof(BackupDestinations)] = new DynamicJsonArray(BackupDestinations);
            json[nameof(LastFullBackup)] = LastFullBackup;
            json[nameof(LastIncrementalBackup)] = LastIncrementalBackup;
            json[nameof(OnGoingBackup)] = OnGoingBackup?.ToJson();
            json[nameof(NextBackup)] = NextBackup?.ToJson();
            json[nameof(RetentionPolicy)] = RetentionPolicy?.ToJson();
            json[nameof(IsEncrypted)] = IsEncrypted;
            json[nameof(LastExecutingNodeTag)] = LastExecutingNodeTag;
            return json;
        }
    }

    public sealed class ModifyOngoingTaskResult
    {
        public long TaskId { get; set; }
        public long RaftCommandIndex;
        public string ResponsibleNode;
    }

    public sealed class NextBackup : IDynamicJson
    {
        public TimeSpan TimeSpan { get; set; }

        public DateTime DateTime { get; set; }

        public bool IsFull { get; set; }

        public DateTime? OriginalBackupTime { get; set; }

        internal long TaskId { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(TimeSpan)] = TimeSpan,
                [nameof(DateTime)] = DateTime,
                [nameof(IsFull)] = IsFull,
                [nameof(OriginalBackupTime)] = OriginalBackupTime
            };
        }
    }

    public sealed class RunningBackup : IDynamicJson
    {
        public DateTime? StartTime { get; set; }

        public bool IsFull { get; set; }

        public long RunningBackupTaskId { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(StartTime)] = StartTime,
                [nameof(IsFull)] = IsFull,
                [nameof(RunningBackupTaskId)] = RunningBackupTaskId
            };
        }
    }
    
    public class OngoingTaskQueueSink : OngoingTask
    {
        public OngoingTaskQueueSink()
        {
            TaskType = OngoingTaskType.QueueSink;
        }

        public QueueSinkConfiguration Configuration { get; set; }
        
        public QueueBrokerType BrokerType { get; set; }
        
        public string ConnectionStringName { get; set; }
        
        public string Url { get; set; }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();

            json[nameof(BrokerType)] = BrokerType;
            json[nameof(ConnectionStringName)] = ConnectionStringName;
            json[nameof(Url)] = Url;
            json[nameof(Configuration)] = Configuration?.ToJson();

            return json;
        }
    }
}
