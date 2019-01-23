using System;
using System.Collections.Generic;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.ServerWide.Operations;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.OngoingTasks
{
    public enum OngoingTaskType
    {
        Replication,
        RavenEtl,
        SqlEtl,
        Backup,
        Subscription,
        PullReplicationAsHub,
        PullReplicationAsSink
    }

    public enum OngoingTaskState
    {
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
                [nameof(Error)] = Error
            };
        }
    }

    public class OngoingTaskSubscription : OngoingTask
    {
        public OngoingTaskSubscription()
        {
            TaskType = OngoingTaskType.Subscription;
        }

        public string Query { get; set; }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(Query)] = Query;
            return json;
        }
    }

    public class OngoingTaskReplication : OngoingTask
    {
       
        public OngoingTaskReplication()
        {
            TaskType = OngoingTaskType.Replication;
        }

        public string DestinationUrl { get; set; }
        public string[] TopologyDiscoveryUrls { get; set; }
        public string DestinationDatabase { get; set; }
        public string MentorNode { get; set; }
        public string ConnectionStringName { get; set; }
        public TimeSpan DelayReplicationFor { get; set; } 
        
        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(DestinationUrl)] = DestinationUrl;
            json[nameof(TopologyDiscoveryUrls)] = TopologyDiscoveryUrls;
            json[nameof(DestinationDatabase)] = DestinationDatabase;
            json[nameof(MentorNode)] = MentorNode;
            json[nameof(ConnectionStringName)] = ConnectionStringName;
            json[nameof(DelayReplicationFor)] = DelayReplicationFor;
            return json;
        }
    }

    public class OngoingTaskPullReplicationAsHub : OngoingTask
    {
        public OngoingTaskPullReplicationAsHub()
        {
            TaskType = OngoingTaskType.PullReplicationAsHub;
        }

        public string DestinationUrl { get; set; }
        public string DestinationDatabase { get; set; }
        public string MentorNode { get; set; }
        public TimeSpan DelayReplicationFor { get; set; }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(DestinationUrl)] = DestinationUrl;
            json[nameof(DestinationDatabase)] = DestinationDatabase;
            json[nameof(MentorNode)] = MentorNode;
            json[nameof(DelayReplicationFor)] = DelayReplicationFor;
            return json;
        }
    }

    public class OngoingTaskPullReplicationAsSink : OngoingTask
    {
        public OngoingTaskPullReplicationAsSink()
        {
            TaskType = OngoingTaskType.PullReplicationAsSink;
        }

        public string HubDefinitionName { get; set; }
        
        public string DestinationUrl { get; set; }
        public string[] TopologyDiscoveryUrls { get; set; }
        public string DestinationDatabase { get; set; }
        public string MentorNode { get; set; }
        public string ConnectionStringName { get; set; }
        
        public string CertificatePublicKey { get; set; }
        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(DestinationUrl)] = DestinationUrl;
            json[nameof(TopologyDiscoveryUrls)] = TopologyDiscoveryUrls;
            json[nameof(DestinationDatabase)] = DestinationDatabase;
            json[nameof(MentorNode)] = MentorNode;
            json[nameof(HubDefinitionName)] = HubDefinitionName;
            json[nameof(ConnectionStringName)] = ConnectionStringName;
            json[nameof(CertificatePublicKey)] = CertificatePublicKey;
            return json;
        }
    }

    public class OngoingTaskRavenEtlListView : OngoingTask
    {
        public OngoingTaskRavenEtlListView()
        {
            TaskType = OngoingTaskType.RavenEtl;
        }

        public string DestinationUrl { get; set; }
        public string DestinationDatabase { get; set; }
        public string ConnectionStringName { get; set; }
        public string[] TopologyDiscoveryUrls { get; set; }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(DestinationUrl)] = DestinationUrl;
            json[nameof(DestinationDatabase)] = DestinationDatabase;
            json[nameof(ConnectionStringName)] = ConnectionStringName;
            json[nameof(TopologyDiscoveryUrls)] = TopologyDiscoveryUrls;

            return json;
        }
    }

    public class OngoingTaskRavenEtlDetails : OngoingTask
    {
        public string DestinationUrl { get; set; }
        
        public OngoingTaskRavenEtlDetails()
        {
            TaskType = OngoingTaskType.RavenEtl;
        }

        public RavenEtlConfiguration Configuration { get; set; }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(Configuration)] = Configuration?.ToJson();
            json[nameof(DestinationUrl)] = DestinationUrl;
            return json;
        }
    }
    
    public class OngoingTaskSqlEtlListView : OngoingTask
    {
        public OngoingTaskSqlEtlListView()
        {
            TaskType = OngoingTaskType.SqlEtl;
        }

        public string DestinationServer { get; set; }

        public string DestinationDatabase { get; set; }
        
        public string ConnectionStringName { get; set; }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();

            json[nameof(DestinationServer)] = DestinationServer;
            json[nameof(DestinationDatabase)] = DestinationDatabase;
            json[nameof(ConnectionStringName)] = ConnectionStringName;

            return json;
        }
    }

    public class OngoingTaskSqlEtlDetails : OngoingTask
    {
        public OngoingTaskSqlEtlDetails()
        {
            TaskType = OngoingTaskType.SqlEtl;
        }

        public SqlEtlConfiguration Configuration { get; set; }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();

            json[nameof(Configuration)] = Configuration?.ToJson();

            return json;
        }
    }

    public class OngoingTaskBackup : OngoingTask
    {
        public BackupType BackupType { get; set; }
        public List<string> BackupDestinations { get; set; }
        public DateTime? LastFullBackup { get; set; }
        public DateTime? LastIncrementalBackup { get; set; }
        public RunningBackup OnGoingBackup { get; set; }
        public NextBackup NextBackup { get; set; }

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
            return json;
        }
    }

    public class ModifyOngoingTaskResult { 
        public long TaskId { get; set; }
        public long RaftCommandIndex;
        public string ResponsibleNode;
    }

    public class NextBackup
    {
        public TimeSpan TimeSpan { get; set; }

        public DateTime DateTime { get; set; }

        public bool IsFull { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(TimeSpan)] = TimeSpan,
                [nameof(DateTime)] = DateTime,
                [nameof(IsFull)] = IsFull
            };
        }
    }

    public class RunningBackup
    {
        public DateTime? StartTime { get; set; }

        public bool IsFull { get; set; }

        public long? RunningBackupTaskId { get; set; }

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
}
