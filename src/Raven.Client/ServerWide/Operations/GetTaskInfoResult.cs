using System;
using System.Collections.Generic;
using Raven.Client.ServerWide.ETL;
using Raven.Client.ServerWide.PeriodicBackup;
using Sparrow.Json.Parsing;

namespace Raven.Client.ServerWide.Operations
{
    public enum OngoingTaskType
    {
        Replication,
        RavenEtl,
        SqlEtl,
        Backup,
        Subscription
    }

    public enum OngoingTaskState
    {
        Enabled,
        Disabled,
        PartiallyEnabled
    }

    public enum OngoingTaskConnectionStatus
    {
        Active,
        NotActive
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
        public enum ReplicationStatus
        {
            None,
            Active,
            Reconnect,
            NotOnThisNode
        }

        public OngoingTaskReplication()
        {
            TaskType = OngoingTaskType.Replication;
        }

        public string DestinationUrl { get; set; }
        public string DestinationDatabase { get; set; }
        public string MentorNode { get; set; }
        public ReplicationStatus Status { get; set; }
        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(DestinationUrl)] = DestinationUrl;
            json[nameof(DestinationDatabase)] = DestinationDatabase;
            json[nameof(Status)] = Status;
            json[nameof(MentorNode)] = MentorNode;
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

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(DestinationUrl)] = DestinationUrl;
            json[nameof(DestinationDatabase)] = DestinationDatabase;
            json[nameof(ConnectionStringName)] = ConnectionStringName;

            return json;
        }
    }

    public class OngoingTaskRavenEtlDetails : OngoingTask
    {
        public OngoingTaskRavenEtlDetails()
        {
            TaskType = OngoingTaskType.RavenEtl;
        }

        public RavenEtlConfiguration Configuration { get; set; }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(Configuration)] = Configuration?.ToJson();

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

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();

            json[nameof(DestinationServer)] = DestinationServer;
            json[nameof(DestinationDatabase)] = DestinationDatabase;

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

        public bool IsFull { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(TimeSpan)] = TimeSpan,
                [nameof(IsFull)] = IsFull
            };
        }
    }
}
