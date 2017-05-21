using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Server.Operations;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Web.System
{
    public class OngoingTasksHandler : RequestHandler
    {
        [RavenAction("/admin/ongoing-tasks", "GET", "/admin/ongoing-tasks?databaseName={databaseName:string}")]
        public Task GetOngoingTasks()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("databaseName");
            var dbId = Constants.Documents.Prefix + name;

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context)) 
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                 var result = GetOngoingTasks(dbId);
                 context.Write(writer, result.ToJson());
            }
           
            return Task.CompletedTask;
        }

        public OngoingTasksResult GetOngoingTasks(string databaseId) 
        {
            // Todo (Aviv): Get real data... !

            // Sample Data:
            var ongoingTasksResult = new OngoingTasksResult();

            NodeId node1 = new NodeId() {NodeTag = "SampleNodeTag-1", NodeUrl = "SampleNodeUrl-1"};
            NodeId node2 = new NodeId() {NodeTag = "SampleNodeTag-2", NodeUrl = "SampleNodeUrl-2"};
            NodeId node3 = new NodeId() {NodeTag = "SampleNodeTag-3", NodeUrl = "SampleNodeUrl-3"};
            NodeId node4 = new NodeId() {NodeTag = "SampleNodeTag-4", NodeUrl = "SampleNodeUrl-4"};
            NodeId node5 = new NodeId() {NodeTag = "SampleNodeTag-5", NodeUrl = "SampleNodeUrl-5"};
            NodeId node6 = new NodeId() {NodeTag = "SampleNodeTag-6", NodeUrl = "SampleNodeUrl-6"};
            
            ongoingTasksResult.OngoingTasksList = new List<OngoingTask>();

            var repTask1 = new OngoingTaskReplication() { TaskType = OngoingTaskType.Replication, ResponsibleNode = node2, DestinationURL = "SampleNodeUrl-4", DestinationDB = "SampleDB-4" };
            var repTask2 = new OngoingTaskReplication() { TaskType = OngoingTaskType.Replication, ResponsibleNode = node1, DestinationURL = "SampleNodeUrl-3", DestinationDB = "SamleDB-3" };
            var repTask3 = new OngoingTaskETL() { TaskType = OngoingTaskType.ETL, ResponsibleNode = node2, DestinationURL = "SampleNodeUrl-5", DestinationDB = "SampleDB-5" };
            var repTask4 = new OngoingTaskBackup() { TaskType = OngoingTaskType.Backup, ResponsibleNode = node6,BackupDestinations = new List<string>() { "SampleDest1", "SampleDest2" }, BackupType = BackupType.Backup };
            var repTask5 = new OngoingTaskSQL() { TaskType = OngoingTaskType.SQL, ResponsibleNode = node1, SqlProvider = "SampleProvider", SqlTable = "SampleTable"};

            ongoingTasksResult.OngoingTasksList.Add(repTask1);
            ongoingTasksResult.OngoingTasksList.Add(repTask2);
            ongoingTasksResult.OngoingTasksList.Add(repTask3);
            ongoingTasksResult.OngoingTasksList.Add(repTask4);
            ongoingTasksResult.OngoingTasksList.Add(repTask5);

            ongoingTasksResult.SubscriptionsCount = 5;

            return ongoingTasksResult;
        }
    }

    public class OngoingTasksResult : IDynamicJson
    {
        public List<OngoingTask> OngoingTasksList { get; set; }
        public int SubscriptionsCount { get; set; }

        public OngoingTasksResult()
        {
            OngoingTasksList = new List<OngoingTask>();
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue 
            {
                [nameof(OngoingTasksList)] = new DynamicJsonArray(OngoingTasksList.Select(x => x.ToJson())),
                [nameof(SubscriptionsCount)] = SubscriptionsCount
            };
        }
    }

    public abstract class OngoingTask : IDynamicJson // Single task info - Common to all tasks types
    {
        public OngoingTaskType TaskType { get; set; }
        public NodeId ResponsibleNode { get; set; }
        public OngoingTaskState TaskState { get; set; }
        public DateTime LastModificationTime { get; set; }
        public OngoingTaskConnectionStatus TaskConnectionStatus { get; set; }

        public virtual DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue 
            {
                [nameof(TaskType)] = TaskType,
                [nameof(ResponsibleNode)] = ResponsibleNode.ToJson(),
                [nameof(TaskState)] = TaskState,
                [nameof(LastModificationTime)] = LastModificationTime,
                [nameof(TaskConnectionStatus)] = TaskConnectionStatus
            };
        }
    }

    public class OngoingTaskReplication : OngoingTask
    {
        public string DestinationURL { get; set; }  
        public string DestinationDB { get; set; }
        
        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(DestinationURL)] = DestinationURL;
            json[nameof(DestinationDB)] = DestinationDB;
            return json;
        }
    }

    public class OngoingTaskETL : OngoingTask
    {
        public string DestinationURL { get; set; }
        public string DestinationDB { get; set; }
        public Dictionary<string, string> CollectionsScripts { get; set; }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(DestinationURL)] = DestinationURL;
            json[nameof(DestinationDB)] = DestinationDB;
            json[nameof(CollectionsScripts)] = TypeConverter.ToBlittableSupportedType(CollectionsScripts);
            return json;
        }
    }

    public class OngoingTaskSQL : OngoingTask
    {
        public string SqlProvider { get; set; }
        public string SqlTable { get; set; }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(SqlProvider)] = SqlProvider;
            json[nameof(SqlTable)] = SqlTable;
            return json;
        }
    }

    public class OngoingTaskBackup : OngoingTask
    {
        public BackupType BackupType { get; set; }
        public List<string> BackupDestinations { get; set; }
        
        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(BackupType)] = BackupType;
            json[nameof(BackupDestinations)] = new DynamicJsonArray(BackupDestinations);
            return json;
        }
    }
   
    public enum OngoingTaskType
    {
        Replication,
        ETL,
        SQL,
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
   
    public enum BackupType // merge w/ Grish code later..
    {
       Backup,
       Snapshot
    }
}