using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Raven.Client.Documents.Replication;
using Raven.Client.Exceptions;
using Raven.Client.Json.Converters;
using Raven.Client.Server;
using Raven.Client.Server.Operations;
using Raven.Client.Server.PeriodicBackup;
using Raven.Server.Rachis;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
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
            var result = GetOngoingTasksAndDbTopology(name, ServerStore).tasks;

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context)) 
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.Write(writer, result.ToJson());
            }
           
            return Task.CompletedTask;
        }

        public static (OngoingTasksResult tasks , DatabaseTopology topology) GetOngoingTasksAndDbTopology(string dbName, ServerStore serverStore)
        {
            var ongoingTasksResult = new OngoingTasksResult();
            using (serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                context.OpenReadTransaction();
                var dbTopology = serverStore.Cluster.ReadDatabase(context, dbName)?.Topology;
                var clusterTopology = serverStore.GetClusterTopology(context);

                CollectReplicationOngoingTasks(dbTopology, clusterTopology, ongoingTasksResult.OngoingTasksList);

                if (serverStore.DatabasesLandlord.DatabasesCache.TryGetValue(dbName, out var database) && database.Status == TaskStatus.RanToCompletion)
                {
                    ongoingTasksResult.SubscriptionsCount = (int)database.Result.SubscriptionStorage.GetAllSubscriptionsCount();
                }

                //TODO: collect all the rest of the ongoing tasks (ETL, SQL, Backup)

                return (ongoingTasksResult, dbTopology);
            }
        }

        private static void CollectReplicationOngoingTasks(DatabaseTopology dbTopology, ClusterTopology clusterTopology, ICollection<OngoingTask> ongoingTasksList)
        {
            if (dbTopology == null)
                return;

            foreach (var watcher in dbTopology.Watchers.Values)
            {
                var tag = dbTopology.WhoseTaskIsIt(watcher);
                var task = GetReplicationTaskInfo(clusterTopology, tag, watcher);
                ongoingTasksList.Add(task);
            }
        }

        private static OngoingTaskReplication GetReplicationTaskInfo(ClusterTopology clusterTopology, string tag, ReplicationNode replicationNode)
        {
            return new OngoingTaskReplication
            {
                TaskType = OngoingTaskType.Replication,
                ResponsibleNode = new NodeId
                {
                    NodeTag = tag,
                    NodeUrl = clusterTopology.GetUrlFromTag(tag)
                },
                DestinationDB = replicationNode.Database,
                TaskState = replicationNode.Disabled ? OngoingTaskState.Disabled : OngoingTaskState.Enabled,
                DestinationURL = replicationNode.Url,
            };
        }

        [RavenAction("/admin/update-watcher", "POST", "/admin/update-watcher?name={databaseName:string}")]
        public async Task UpdateWatcher()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            if (ResourceNameValidator.IsValidResourceName(name, ServerStore.Configuration.Core.DataDirectory.FullPath, out string errorMessage) == false)
                throw new BadRequestException(errorMessage);

            ServerStore.EnsureNotPassive();
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var updateJson = await context.ReadForMemoryAsync(RequestBodyStream(), "read-update-watcher");
                if (updateJson.TryGet(nameof(DatabaseWatcher), out BlittableJsonReaderObject watcherBlittable) == false)
                {
                    throw new InvalidDataException("NewWatcher property was not found.");
                }
                using (context.OpenReadTransaction())
                {
                    var watcher = JsonDeserializationClient.DatabaseWatcher(watcherBlittable);
                    var (index, _) = await ServerStore.UpdateDatabaseWatcher(name, watcher);
                    await ServerStore.Cluster.WaitForIndexNotification(index);

                    HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;

                    using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        context.Write(writer, new DynamicJsonValue
                        {
                            [nameof(DatabasePutResult.ETag)] = index,
                            [nameof(DatabasePutResult.Key)] = name,
                            [nameof(OngoingTask.TaskId)] = watcher?.GetTaskKey().ToString(),
                        });
                        writer.Flush();
                    }
                }
            }
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
        public ulong? TaskId { get; set; }
        public OngoingTaskType TaskType { get; set; }
        public NodeId ResponsibleNode { get; set; }
        public OngoingTaskState TaskState { get; set; }
        public DateTime LastModificationTime { get; set; }
        public OngoingTaskConnectionStatus TaskConnectionStatus { get; set; }
        public virtual DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue 
            {
                [nameof(TaskId)] = TaskId?.ToString(),
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
}
