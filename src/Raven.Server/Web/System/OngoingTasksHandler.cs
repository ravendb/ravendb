using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Raven.Client.Exceptions;
using Raven.Client.Json.Converters;
using Raven.Client.Http;
using Raven.Client.Server;
using Raven.Client.Server.ETL;
using Raven.Client.Server.ETL.SQL;
using Raven.Client.Server.Operations;
using Raven.Client.Server.PeriodicBackup;
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

        public static (OngoingTasksResult tasks, DatabaseTopology topology) GetOngoingTasksAndDbTopology(string dbName, ServerStore serverStore)
        {
            var ongoingTasksResult = new OngoingTasksResult();
            using (serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                context.OpenReadTransaction();

                var databaseRecord = serverStore.Cluster.ReadDatabase(context, dbName);
                var dbTopology = databaseRecord?.Topology;
                var clusterTopology = serverStore.GetClusterTopology(context);

                foreach (var tasks in new []
                {
                    CollectExternalReplicationTasks(dbTopology, clusterTopology),
                    CollectEtlTasks(databaseRecord, dbTopology, clusterTopology)
                })
                {
                    ongoingTasksResult.OngoingTasksList.AddRange(tasks);
                }

                if (serverStore.DatabasesLandlord.DatabasesCache.TryGetValue(dbName, out var database) && database.Status == TaskStatus.RanToCompletion)
                {
                    ongoingTasksResult.SubscriptionsCount = (int)database.Result.SubscriptionStorage.GetAllSubscriptionsCount();
                }

                //TODO: collect all the rest of the ongoing tasks (Backup)

                return (ongoingTasksResult, dbTopology);
            }
        }

        private static IEnumerable<OngoingTask> CollectExternalReplicationTasks(DatabaseTopology dbTopology, ClusterTopology clusterTopology)
        {
            if (dbTopology == null)
                yield break;

            foreach (var watcher in dbTopology.Watchers)
            {
                var tag = dbTopology.WhoseTaskIsIt(watcher);

                yield return new OngoingTaskReplication
                {
                    TaskId = watcher.TaskId.ToString(),
                    ResponsibleNode = new NodeId
                    {
                        NodeTag = tag,
                        NodeUrl = clusterTopology.GetUrlFromTag(tag)
                    },
                    DestinationDB = watcher.Database,
                    TaskState = watcher.Disabled ? OngoingTaskState.Disabled : OngoingTaskState.Enabled,
                    DestinationURL = watcher.Url,
                };
            }
        }

        private static IEnumerable<OngoingTask> CollectEtlTasks(DatabaseRecord databaseRecord, DatabaseTopology dbTopology, ClusterTopology clusterTopology)
        {
            if (dbTopology == null)
                yield break;

            if (databaseRecord.RavenEtls != null)
            {
                foreach (var ravenEtl in databaseRecord.RavenEtls)
                {
                    var tag = dbTopology.WhoseTaskIsIt(ravenEtl);

                    yield return new OngoingRavenEtl
                    {
                        TaskId = ravenEtl.GetTaskKey().ToString(), // TODO TaskId vs TaskKey?
                        // TODO arek TaskConnectionStatus = 
                        // TODO arek TaskState = 
                        ResponsibleNode = new NodeId
                        {
                            NodeTag = tag,
                            NodeUrl = clusterTopology.GetUrlFromTag(tag)
                        },
                        DestinationUrl = ravenEtl.Destination.Url,
                        DestinationDatabase = ravenEtl.Destination.Database,
                    };
                }
            }

            if (databaseRecord.SqlEtls != null)
            {
                foreach (var sqlEtl in databaseRecord.SqlEtls)
                {
                    var tag = dbTopology.WhoseTaskIsIt(sqlEtl);

                    var (database, server) =
                        SqlConnectionStringParser.GetDatabaseAndServerFromConnectionString(sqlEtl.Destination.Connection.FactoryName,
                            sqlEtl.Destination.Connection.ConnectionString);

                    yield return new OngoingSqlEtl
                    {
                        TaskId = sqlEtl.GetTaskKey().ToString(), // TODO TaskId vs TaskKey?
                        // TODO arek TaskConnectionStatus = 
                        // TODO arek TaskState = 
                        ResponsibleNode = new NodeId
                        {
                            NodeTag = tag,
                            NodeUrl = clusterTopology.GetUrlFromTag(tag)
                        },
                        DestinationServer = server,
                        DestinationDatabase = database,
                    };
                }
            }
        }

        [RavenAction("/admin/update-watcher", "POST", "/admin/update-watcher?name={databaseName:string}")]
        public async Task UpdateWatcher()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            if (ResourceNameValidator.IsValidResourceName(name, ServerStore.Configuration.Core.DataDirectory.FullPath, out string errorMessage) == false)
                throw new BadRequestException(errorMessage);

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var updateJson = await context.ReadForMemoryAsync(RequestBodyStream(), "read-update-watcher");
                if (updateJson.TryGet(nameof(DatabaseWatcher), out BlittableJsonReaderObject watcherBlittable) == false)
                {
                    throw new InvalidDataException("DatabaseWatcher was not found.");
                }

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
                        [nameof(OngoingTask.TaskId)] = watcher.TaskId == 0 ? index : watcher.TaskId
                    });
                    writer.Flush();
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
        public string TaskId { get; set; }
        public OngoingTaskType TaskType { get; protected set; }
        public NodeId ResponsibleNode { get; set; }
        public OngoingTaskState TaskState { get; set; }
        public DateTime LastModificationTime { get; set; }
        public OngoingTaskConnectionStatus TaskConnectionStatus { get; set; }
        public virtual DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(TaskId)] = TaskId,
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
        public OngoingTaskReplication()
        {
            TaskType = OngoingTaskType.Replication;
        }

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

    public class OngoingRavenEtl : OngoingTask
    {
        public OngoingRavenEtl()
        {
            TaskType = OngoingTaskType.RavenEtl;
        }

        public string DestinationUrl { get; set; }

        public string DestinationDatabase { get; set; }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(DestinationUrl)] = DestinationUrl;
            json[nameof(DestinationDatabase)] = DestinationDatabase;
            return json;
        }
    }

    public class OngoingSqlEtl : OngoingTask
    {
        public OngoingSqlEtl()
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
}
