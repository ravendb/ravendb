// -----------------------------------------------------------------------
//  <copyright file="ShardedOngoingTasksHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Nito.AsyncEx;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Database;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server.Documents.ETL;
using Raven.Server.Documents.ETL.Providers.Raven;
using Raven.Server.Documents.Sharding;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.System;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ShardedHandlers
{
    public class ShardedOngoingTasksHandler : ShardedRequestHandler
    {
        [RavenShardedAction("/databases/*/admin/connection-strings", "PUT")]
        public async Task PutConnectionString()
        {
            await DatabaseConfigurations((_, databaseName, connectionString, guid) => ServerStore.PutConnectionString(_, databaseName, connectionString, guid), 
                "put-connection-string", GetRaftRequestIdFromQuery());
        }

        [RavenShardedAction("/databases/*/admin/etl", "PUT")]
        public async Task AddEtl()
        {
            var id = GetLongQueryString("id", required: false);

            if (id == null)
            {
                await DatabaseConfigurations((_, databaseName, etlConfiguration, guid) =>
                        ServerStore.AddEtl(_, databaseName, etlConfiguration, guid), "etl-add",
                    GetRaftRequestIdFromQuery(),
                    beforeSetupConfiguration: AssertCanAddOrUpdateEtl,
                    fillJson: (json, _, index) => json[nameof(EtlConfiguration<ConnectionString>.TaskId)] = index);

                return;
            }

            string etlConfigurationName = null;

            await DatabaseConfigurations((_, databaseName, etlConfiguration, guid) =>
                {
                    var task = ServerStore.UpdateEtl(_, databaseName, id.Value, etlConfiguration, guid);
                    etlConfiguration.TryGet(nameof(RavenEtlConfiguration.Name), out etlConfigurationName);
                    return task;
                }, "etl-update",
                GetRaftRequestIdFromQuery(),
                beforeSetupConfiguration: AssertCanAddOrUpdateEtl,
                fillJson: (json, _, index) => json[nameof(EtlConfiguration<ConnectionString>.TaskId)] = index);

            // Reset scripts if needed
            var scriptsToReset = HttpContext.Request.Query["reset"];
            var raftRequestId = GetRaftRequestIdFromQuery();

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                foreach (var script in scriptsToReset)
                {
                    await ServerStore.RemoveEtlProcessState(ctx, ShardedContext.DatabaseName, etlConfigurationName, script, $"{raftRequestId}/{script}");
                }
            }
        }

        protected delegate void RefAction(string databaseName, ref BlittableJsonReaderObject configuration, JsonOperationContext context);

        protected delegate Task<(long, object)> SetupFunc(TransactionOperationContext context, string databaseName, BlittableJsonReaderObject json, string raftRequestId);

        protected async Task DatabaseConfigurations(SetupFunc setupConfigurationFunc,
           string debug,
           string raftRequestId,
           RefAction beforeSetupConfiguration = null,
           Action<DynamicJsonValue, BlittableJsonReaderObject, long> fillJson = null,
           HttpStatusCode statusCode = HttpStatusCode.OK)
        {
            if (await CanAccessDatabaseAsync(ShardedContext.DatabaseName, requireAdmin: true, requireWrite: true) == false)
                return;

            if (ResourceNameValidator.IsValidResourceName(ShardedContext.DatabaseName, ServerStore.Configuration.Core.DataDirectory.FullPath, out string errorMessage) == false)
                throw new BadRequestException(errorMessage);

            await ServerStore.EnsureNotPassiveAsync();
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var configurationJson = await context.ReadForMemoryAsync(RequestBodyStream(), debug);
                beforeSetupConfiguration?.Invoke(ShardedContext.DatabaseName, ref configurationJson, context);

                var (index, _) = await setupConfigurationFunc(context, ShardedContext.DatabaseName, configurationJson, raftRequestId);
                await WaitForIndexToBeApplied(context, index);
                HttpContext.Response.StatusCode = (int)statusCode;

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    var json = new DynamicJsonValue
                    {
                        ["RaftCommandIndex"] = index,
                    };
                    fillJson?.Invoke(json, configurationJson, index);
                    context.Write(writer, json);
                }
            }
        }

        protected async Task WaitForIndexToBeApplied(TransactionOperationContext context, long index)
        {
            var dbs = ServerStore.DatabasesLandlord.TryGetOrCreateShardedResourcesStore(ShardedContext.DatabaseName).ToList();
            if (dbs.Count == 0)
            {
                await ServerStore.Cluster.WaitForIndexNotification(index);
            }
            else
            {
                var tasks = new List<Task>();
                foreach (var task in dbs)
                {
                    var db = await task;
                    tasks.Add(db.RachisLogIndexNotifications.WaitForIndexNotification(index, ServerStore.Engine.OperationTimeout));
                }
                await tasks.WhenAll();
            }
        }

        private void AssertCanAddOrUpdateEtl(string databaseName, ref BlittableJsonReaderObject etlConfiguration, JsonOperationContext context)
        {
            switch (EtlConfiguration<ConnectionString>.GetEtlType(etlConfiguration))
            {
                case EtlType.Raven:
                    ServerStore.LicenseManager.AssertCanAddRavenEtl();
                    break;
                case EtlType.Sql:
                    ServerStore.LicenseManager.AssertCanAddSqlEtl();
                    break;
                case EtlType.Olap:
                    ServerStore.LicenseManager.AssertCanAddOlapEtl();
                    break;
                default:
                    throw new NotSupportedException($"Unknown ETL configuration type. Configuration: {etlConfiguration}");
            }
        }

        // Get Info about a specific task - For Edit View in studio - Each task should return its own specific object
        [RavenShardedAction("/databases/*/task", "GET")]
        public async Task GetOngoingTaskInfo()
        {
            if (ResourceNameValidator.IsValidResourceName(ShardedContext.DatabaseName, ServerStore.Configuration.Core.DataDirectory.FullPath, out string errorMessage) == false)
                throw new BadRequestException(errorMessage);
            long key = 0;
            var taskId = GetLongQueryString("key", false);
            if (taskId != null)
                key = taskId.Value;
            var name = GetStringQueryString("taskName", false);

            if ((taskId == null) && (name == null))
                throw new ArgumentException($"You must specify a query string argument of either 'key' or 'name' , but none was specified.");

            var typeStr = GetQueryStringValueAndAssertIfSingleAndNotEmpty("type");

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                using (context.OpenReadTransaction())
                {
                    var clusterTopology = ServerStore.GetClusterTopology(context);
                    var record = ServerStore.Cluster.ReadDatabase(context, ShardedContext.DatabaseName);
                    if (record == null)
                        throw new DatabaseDoesNotExistException(ShardedContext.DatabaseName);

                    if (Enum.TryParse<OngoingTaskType>(typeStr, true, out var type) == false)
                        throw new ArgumentException($"Unknown task type: {type}", "type");

                    switch (type)
                    {
                        case OngoingTaskType.Replication:
                        case OngoingTaskType.Subscription:
                        case OngoingTaskType.PullReplicationAsSink:
                        case OngoingTaskType.Backup:
                            // todo
                            break;
                        case OngoingTaskType.PullReplicationAsHub:
                            throw new BadRequestException("Getting task info for " + OngoingTaskType.PullReplicationAsHub + " is not supported");

                        case OngoingTaskType.SqlEtl:
/*

                            var sqlEtl = name != null ?
                                record.SqlEtls.Find(x => x.Name.Equals(name, StringComparison.OrdinalIgnoreCase))
                                : record.SqlEtls?.Find(x => x.TaskId == key);

                            if (sqlEtl == null)
                            {
                                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                                break;
                            }

                            await WriteResult(context, new OngoingTaskSqlEtlDetails
                            {
                                TaskId = sqlEtl.TaskId,
                                TaskName = sqlEtl.Name,
                                MentorNode = sqlEtl.MentorNode,
                                Configuration = sqlEtl,
                                TaskState = GetEtlTaskState(sqlEtl),
                                TaskConnectionStatus = GetEtlTaskConnectionStatus(record, sqlEtl, out var sqlNode, out var sqlEtlError),
                                ResponsibleNode = new NodeId
                                {
                                    NodeTag = sqlNode,
                                    NodeUrl = clusterTopology.GetUrlFromTag(sqlNode)
                                },
                                Error = sqlEtlError
                            });*/
                            break;

                        case OngoingTaskType.OlapEtl:

/*                            var olapEtl = name != null ?
                                record.OlapEtls.Find(x => x.Name.Equals(name, StringComparison.OrdinalIgnoreCase))
                                : record.OlapEtls?.Find(x => x.TaskId == key);

                            if (olapEtl == null)
                            {
                                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                                break;
                            }

                            await WriteResult(context, new OngoingTaskOlapEtlDetails
                            {
                                TaskId = olapEtl.TaskId,
                                TaskName = olapEtl.Name,
                                MentorNode = olapEtl.MentorNode,
                                Configuration = olapEtl,
                                TaskState = GetEtlTaskState(olapEtl),
                                TaskConnectionStatus = GetEtlTaskConnectionStatus(record, olapEtl, out var olapNode, out var olapEtlError),
                                ResponsibleNode = new NodeId
                                {
                                    NodeTag = olapNode,
                                    NodeUrl = clusterTopology.GetUrlFromTag(olapNode)
                                },
                                Error = olapEtlError
                            });*/
                            break;

                        case OngoingTaskType.RavenEtl:

                            var ravenEtl = name != null ?
                                record.RavenEtls.Find(x => x.Name.Equals(name, StringComparison.OrdinalIgnoreCase))
                                : record.RavenEtls?.Find(x => x.TaskId == key);

                            if (ravenEtl == null)
                            {
                                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                                break;
                            }

                            var tasks = ServerStore.DatabasesLandlord.TryGetOrCreateShardedResourcesStore(ShardedContext.DatabaseName);
                            var databases = new List<DocumentDatabase>(record.Shards.Length);
                            string url = default;
                            foreach (var task in tasks)
                            {
                                var db = await task;
                                databases.Add(db);
                                var process = db.EtlLoader.Processes.OfType<RavenEtl>().FirstOrDefault(x => x.ConfigurationName == ravenEtl.Name);
                                if (process != null)
                                {
                                    url = process.Url;
                                }
                            }

                            var ongoingTaskRavenEtlDetails = new ShardedOngoingTaskRavenEtlDetails
                            {
                                TaskId = ravenEtl.TaskId,
                                TaskName = ravenEtl.Name,
                                Configuration = ravenEtl,
                                TaskState = OngoingTasksHandler.GetEtlTaskState(ravenEtl),
                                MentorNode = ravenEtl.MentorNode,
                                DestinationUrl = url,
                                TaskConnectionStatus = GetShardedEtlTaskConnectionStatus(record, databases, ravenEtl, out var responsibleNodes, out var ravenEtlError),
                                Error = ravenEtlError,
                                ResponsibleNodes = new Dictionary<string, NodeId>()
                            };

                            foreach (var kvp in responsibleNodes)
                            {
                                ongoingTaskRavenEtlDetails.ResponsibleNodes[kvp.Key] = new NodeId
                                {
                                    NodeTag = kvp.Value,
                                    NodeUrl = clusterTopology.GetUrlFromTag(kvp.Value)
                                };
                            }

                            await WriteResult(context, ongoingTaskRavenEtlDetails);
                            break;

                        default:
                            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                            break;
                    }
                }
            }
        }

        private async Task WriteResult(JsonOperationContext context, IDynamicJson taskInfo)
        {
            HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;

            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.Write(writer, taskInfo.ToJson());
            }
        }

        private Dictionary<string, OngoingTaskConnectionStatus> GetShardedEtlTaskConnectionStatus<T>(DatabaseRecord record, IReadOnlyList<DocumentDatabase> databases, EtlConfiguration<T> config, out Dictionary<string, string> responsibleNodes, out string error)
            where T : ConnectionString
        {
            var connectionStatus = new Dictionary<string, OngoingTaskConnectionStatus>();
            error = null;

            responsibleNodes = new Dictionary<string, string>();
            for (var index = 0; index < databases.Count; index++)
            {
                var shard = record.Shards[index];
                var db = databases[index];
                var dbName = db.Name;
                var processState = EtlLoader.GetProcessState(config.Transforms, db, config.Name);
                var tag = db.WhoseTaskIsIt(shard, config, processState);
                
                responsibleNodes[dbName] = tag;

                if (tag == ServerStore.NodeTag)
                {
                    var process = db.EtlLoader.Processes.FirstOrDefault(x => x.ConfigurationName == config.Name);

                    if (process != null)
                        connectionStatus[dbName] = process.GetConnectionStatus();
                    else
                    {
                        if (config.Disabled)
                            connectionStatus[dbName] = OngoingTaskConnectionStatus.NotActive;
                        else
                            error = $"ETL process '{config.Name}' was not found.";
                    }
                }
                else
                {
                    connectionStatus[dbName] = OngoingTaskConnectionStatus.NotOnThisNode;
                }
            }

            return connectionStatus;
        }
    }
}
