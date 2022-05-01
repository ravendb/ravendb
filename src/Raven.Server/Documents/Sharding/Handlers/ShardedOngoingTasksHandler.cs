// -----------------------------------------------------------------------
//  <copyright file="ShardedOngoingTasksHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Net;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Database;
using Raven.Client.Util;
using Raven.Server.Documents.Handlers.Processors.OngoingTasks;
using Raven.Server.Documents.Sharding.Commands;
using Raven.Server.Documents.Sharding.Handlers.Processors.OngoingTasks;
using Raven.Server.Documents.Sharding.Handlers.Processors.Replication;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public class ShardedOngoingTasksHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/tasks", "GET")]
        public async Task GetOngoingTasks()
        {
            using (var processor = new ShardedOngoingTasksHandlerProcessorForGetOngoingTasks(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/connection-strings", "PUT")]
        public async Task PutConnectionString()
        {
            using (var processor = new ShardedOngoingTasksHandlerProcessorForPutConnectionString(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/connection-strings", "GET")]
        public async Task GetConnectionStrings()
        {
            using (var processor = new OngoingTasksHandlerProcessorForGetConnectionString<ShardedDatabaseRequestHandler, TransactionOperationContext>(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/connection-strings", "DELETE")]
        public async Task RemoveConnectionString()
        {
            using (var processor = new ShardedOngoingTasksHandlerProcessorForRemoveConnectionString(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/etl", "PUT")]
        public async Task AddEtl()
        {
            using (var processor = new ShardedOngoingTasksHandlerProcessorForAddEtl(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/etl", "RESET")]
        public async Task ResetEtl()
        {
            using (var processor = new ShardedOngoingTasksHandlerProcessorForResetEtl(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/tasks", "DELETE")]
        public async Task DeleteOngoingTask()
        {
            using (var processor = new ShardedOngoingTasksHandlerProcessorForDeleteOngoingTask(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/tasks/state", "POST")]
        public async Task ToggleTaskState()
        {
            using (var processor = new ShardedOngoingTasksHandlerProcessorForToggleTaskState(this))
                await processor.ExecuteAsync();
        }

        // Get Info about a specific task - For Edit View in studio - Each task should return its own specific object
        [RavenShardedAction("/databases/*/task", "GET")]
        public async Task GetOngoingTaskInfo()
        {
            if (ResourceNameValidator.IsValidResourceName(DatabaseContext.DatabaseName, ServerStore.Configuration.Core.DataDirectory.FullPath, out string errorMessage) == false)
                throw new BadRequestException(errorMessage);
            long key = 0;
            var taskId = GetLongQueryString("key", false);
            if (taskId != null)
                key = taskId.Value;
            var taskName = GetStringQueryString("taskName", false);

            if ((taskId == null) && (taskName == null))
                throw new ArgumentException("You must specify a query string argument of either 'key' or 'taskName' , but none was specified.");

            var typeStr = GetQueryStringValueAndAssertIfSingleAndNotEmpty("type");

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                using (context.OpenReadTransaction())
                {
                    var record = ServerStore.Cluster.ReadDatabase(context, DatabaseContext.DatabaseName);
                    if (record == null)
                        throw new DatabaseDoesNotExistException(DatabaseContext.DatabaseName);

                    if (Enum.TryParse<OngoingTaskType>(typeStr, true, out var type) == false)
                        throw new ArgumentException($"Unknown task type: {type}", "type");

                    switch (type)
                    {
                        case OngoingTaskType.Subscription:
                            // todo https://issues.hibernatingrhinos.com/issue/RavenDB-13113
                            break;
                        case OngoingTaskType.PullReplicationAsSink:
                        case OngoingTaskType.Replication:
                            // todo https://issues.hibernatingrhinos.com/issue/RavenDB-13110
                            await GetTaskInfoForSingleShard(context, key, taskName);
                            break;
                        case OngoingTaskType.Backup:
                            // todo https://issues.hibernatingrhinos.com/issue/RavenDB-13112
                            break;
                        case OngoingTaskType.PullReplicationAsHub:
                            throw new BadRequestException("Getting task info for " + OngoingTaskType.PullReplicationAsHub + " is not supported");
                        case OngoingTaskType.SqlEtl:
                        case OngoingTaskType.OlapEtl:
                        case OngoingTaskType.RavenEtl:
                            await GetTaskInfoForSingleShard(context, key, taskName);
                            break;
                        default:
                            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                            break;
                    }
                }
            }
        }

        private async Task GetTaskInfoForSingleShard(TransactionOperationContext context, long key, string taskName)
        {
            // TODO Shiran
            // if (taskName == null)
            //     throw new ArgumentException($"This task {key} is sharded, you must specify a query string argument for 'taskName', but none was specified.");
            
            // if (ShardHelper.TryGetShardNumberAndDatabaseName(ref taskName, out var shardNumber) == false)
            //     throw new ArgumentException($"Task '{taskName}' is sharded, you must specify the shard index, for example : '{taskName}$0'");

            var cmd = new ShardedCommand(this, Headers.None);
            // await DatabaseContext.ShardExecutor.ExecuteSingleShardAsync(context, cmd, shardNumber);
            await DatabaseContext.ShardExecutor.ExecuteSingleShardAsync(context, cmd, 0);

            HttpContext.Response.StatusCode = (int)cmd.StatusCode;
            HttpContext.Response.Headers[Constants.Headers.Etag] = cmd.Response?.Headers?.ETag?.Tag;

            if (cmd.Result != null)
                await cmd.Result.WriteJsonToAsync(ResponseBodyStream());
        }

        [RavenShardedAction("/databases/*/admin/periodic-backup", "POST")]
        public async Task UpdatePeriodicBackup()
        {
            using (var processor = new ShardedOngoingTasksHandlerProcessorForUpdatePeriodicBackup(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/periodic-backup/config", "GET")]
        public async Task GetConfiguration()
        {
            using (var processor = new OngoingTasksHandlerProcessorForGetPeriodicBackupConfiguration<ShardedDatabaseRequestHandler, TransactionOperationContext>(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/backup-data-directory", "GET")]
        public async Task FullBackupDataDirectory()
        {
            using (var processor = new OngoingTasksHandlerProcessorForGetFullBackupDataDirectory<ShardedDatabaseRequestHandler, TransactionOperationContext>(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/tasks/external-replication", "POST")]
        public async Task UpdateExternalReplication()
        {
            using (var processor = new ShardedOngoingTasksHandlerProcessorForUpdateExternalReplication(this))
                await processor.ExecuteAsync();
        }
    }
}
