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
using Raven.Server.Documents.ShardedHandlers.ShardedCommands;
using Raven.Server.Documents.Sharding;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Web.System;

namespace Raven.Server.Documents.ShardedHandlers
{
    public class ShardedOngoingTasksHandler : ShardedRequestHandler
    {
        [RavenShardedAction("/databases/*/admin/connection-strings", "PUT")]
        public async Task PutConnectionString()
        {
            await OngoingTasksHandler.PutConnectionString(ShardedContext.DatabaseName, this);
        }


        [RavenShardedAction("/databases/*/admin/connection-strings", "GET")]
        public async Task GetConnectionStrings()
        {
            await OngoingTasksHandler.GetConnectionStrings(ShardedContext.DatabaseName, this);

        }

        [RavenShardedAction("/databases/*/admin/connection-strings", "DELETE")]
        public async Task RemoveConnectionString()
        {
            await OngoingTasksHandler.RemoveConnectionString(ShardedContext.DatabaseName, this);
        }

        [RavenShardedAction("/databases/*/admin/etl", "PUT")]
        public async Task AddEtl()
        {
            await OngoingTasksHandler.AddEtl(ShardedContext.DatabaseName, this);
        }

        [RavenShardedAction("/databases/*/admin/etl", "RESET")]
        public async Task ResetEtl()
        {
            await OngoingTasksHandler.ResetEtl(ShardedContext.DatabaseName, this);
        }

        [RavenShardedAction("/databases/*/admin/tasks", "DELETE")]
        public async Task DeleteOngoingTask()
        {
            await OngoingTasksHandler.DeleteOngoingTask(ShardedContext.DatabaseName, this);
        }

        [RavenShardedAction("/databases/*/admin/tasks/state", "POST")]
        public async Task ToggleTaskState()
        {
            await OngoingTasksHandler.ToggleTaskState(ShardedContext.DatabaseName, this);
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
                throw new ArgumentException("You must specify a query string argument of either 'key' or 'name' , but none was specified.");

            var typeStr = GetQueryStringValueAndAssertIfSingleAndNotEmpty("type");

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                using (context.OpenReadTransaction())
                {
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
                        case OngoingTaskType.OlapEtl:
                        case OngoingTaskType.RavenEtl:
                            if (name == null)
                                throw new ArgumentException($"ETL task {key} is sharded, you must specify a query string argument for 'name', but none was specified.");

                            var index = ShardHelper.TryGetShardIndexAndDatabaseName(ref name);
                            if (index == -1)
                                throw new ArgumentException($"ETL task '{name}' is sharded, you must specify the shard index, for example : '{name}$0'");

                            var cmd = new ShardedCommand(this, Headers.None);
                            await ShardedContext.RequestExecutors[index].ExecuteAsync(cmd, context);

                            HttpContext.Response.StatusCode = (int)cmd.StatusCode;
                            HttpContext.Response.Headers[Constants.Headers.Etag] = cmd.Response?.Headers?.ETag?.Tag;

                            if (cmd.Result != null)
                                await cmd.Result.WriteJsonToAsync(ResponseBodyStream());
                            break;
                        default:
                            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                            break;
                    }
                }
            }
        }
    }
}
