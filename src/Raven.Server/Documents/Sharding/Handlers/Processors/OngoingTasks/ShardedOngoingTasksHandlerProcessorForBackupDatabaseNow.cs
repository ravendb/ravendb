using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http;
using Raven.Client;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Exceptions.Database;
using Raven.Client.Http;
using Raven.Client.Util;
using Raven.Server.Documents.Handlers.Processors.OngoingTasks;
using Raven.Server.Documents.Sharding.Executors;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.OngoingTasks
{
    internal sealed class ShardedOngoingTasksHandlerProcessorForBackupDatabaseNow : AbstractOngoingTasksHandlerProcessorForBackupDatabaseNow<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedOngoingTasksHandlerProcessorForBackupDatabaseNow([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async ValueTask<(long OperationId, bool IsResponsibleNode)> ScheduleBackupOperationAsync(long taskId, bool isFullBackup, long operationId,  DateTime? __)
        {
            var token = RequestHandler.CreateTimeLimitedBackgroundOperationToken();
            
            // backup might be already running on one of the shards, get the current operation id from that shard if so
            var shardsOperationId =
                await RequestHandler.ShardExecutor.ExecuteParallelForAllAsync(new ShardedGetOperationIdForBackupOperation(RequestHandler.HttpContext, taskId));
            
            if (shardsOperationId != Constants.Operations.InvalidOperationId)
                throw new BackupAlreadyRunningException($"Can't start backup with taskId {taskId} because it is already running under operation id {shardsOperationId}")
                {
                    OperationId = shardsOperationId,
                    NodeTag = ServerStore.NodeTag
                };
            
            // backup isn't running on any shard, use the operation id we got to create a new backup task
            var startTime = SystemTime.UtcNow;
            var t = RequestHandler.DatabaseContext.Operations.AddRemoteOperation<OperationIdResult<StartBackupOperationResult>, ShardedBackupResult, ShardedBackupProgress>(operationId,
                Server.Documents.Operations.OperationType.DatabaseBackup,
                "Backup of sharded database : " + RequestHandler.DatabaseName,
                detailedDescription: null,
                (_, shardNumber) => new StartBackupOperation.StartBackupCommand(isFullBackup, taskId, operationId, startTime),
                token);
            
            _ = t.ContinueWith(_ =>
            {
                token.Dispose();
            });

            return (operationId, IsResponsibleNode: true);
        }

        protected override long GetNextOperationId()
        {
            return RequestHandler.DatabaseContext.Operations.GetNextOperationId();
        }

        internal readonly struct ShardedGetOperationIdForBackupOperation : IShardedOperation<OperationIdResult, long>
        {
            private readonly long _taskId;
            private readonly HttpContext _httpContext;

            public ShardedGetOperationIdForBackupOperation(HttpContext httpContext, long taskId)
            {
                _taskId = taskId;
                _httpContext = httpContext;
            }

            public HttpRequest HttpRequest => _httpContext.Request;

            public long Combine(Dictionary<int, ShardExecutionResult<OperationIdResult>> results)
            {
                foreach (var (shardNumber, shardResult) in results)
                {
                    var operationIdResult = shardResult.Result;
                    if (operationIdResult != null)
                    {
                        // if one of the backups is still running it will return its current operationId
                        if (operationIdResult.OperationId != Constants.Operations.InvalidOperationId)
                            return operationIdResult.OperationId;
                    }
                }
                
                return Constants.Operations.InvalidOperationId;
            }

            public RavenCommand<OperationIdResult> CreateCommandForShard(int shardNumber) => 
                new GetRunningBackupStatusCommand(_taskId);
        }
    }
}
