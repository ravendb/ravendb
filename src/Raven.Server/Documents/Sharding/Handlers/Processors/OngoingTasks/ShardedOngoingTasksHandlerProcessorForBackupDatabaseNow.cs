using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Http;
using Raven.Client.Util;
using Raven.Server.Documents.Handlers.Processors.OngoingTasks;
using Raven.Server.Documents.Operations;
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

        protected override async ValueTask<(long, bool)> ScheduleBackupOperationAsync(long taskId, bool isFullBackup, long operationId, bool __, DateTime? _)
        {
            var token = RequestHandler.CreateTimeLimitedBackgroundOperationToken();

            // backup might be already running on one of the shards, get the old operation id if so
            var actualOperationId = await RequestHandler.ShardExecutor.ExecuteParallelForAllAsync(new ShardedGetOperationIdForBackupOperation(RequestHandler.HttpContext, taskId, operationId, isFullBackup));
            
            // if we got the old operation id then the old backup is still running on one of the shards, we want to keep tracking it
            bool inProgressOnAnotherShard = actualOperationId != operationId;

            // if only some of the shards are running this backup, 
            var startTime = SystemTime.UtcNow;
            var t = RequestHandler.DatabaseContext.Operations.AddRemoteOperation<OperationIdResult<StartBackupOperationResult>, ShardedBackupResult, ShardedBackupProgress>(actualOperationId,
                Server.Documents.Operations.OperationType.DatabaseBackup,
                "Backup of sharded database : " + RequestHandler.DatabaseName,
                detailedDescription: null,
                (_, shardNumber) => new StartBackupOperation.StartBackupCommand(isFullBackup, taskId, actualOperationId, inProgressOnAnotherShard, startTime),
                token);
            
            t.ContinueWith(_ =>
            {
                token.Dispose();
            });

            return (actualOperationId, true);
        }

        protected override long GetNextOperationId()
        {
            return RequestHandler.DatabaseContext.Operations.GetNextOperationId();
        }

        internal readonly struct ShardedGetOperationIdForBackupOperation : IShardedOperation<OperationIdResult<StartBackupOperationResult>, long>
        {
            private readonly long _operationId;
            private readonly long _taskId;
            private readonly bool _isFullBackup;
            private readonly HttpContext _httpContext;

            public ShardedGetOperationIdForBackupOperation(HttpContext httpContext, long taskId, long operationId, bool isFullBackup)
            {
                _operationId = operationId;
                _isFullBackup = isFullBackup;
                _taskId = taskId;
                _httpContext = httpContext;
            }

            public HttpRequest HttpRequest => _httpContext.Request;

            public long Combine(Dictionary<int, ShardExecutionResult<OperationIdResult<StartBackupOperationResult>>> results)
            {
                foreach (var (shardNumber, shardResult) in results)
                {
                    if (shardResult.Result is OperationIdResult o)
                    {
                        // if one of the backups is still running it will return its current operationId which we will use to finish waiting on it
                        if (o.OperationId != AbstractOperations<Server.Documents.Operations.Operation>.InvalidOperationId)
                            return o.OperationId;
                    }
                }
                
                return _operationId;
            }

            public RavenCommand<OperationIdResult<StartBackupOperationResult>> CreateCommandForShard(int shardNumber) => 
                new StartBackupOperation.StartBackupCommand(_isFullBackup, _taskId, inProgressOnAnotherShard: false, operationId: AbstractOperations<Server.Documents.Operations.Operation>.InvalidOperationId);
        }
    }
}
