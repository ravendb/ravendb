using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Raven.Server.Documents.Operations;
using Raven.Server.Documents.Sharding.Changes;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Platform;

namespace Raven.Server.Documents.Sharding;

public partial class ShardedDatabaseContext
{
    public ShardedOperations Operations;

    public sealed class ShardedOperations : AbstractOperations<ShardedOperation>
    {
        private readonly ShardedDatabaseContext _context;

        internal readonly ConcurrentDictionary<ShardedDatabaseIdentifier, DatabaseChanges> _changes = new();

        public ShardedOperations([NotNull] ShardedDatabaseContext context)
            : base(context.Changes, PlatformDetails.Is32Bits || context.Configuration.Storage.ForceUsing32BitsPager
                ? TimeSpan.FromHours(12)
                : TimeSpan.FromDays(2))
        {
            _context = context ?? throw new ArgumentNullException(nameof(context));
        }

        public override long GetNextOperationId()
        {
            var nextId = _context.ServerStore.Operations.GetNextOperationId();

            return OperationIdEncoder.EncodeOperationId(nextId, _context.ServerStore.NodeTag);
        }

        protected override void RaiseNotifications(OperationStatusChange change, AbstractOperation operation)
        {
            var operationChanged = OperationChanged.Create(_context.DatabaseName, change.OperationId, operation.Description, change.State, operation.Killable);

            operation.NotifyCenter(operationChanged, x => _context.NotificationCenter.Add(x));

            base.RaiseNotifications(change, operation);
        }

        public override Task<IOperationResult> AddLocalOperation(
            long id,
            OperationType operationType,
            string description,
            IOperationDetailedDescription detailedDescription,
            Func<Action<IOperationProgress>, Task<IOperationResult>> taskFactory,
            string resourceName = null,
            OperationCancelToken token = null)
        {
            var operation = CreateOperationInstance(id, _context.DatabaseName, operationType, description, detailedDescription, token);

            return AddOperationInternalAsync(operation, taskFactory);
        }

        public Task AddRemoteOperation<TResult, TOrchestratorResult, TOperationProgress>(
            long id,
            OperationType operationType,
            string description,
            IOperationDetailedDescription detailedDescription,
            Func<JsonOperationContext, int, RavenCommand<TResult>> commandFactory,
            OperationCancelToken token = null)
            where TResult : OperationIdResult
            where TOrchestratorResult : IOperationResult, new()
            where TOperationProgress : IOperationProgress, new()
        {
            var operation = CreateOperationInstance(id, _context.DatabaseName, operationType, description, detailedDescription, token);

            return AddOperationInternalAsync(operation, 
                onProgress => CreateTaskAsync<TResult, TOrchestratorResult, TOperationProgress>(
                    new ShardedDatabaseMultiOperation(id, _context, onProgress),
                    commandFactory,
                    token)
                );
        }

        public Task<IOperationResult> CreateServerWideMultiOperationTask<TResult, TOrchestratorResult, TOperationProgress>(
            long id,
            Func<JsonOperationContext, int, RavenCommand<TResult>> commandFactory,
            Action<IOperationProgress> onProgress,
            OperationCancelToken token = null)
            where TResult : OperationIdResult
            where TOrchestratorResult : IOperationResult, new()
            where TOperationProgress : IOperationProgress, new()
        {
            var multiOperation = new ShardedServerMultiOperation(id, _context, onProgress);
            return CreateTaskAsync<TResult, TOrchestratorResult, TOperationProgress>(multiOperation, commandFactory, token);
        }

        private async Task<IOperationResult> CreateTaskAsync<TResult, TOrchestratorResult, TOperationProgress>(
            AbstractShardedMultiOperation multiOperation,
            Func<JsonOperationContext, int, RavenCommand<TResult>> commandFactory,
            OperationCancelToken token)
            where TResult : OperationIdResult
            where TOrchestratorResult : IOperationResult, new()
            where TOperationProgress : IOperationProgress, new()
        {
            var t = token?.Token ?? default;

            var tasks = new Task[_context.ShardCount];
            int i = 0;
            using (_context.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                foreach (var shardNumber in _context.ShardsTopology.Keys)
                {
                    var command = commandFactory(context, shardNumber);

                    tasks[i] = ConnectAsync(command, shardNumber);
                    i++;
                }
            }

            try
            {
                await Task.WhenAll(tasks);
            }
            catch
            {
                if (token != null)
                    await multiOperation.KillAsync(t);
                throw;
            }

            return await multiOperation.WaitForCompletionAsync<TOrchestratorResult>(t);

            async Task ConnectAsync(RavenCommand<TResult> command, int shardNumber)
            {
                var result = await multiOperation.ExecuteCommandForShard(command, shardNumber, t);
                
                var key = new ShardedDatabaseIdentifier(result.OperationNodeTag, shardNumber);
                var op = multiOperation.CreateOperationInstance(key, result.OperationId);

                multiOperation.Watch<TOperationProgress>(key, op);
            }
        }

        internal DatabaseChanges GetChanges(ShardedDatabaseIdentifier key) => _changes.GetOrAdd(key, k => new DatabaseChangesForShard(_context.ServerStore, _context.ShardExecutor.GetRequestExecutorAt(k.ShardNumber), ShardHelper.ToShardName(_context.DatabaseName, k.ShardNumber), onDispose: null, k.NodeTag));


        public override void Dispose(ExceptionAggregator exceptionAggregator)
        {
            foreach (var changes in _changes)
            {
                changes.Value.Dispose();
            }

            base.Dispose(exceptionAggregator);
        }
    }
}
