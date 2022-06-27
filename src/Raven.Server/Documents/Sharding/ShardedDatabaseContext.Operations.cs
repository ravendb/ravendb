using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Raven.Server.Documents.Operations;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Platform;
using Sparrow.Utils;
using Operation = Raven.Client.Documents.Operations.Operation;

namespace Raven.Server.Documents.Sharding;

public partial class ShardedDatabaseContext
{
    public ShardedOperations Operations;

    public class ShardedOperations : AbstractOperations<ShardedOperation>
    {
        private readonly ShardedDatabaseContext _context;

        private readonly ConcurrentDictionary<ShardedDatabaseIdentifier, DatabaseChanges> _changes = new();

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
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Pawel, DevelopmentHelper.Severity.Normal, "handle notification center");

            base.RaiseNotifications(change, operation);
        }

        public override Task<IOperationResult> AddLocalOperation(
            long id,
            OperationType operationType,
            string description,
            IOperationDetailedDescription detailedDescription,
            Func<Action<IOperationProgress>, Task<IOperationResult>> taskFactory,
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

            return AddOperationInternalAsync(operation, onProgress => CreateTaskAsync<TResult, TOrchestratorResult, TOperationProgress>(operation, commandFactory, onProgress, token));
        }

        private async Task<IOperationResult> CreateTaskAsync<TResult, TOrchestratorResult, TOperationProgress>(
            ShardedOperation operation,
            Func<JsonOperationContext, int, RavenCommand<TResult>> commandFactory,
            Action<IOperationProgress> onProgress,
            OperationCancelToken token) 
            where TResult : OperationIdResult
            where TOrchestratorResult : IOperationResult, new()
            where TOperationProgress : IOperationProgress, new()
        {
            var t = token?.Token ?? default;

            operation.Operation = new MultiOperation(operation.Id, _context, onProgress);

            var tasks = new Task[_context.ShardCount];
            using (_context.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                for (var shardNumber = 0; shardNumber < tasks.Length; shardNumber++)
                {
                    var command = commandFactory(context, shardNumber);

                    tasks[shardNumber] = ConnectAsync(_context, operation.Operation, command, shardNumber, t);
                }
            }

            try
            {
                await Task.WhenAll(tasks);
            }
            catch
            {
                if (operation.Killable)
                    await operation.KillAsync(waitForCompletion: true, t);
            }

            return await operation.Operation.WaitForCompletionAsync<TOrchestratorResult>(t);

            async Task ConnectAsync(ShardedDatabaseContext context, MultiOperation operation, RavenCommand<TResult> command, int shardNumber, CancellationToken token)
            {
                await context.ShardExecutor.ExecuteSingleShardAsync(command, shardNumber, token);

                var key = new ShardedDatabaseIdentifier(command.Result.OperationNodeTag, shardNumber);

                var changes = GetChanges(key);

                var shardOperation = new Operation(context.ShardExecutor.GetRequestExecutorAt(shardNumber), () => changes, DocumentConventions.DefaultForServer, command.Result.OperationId, command.Result.OperationNodeTag);

                operation.Watch<TOperationProgress>(key, shardOperation);
            }
        }

        internal DatabaseChanges GetChanges(ShardedDatabaseIdentifier key) => _changes.GetOrAdd(key, k => new DatabaseChanges(_context.ShardExecutor.GetRequestExecutorAt(k.ShardNumber), ShardHelper.ToShardName(_context.DatabaseName, k.ShardNumber), onDispose: null, k.NodeTag));
    }
}
