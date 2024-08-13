using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Http;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Sparrow.Json;
using Sparrow.Utils;

namespace Raven.Client.Documents.Operations
{
    public sealed class MaintenanceOperationExecutor
    {
        private readonly DocumentStoreBase _store;
        private readonly string _nodeTag;
        private readonly int? _shardNumber;
        internal readonly string _databaseName;
        private RequestExecutor _requestExecutor;
        private ServerOperationExecutor _serverOperationExecutor;

        private RequestExecutor RequestExecutor => _requestExecutor ?? (_databaseName != null ? _requestExecutor = _store.GetRequestExecutor(_databaseName) : null);

        internal MaintenanceOperationExecutor(DocumentStoreBase store, string databaseName = null, string nodeTag = null, int? shardNumber = null)
        {
            _store = store;
            _nodeTag = nodeTag;
            _shardNumber = shardNumber;
            _databaseName = databaseName ?? store.Database;
        }

        public ServerOperationExecutor Server => _serverOperationExecutor ??= new ServerOperationExecutor(_store);

        public MaintenanceOperationExecutor ForDatabase(string databaseName)
        {
            if (string.Equals(_databaseName, databaseName, StringComparison.OrdinalIgnoreCase))
                return this;

            return new MaintenanceOperationExecutor(_store, databaseName, _nodeTag, _shardNumber);
        }

        public MaintenanceOperationExecutor ForNode(string nodeTag)
        {
            if (string.Equals(_nodeTag, nodeTag, StringComparison.OrdinalIgnoreCase))
                return this;

            return new MaintenanceOperationExecutor(_store, _databaseName, nodeTag, _shardNumber);
        }

        public MaintenanceOperationExecutor ForShard(int shardNumber)
        {
            var databaseName = ClientShardHelper.ToDatabaseName(_databaseName);
            var newDatabaseName = ClientShardHelper.ToShardName(databaseName, shardNumber);
            if (string.Equals(_databaseName, newDatabaseName, StringComparison.OrdinalIgnoreCase))
                return this;

            return new MaintenanceOperationExecutor(_store, newDatabaseName, _nodeTag, shardNumber: null);
        }

        /// <summary>
        /// For internal use only
        /// </summary>
        internal MaintenanceOperationExecutor ForShardWithProxy(int shardNumber)
        {
            if (_nodeTag == null)
                throw new InvalidOperationException($"NodeTag cannot be null. First use '.{nameof(ForNode)}' method.");

            var databaseName = ClientShardHelper.ToDatabaseName(_databaseName); // we want orchestrator to proxy
            if (string.Equals(_databaseName, databaseName, StringComparison.OrdinalIgnoreCase) && _shardNumber == shardNumber)
                return this;

            return new MaintenanceOperationExecutor(_store, databaseName, _nodeTag, shardNumber);
        }

        public void Send(IMaintenanceOperation operation)
        {
            AsyncHelpers.RunSync(() => SendAsync(operation));
        }

        public TResult Send<TResult>(IMaintenanceOperation<TResult> operation)
        {
            return AsyncHelpers.RunSync(() => SendAsync(operation));
        }

        internal TResult Send<TResult>(JsonOperationContext context, IMaintenanceOperation<TResult> operation)
        {
            return AsyncHelpers.RunSync(() => SendAsync(context, operation));
        }

        public async Task SendAsync(IMaintenanceOperation operation, CancellationToken token = default)
        {
            using (GetContext(out JsonOperationContext context))
            {
                var command = operation.GetCommand(RequestExecutor.Conventions, context);
                ApplyNodeTagAndShardNumberToCommandIfSet(command);

                await RequestExecutor.ExecuteAsync(command, context, sessionInfo: null, token: token).ConfigureAwait(false);
            }
        }

        public async Task<TResult> SendAsync<TResult>(IMaintenanceOperation<TResult> operation, CancellationToken token = default)
        {
            using (GetContext(out JsonOperationContext context))
            {
                var command = operation.GetCommand(RequestExecutor.Conventions, context);

                ApplyNodeTagAndShardNumberToCommandIfSet(command);

                await RequestExecutor.ExecuteAsync(command, context, sessionInfo: null, token: token).ConfigureAwait(false);
#if DEBUG
                if (command.Result.ContainsBlittableObject())
                {
                    throw new InvalidOperationException("The return type is unmanaged, please use the overload with the context");
                }
#endif
                return command.Result;
            }
        }

        internal async Task<TResult> SendAsync<TResult>(JsonOperationContext context, IMaintenanceOperation<TResult> operation, CancellationToken token = default)
        {
            var command = operation.GetCommand(RequestExecutor.Conventions, context);

            ApplyNodeTagAndShardNumberToCommandIfSet(command);

            await RequestExecutor.ExecuteAsync(command, context, sessionInfo: null, token: token).ConfigureAwait(false);
            return command.Result;
        }

        public Operation Send(IMaintenanceOperation<OperationIdResult> operation)
        {
            return AsyncHelpers.RunSync(() => SendAsync(operation));
        }

        public Operation<TResult> Send<TResult>(IMaintenanceOperation<OperationIdResult<TResult>> operation)
        {
            return AsyncHelpers.RunSync(() => SendAsync(operation));
        }

        public async Task<Operation> SendAsync(IMaintenanceOperation<OperationIdResult> operation, CancellationToken token = default)
        {
            using (GetContext(out JsonOperationContext context))
            {
                var command = operation.GetCommand(RequestExecutor.Conventions, context);
                ApplyNodeTagAndShardNumberToCommandIfSet(command);

                await RequestExecutor.ExecuteAsync(command, context, sessionInfo: null, token: token).ConfigureAwait(false);
                var node = command.SelectedNodeTag ?? command.Result.OperationNodeTag;
                return new Operation(RequestExecutor, () => _store.Changes(_databaseName, node), RequestExecutor.Conventions, command.Result.OperationId, node);
            }
        }

        public async Task<Operation<TResult>> SendAsync<TResult>(IMaintenanceOperation<OperationIdResult<TResult>> operation, CancellationToken token = default)
        {
            using (GetContext(out JsonOperationContext context))
            {
                var command = operation.GetCommand(RequestExecutor.Conventions, context);
                ApplyNodeTagAndShardNumberToCommandIfSet(command);

                await RequestExecutor.ExecuteAsync(command, context, sessionInfo: null, token: token).ConfigureAwait(false);
                var node = command.SelectedNodeTag ?? command.Result.OperationNodeTag;

                if(_requestExecutor.ForTestingPurposes?.Logs != null)
                    _requestExecutor.ForTestingPurposes?.Logs.Enqueue($"OperationsExecutor: SendAsync: node of changes api: {node} (command.SelectedNodeTag ({command.SelectedNodeTag}) ?? command.Result.OperationNodeTag({command.Result.OperationNodeTag}))");
                
                Console.WriteLine($"OperationsExecutor: SendAsync: node of changes api: {node} (command.SelectedNodeTag ({command.SelectedNodeTag}) ?? command.Result.OperationNodeTag({command.Result.OperationNodeTag}))");
                return new Operation<TResult>(RequestExecutor, () => _store.Changes(_databaseName, node), RequestExecutor.Conventions, command.Result.Result, command.Result.OperationId, node);
            }
        }

        private IDisposable GetContext(out JsonOperationContext context)
        {
            if (RequestExecutor == null)
                throw new InvalidOperationException("Cannot use Maintenance without a database defined, did you forget to call ForDatabase?");
            return RequestExecutor.ContextPool.AllocateOperationContext(out context);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ApplyNodeTagAndShardNumberToCommandIfSet<T>(RavenCommand<T> command)
        {
            if (_nodeTag != null)
                command.SelectedNodeTag = _nodeTag;

            if (_shardNumber != null)
                command.SelectedShardNumber = _shardNumber.Value;
        }
    }
}
