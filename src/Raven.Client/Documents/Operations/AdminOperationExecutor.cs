using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Http;
using Raven.Client.Server.Operations;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations
{
    public class AdminOperationExecutor
    {
        private readonly DocumentStoreBase _store;
        private readonly string _databaseName;
        private RequestExecutor _requestExecutor;
        private ServerOperationExecutor _serverOperationExecutor;

        private RequestExecutor RequestExecutor => _requestExecutor ?? (_requestExecutor = _store.GetRequestExecutor(_databaseName));

        public AdminOperationExecutor(DocumentStoreBase store, string databaseName = null)
        {
            _store = store;
            _databaseName = databaseName ?? store.Database;
        }

        public ServerOperationExecutor Server => _serverOperationExecutor ?? (_serverOperationExecutor = new ServerOperationExecutor(_store));

        public AdminOperationExecutor ForDatabase(string databaseName)
        {
            if (string.Equals(_databaseName, databaseName, StringComparison.OrdinalIgnoreCase))
                return this;

            return new AdminOperationExecutor(_store, databaseName);
        }

        public void Send(IAdminOperation operation)
        {
            AsyncHelpers.RunSync(() => SendAsync(operation));
        }

        public TResult Send<TResult>(IAdminOperation<TResult> operation)
        {
            return AsyncHelpers.RunSync(() => SendAsync(operation));
        }

        public async Task SendAsync(IAdminOperation operation, CancellationToken token = default(CancellationToken))
        {
            JsonOperationContext context;
            using (RequestExecutor.ContextPool.AllocateOperationContext(out context))
            {
                var command = operation.GetCommand(_store.Conventions, context);
                await RequestExecutor.ExecuteAsync(command, context, token).ConfigureAwait(false);
            }
        }

        public async Task<TResult> SendAsync<TResult>(IAdminOperation<TResult> operation, CancellationToken token = default(CancellationToken))
        {
            JsonOperationContext context;
            using (RequestExecutor.ContextPool.AllocateOperationContext(out context))
            {
                var command = operation.GetCommand(_store.Conventions, context);

                await RequestExecutor.ExecuteAsync(command, context, token).ConfigureAwait(false);
                return command.Result;
            }
        }

        public Operation Send(IAdminOperation<OperationIdResult> operation)
        {
            return AsyncHelpers.RunSync(() => SendAsync(operation));
        }

        public async Task<Operation> SendAsync(IAdminOperation<OperationIdResult> operation, CancellationToken token = default(CancellationToken))
        {
            JsonOperationContext context;
            using (RequestExecutor.ContextPool.AllocateOperationContext(out context))
            {
                var command = operation.GetCommand(_store.Conventions, context);

                await _requestExecutor.ExecuteAsync(command, context, token).ConfigureAwait(false);
                return new Operation(_requestExecutor, () => _store.Changes(_databaseName), _store.Conventions, command.Result.OperationId);
            }
        }
    }
}