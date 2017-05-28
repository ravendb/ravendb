using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Http;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Server.Operations
{
    public class ServerOperationExecutor
    {
        private readonly DocumentStoreBase _store;
        private readonly ClusterRequestExecutor _requestExecutor;

        public ServerOperationExecutor(DocumentStoreBase store)
        {
            _store = store;
            _requestExecutor = ClusterRequestExecutor.Create(_store.Urls, _store.ApiKey);
        }

        public void Send(IServerOperation operation)
        {
            AsyncHelpers.RunSync(() => SendAsync(operation));
        }

        public TResult Send<TResult>(IServerOperation<TResult> operation)
        {
            return AsyncHelpers.RunSync(() => SendAsync(operation));
        }

        public async Task SendAsync(IServerOperation operation, CancellationToken token = default(CancellationToken))
        {
            using (GetContext(out JsonOperationContext context))
            {
                var command = operation.GetCommand(_store.Conventions, context);
                await _requestExecutor.ExecuteAsync(command, context, token).ConfigureAwait(false);
            }
        }

        public async Task<TResult> SendAsync<TResult>(IServerOperation<TResult> operation, CancellationToken token = default(CancellationToken))
        {
            using (GetContext(out JsonOperationContext context))
            {
                var command = operation.GetCommand(_store.Conventions, context);

                await _requestExecutor.ExecuteAsync(command, context, token).ConfigureAwait(false);
                return command.Result;
            }
        }

        private IDisposable GetContext(out JsonOperationContext context)
        {
            return _requestExecutor.ContextPool.AllocateOperationContext(out context);
        }
    }
}