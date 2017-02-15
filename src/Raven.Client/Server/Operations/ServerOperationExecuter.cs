using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Http;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Server.Operations
{
    public class ServerOperationExecuter
    {
        private readonly DocumentStoreBase _store;
        private readonly RequestExecuter _requestExecuter;

        public ServerOperationExecuter(DocumentStoreBase store)
        {
            _store = store;
            _requestExecuter = store.GetRequestExecuter();
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
            JsonOperationContext context;
            using (GetContext(out context))
            {
                var command = operation.GetCommand(_store.Conventions, context);

                await _requestExecuter.ExecuteAsync(command, context, token).ConfigureAwait(false);
            }
        }

        public async Task<TResult> SendAsync<TResult>(IServerOperation<TResult> operation, CancellationToken token = default(CancellationToken))
        {
            JsonOperationContext context;
            using (GetContext(out context))
            {
                var command = operation.GetCommand(_store.Conventions, context);

                await _requestExecuter.ExecuteAsync(command, context, token).ConfigureAwait(false);
                return command.Result;
            }
        }

        private IDisposable GetContext(out JsonOperationContext context)
        {
            return _requestExecuter.ContextPool.AllocateOperationContext(out context);
        }
    }
}