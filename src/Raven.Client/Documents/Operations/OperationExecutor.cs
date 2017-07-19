using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Http;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations
{
    public partial class OperationExecutor
    {
        private readonly DocumentStoreBase _store;
        private readonly string _databaseName;
        private readonly RequestExecutor _requestExecutor;

        public OperationExecutor(DocumentStoreBase store, string databaseName = null)
        {
            _store = store;
            _databaseName = databaseName ?? store.Database;
            _requestExecutor = store.GetRequestExecutor(databaseName);
        }

        public OperationExecutor ForDatabase(string databaseName)
        {
            if (string.Equals(_databaseName, databaseName, StringComparison.OrdinalIgnoreCase))
                return this;

            return new OperationExecutor(_store, databaseName);
        }

        public void Send(IOperation operation, int? sessionId = null)
        {
            AsyncHelpers.RunSync(() => SendAsync(operation, sessionId: sessionId));
        }

        public TResult Send<TResult>(IOperation<TResult> operation, int? sessionId = null)
        {
            return AsyncHelpers.RunSync(() => SendAsync(operation, sessionId: sessionId));
        }

        public Task SendAsync(IOperation operation, CancellationToken token = default(CancellationToken), int? sessionId = null)
        {
            using (GetContext(out JsonOperationContext context))
            {
                var command = operation.GetCommand(_store, _requestExecutor.Conventions, context, _requestExecutor.Cache);

                return _requestExecutor.ExecuteAsync(command, context, token, sessionId);
            }
        }

        public async Task<TResult> SendAsync<TResult>(IOperation<TResult> operation, CancellationToken token = default(CancellationToken), int? sessionId = null)
        {
            using (GetContext(out JsonOperationContext context))
            {
                var command = operation.GetCommand(_store, _requestExecutor.Conventions, context, _requestExecutor.Cache);

                await _requestExecutor.ExecuteAsync(command, context, token, sessionId).ConfigureAwait(false);

                return command.Result;
            }
        }

        private IDisposable GetContext(out JsonOperationContext context)
        {
            return _requestExecutor.ContextPool.AllocateOperationContext(out context);
        }
    }
}