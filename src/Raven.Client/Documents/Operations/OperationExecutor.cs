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
        private readonly JsonOperationContext _context;

        public OperationExecutor(DocumentStoreBase store, string databaseName = null)
        {
            _store = store;
            _databaseName = databaseName ?? store.Database;
            _requestExecutor = store.GetRequestExecutor(databaseName);
        }

        internal OperationExecutor(DocumentStoreBase store, RequestExecutor requestExecutor, JsonOperationContext context)
        {
            _store = store;
            _requestExecutor = requestExecutor;
            _context = context;
        }

        public OperationExecutor ForDatabase(string databaseName)
        {
            if (string.Equals(_databaseName, databaseName, StringComparison.OrdinalIgnoreCase))
                return this;

            return new OperationExecutor(_store, databaseName);
        }

        public void Send(IOperation operation)
        {
            AsyncHelpers.RunSync(() => SendAsync(operation));
        }

        public TResult Send<TResult>(IOperation<TResult> operation)
        {
            return AsyncHelpers.RunSync(() => SendAsync(operation));
        }

        public Task SendAsync(IOperation operation, CancellationToken token = default(CancellationToken))
        {
            JsonOperationContext context;
            using (GetContext(out context))
            {
                var command = operation.GetCommand(_store.Conventions, context, _requestExecutor.Cache);

                return _requestExecutor.ExecuteAsync(command, context, token);
            }
        }

        public async Task<TResult> SendAsync<TResult>(IOperation<TResult> operation, CancellationToken token = default(CancellationToken))
        {
            JsonOperationContext context;
            using (GetContext(out context))
            {
                var command = operation.GetCommand(_store.Conventions, context, _requestExecutor.Cache);

                await _requestExecutor.ExecuteAsync(command, context, token).ConfigureAwait(false);

                return command.Result;
            }
        }

        private IDisposable GetContext(out JsonOperationContext context)
        {
            if (_context == null)
                return _requestExecutor.ContextPool.AllocateOperationContext(out context);

            context = _context;
            return null;
        }
    }
}