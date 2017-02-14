using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Http;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations
{
    public partial class OperationExecuter
    {
        private readonly DocumentStoreBase _store;
        private readonly string _databaseName;
        private readonly RequestExecuter _requestExecuter;
        private readonly JsonOperationContext _context;

        public OperationExecuter(DocumentStoreBase store, string databaseName = null)
        {
            _store = store;
            _databaseName = databaseName ?? store.DefaultDatabase;
            _requestExecuter = store.GetRequestExecuter(databaseName);
        }

        internal OperationExecuter(DocumentStoreBase store, RequestExecuter requestExecuter, JsonOperationContext context)
        {
            _store = store;
            _requestExecuter = requestExecuter;
            _context = context;
        }

        public OperationExecuter ForDatabase(string databaseName)
        {
            if (string.Equals(_databaseName, databaseName, StringComparison.OrdinalIgnoreCase))
                return this;

            return new OperationExecuter(_store, databaseName);
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
                var command = operation.GetCommand(_store.Conventions, context, _requestExecuter.Cache);

                return _requestExecuter.ExecuteAsync(command, context, token);
            }
        }

        public async Task<TResult> SendAsync<TResult>(IOperation<TResult> operation, CancellationToken token = default(CancellationToken))
        {
            JsonOperationContext context;
            using (GetContext(out context))
            {
                var command = operation.GetCommand(_store.Conventions, context, _requestExecuter.Cache);

                await _requestExecuter.ExecuteAsync(command, context, token).ConfigureAwait(false);

                return command.Result;
            }
        }

        private IDisposable GetContext(out JsonOperationContext context)
        {
            if (_context == null)
                return _requestExecuter.ContextPool.AllocateOperationContext(out context);

            context = _context;
            return null;
        }
    }
}