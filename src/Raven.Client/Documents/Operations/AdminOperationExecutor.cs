using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Http;
using Raven.Client.Server.Operations;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations
{
    public partial class AdminOperationExecutor
    {
        private readonly DocumentStoreBase _store;
        private readonly string _databaseName;
        private readonly RequestExecutor _requestExecutor;
        private readonly JsonOperationContext _context;
        private ServerOperationExecutor _serverOperationExecutor;

        public AdminOperationExecutor(DocumentStoreBase store, string databaseName = null)
        {
            _store = store;
            _databaseName = databaseName ?? store.Database;
            _requestExecutor = store.GetRequestExecutor(databaseName);
        }

        internal AdminOperationExecutor(DocumentStoreBase store, RequestExecutor requestExecutor, JsonOperationContext context)
        {
            _store = store;
            _requestExecutor = requestExecutor;
            _context = context;
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
            using (GetContext(out context))
            {
                var command = operation.GetCommand(_store.Conventions, context);

                await _requestExecutor.ExecuteAsync(command, context, token).ConfigureAwait(false);
            }
        }

        public async Task<TResult> SendAsync<TResult>(IAdminOperation<TResult> operation, CancellationToken token = default(CancellationToken))
        {
            JsonOperationContext context;
            using (GetContext(out context))
            {
                var command = operation.GetCommand(_store.Conventions, context);

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