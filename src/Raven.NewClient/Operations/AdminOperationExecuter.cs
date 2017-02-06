using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.NewClient.Abstractions.Util;
using Raven.NewClient.Client;
using Raven.NewClient.Client.Http;
using Sparrow.Json;

namespace Raven.NewClient.Operations
{
    public partial class AdminOperationExecuter
    {
        private readonly DocumentStoreBase _store;
        private readonly string _databaseName;
        private readonly RequestExecuter _requestExecuter;
        private readonly JsonOperationContext _context;

        public AdminOperationExecuter(DocumentStoreBase store, string databaseName = null)
        {
            _store = store;
            _databaseName = databaseName ?? store.DefaultDatabase;
            _requestExecuter = store.GetRequestExecuter(databaseName);
        }

        internal AdminOperationExecuter(DocumentStoreBase store, RequestExecuter requestExecuter, JsonOperationContext context)
        {
            _store = store;
            _requestExecuter = requestExecuter;
            _context = context;
        }

        public AdminOperationExecuter ForDatabase(string databaseName)
        {
            if (string.Equals(_databaseName, databaseName, StringComparison.OrdinalIgnoreCase))
                return this;

            return new AdminOperationExecuter(_store, databaseName);
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

                await _requestExecuter.ExecuteAsync(command, context, token).ConfigureAwait(false);
            }
        }

        public async Task<TResult> SendAsync<TResult>(IAdminOperation<TResult> operation, CancellationToken token = default(CancellationToken))
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
            if (_context == null)
                return _requestExecuter.ContextPool.AllocateOperationContext(out context);

            context = _context;
            return null;
        }
    }
}