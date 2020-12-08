using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations
{
    public partial class OperationExecutor
    {
        private readonly IDocumentStore _store;
        private readonly string _databaseName;
        private readonly RequestExecutor _requestExecutor;

        public OperationExecutor(DocumentStoreBase store, string databaseName = null) 
            : this((IDocumentStore)store, databaseName)
        {
        }

        protected OperationExecutor(IDocumentStore store, string databaseName = null)
        {
            _store = store;
            _databaseName = databaseName ?? store.Database;
            if (string.IsNullOrWhiteSpace(_databaseName) == false)
                _requestExecutor = store.GetRequestExecutor(_databaseName);
        }

        public virtual OperationExecutor ForDatabase(string databaseName)
        {
            if (string.Equals(_databaseName, databaseName, StringComparison.OrdinalIgnoreCase))
                return this;

            return new OperationExecutor(_store, databaseName);
        }

        public void Send(IOperation operation, SessionInfo sessionInfo = null)
        {
            AsyncHelpers.RunSync(() => SendAsync(operation, sessionInfo: sessionInfo));
        }

        public TResult Send<TResult>(IOperation<TResult> operation, SessionInfo sessionInfo = null)
        {
            return AsyncHelpers.RunSync(() => SendAsync(operation, sessionInfo));
        }

        public async Task SendAsync(IOperation operation, SessionInfo sessionInfo = null, CancellationToken token = default)
        {
            using (GetContext(out JsonOperationContext context))
            {
                var command = operation.GetCommand(_store, _requestExecutor.Conventions, context, _requestExecutor.Cache);

                await _requestExecutor.ExecuteAsync(command, context, sessionInfo, token).ConfigureAwait(false);
            }
        }

        public async Task<TResult> SendAsync<TResult>(IOperation<TResult> operation, SessionInfo sessionInfo = null, CancellationToken token = default)
        {
            using (GetContext(out JsonOperationContext context))
            {
                var command = operation.GetCommand(_store, _requestExecutor.Conventions, context, _requestExecutor.Cache);

                await _requestExecutor.ExecuteAsync(command, context, sessionInfo, token).ConfigureAwait(false);

                return command.Result;
            }
        }

        protected virtual IDisposable GetContext(out JsonOperationContext context)
        {
            if (_requestExecutor == null)
                throw new InvalidOperationException("Cannot use Operations without a database defined, did you forget to call ForDatabase?");
            return _requestExecutor.ContextPool.AllocateOperationContext(out context);
        }
    }
}
