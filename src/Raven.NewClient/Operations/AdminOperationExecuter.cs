using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.NewClient.Abstractions.Util;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Http;
using Sparrow.Json;

namespace Raven.NewClient.Operations
{
    public class AdminOperationExecuter
    {
        private readonly DocumentStore _store;
        private readonly string _databaseName;
        private readonly RequestExecuter _requestExecuter;

        public AdminOperationExecuter(DocumentStore store, string databaseName = null)
        {
            _store = store;
            _databaseName = databaseName ?? store.DefaultDatabase;
            _requestExecuter = string.Equals(_databaseName, store.DefaultDatabase, StringComparison.OrdinalIgnoreCase)
                ? store.GetRequestExecuterForDefaultDatabase()
                : store.GetRequestExecuter(_databaseName);
        }

        public AdminOperationExecuter ForDatabase(string databaseName)
        {
            if (string.Equals(_databaseName, databaseName))
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
            var command = operation.GetCommand();

            JsonOperationContext context;
            using (_requestExecuter.ContextPool.AllocateOperationContext(out context))
                await _requestExecuter.ExecuteAsync(command, context, token).ConfigureAwait(false);
        }

        public async Task<TResult> SendAsync<TResult>(IAdminOperation<TResult> operation, CancellationToken token = default(CancellationToken))
        {
            var command = operation.GetCommand();

            JsonOperationContext context;
            using (_requestExecuter.ContextPool.AllocateOperationContext(out context))
            {
                await _requestExecuter.ExecuteAsync(command, context, token).ConfigureAwait(false);

                return command.Result;
            }
        }
    }
}