using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Raven.Client.Util;

namespace Raven.Client.ServerWide.Operations
{
    public class ServerOperationExecutor : IDisposable
    {
        private readonly ClusterRequestExecutor _requestExecutor;

        public ServerOperationExecutor(DocumentStoreBase store)
        {
            _requestExecutor = store.Conventions.DisableTopologyUpdates
                ? ClusterRequestExecutor.CreateForSingleNode(store.Urls[0], store.Certificate, store.Conventions)
                : ClusterRequestExecutor.Create(store.Urls, store.Certificate, store.Conventions);

            store.AfterDispose += (sender, args) => _requestExecutor.Dispose();
        }

        public void Dispose()
        {
            _requestExecutor.Dispose();
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
            using (_requestExecutor.ContextPool.AllocateOperationContext(out var context))
            {
                var command = operation.GetCommand(_requestExecutor.Conventions, context);
                await _requestExecutor.ExecuteAsync(command, context, token: token).ConfigureAwait(false);
            }
        }

        public async Task<TResult> SendAsync<TResult>(IServerOperation<TResult> operation, CancellationToken token = default(CancellationToken))
        {
            using (_requestExecutor.ContextPool.AllocateOperationContext(out var context))
            {
                var command = operation.GetCommand(_requestExecutor.Conventions, context);

                await _requestExecutor.ExecuteAsync(command, context, token: token).ConfigureAwait(false);
                return command.Result;
            }
        }

        public Operation Send(IServerOperation<OperationIdResult> operation)
        {
            return AsyncHelpers.RunSync(() => SendAsync(operation));
        }

        public async Task<Operation> SendAsync(IServerOperation<OperationIdResult> operation, CancellationToken token = default(CancellationToken))
        {
            using (_requestExecutor.ContextPool.AllocateOperationContext(out var context))
            {
                var command = operation.GetCommand(_requestExecutor.Conventions, context);

                await _requestExecutor.ExecuteAsync(command, context, token: token).ConfigureAwait(false);
                return new ServerWideOperation(_requestExecutor, _requestExecutor.Conventions, command.Result.OperationId);
            }
        }
    }
}
