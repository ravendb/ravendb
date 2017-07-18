using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations
{
    public partial class OperationExecutor
    {
        public Operation Send(IOperation<OperationIdResult> operation)
        {
            return AsyncHelpers.RunSync(() => SendAsyncAndFetchOperation(operation));
        }

        public async Task<Operation> SendAsyncAndFetchOperation(IOperation<OperationIdResult> operation, CancellationToken token = default(CancellationToken))
        {
            using (GetContext(out JsonOperationContext context))
            {
                var command = operation.GetCommand(_store, _requestExecutor.Conventions, context, _requestExecutor.Cache);

                await _requestExecutor.ExecuteAsync(command, context, token).ConfigureAwait(false);
                return new Operation(_requestExecutor, () => _store.Changes(_databaseName), _requestExecutor.Conventions, command.Result.OperationId);
            }
        }
    }
}