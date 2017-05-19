using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations
{
    public partial class AdminOperationExecutor
    {
        public Operation Send(IAdminOperation<OperationIdResult> operation)
        {
            return AsyncHelpers.RunSync(() => SendAsync(operation));
        }

        public async Task<Operation> SendAsync(IAdminOperation<OperationIdResult> operation, CancellationToken token = default(CancellationToken))
        {
            JsonOperationContext context;
            using (GetContext(out context))
            {
                var command = operation.GetCommand(_store.Conventions, context);

                await _requestExecutor.ExecuteAsync(command, context, token).ConfigureAwait(false);
                return new Operation(_requestExecutor, () => _store.Changes(_databaseName), _store.Conventions, command.Result.OperationId);
            }
        }
    }
}