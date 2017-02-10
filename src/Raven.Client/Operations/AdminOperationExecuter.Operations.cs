using System.Threading;
using System.Threading.Tasks;
using Raven.NewClient.Abstractions.Util;
using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Connection;
using Sparrow.Json;

namespace Raven.NewClient.Operations
{
    public partial class AdminOperationExecuter
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

                await _requestExecuter.ExecuteAsync(command, context, token).ConfigureAwait(false);
                return new Operation(_requestExecuter, _store.Conventions, command.Result.OperationId);
            }
        }
    }
}