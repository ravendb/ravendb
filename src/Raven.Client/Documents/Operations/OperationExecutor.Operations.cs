using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Session;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations
{
    public partial class OperationExecutor
    {
        public Operation Send(IOperation<OperationIdResult> operation, SessionInfo sessionInfo = null)
        {
            return AsyncHelpers.RunSync(() => SendAsync(operation, sessionInfo));
        }

        public async Task<Operation> SendAsync(IOperation<OperationIdResult> operation, SessionInfo sessionInfo = null, CancellationToken token = default(CancellationToken))
        {
            using (GetContext(out JsonOperationContext context))
            {
                var command = operation.GetCommand(_store, _requestExecutor.Conventions, context, _requestExecutor.Cache);

                await _requestExecutor.ExecuteAsync(command, context, sessionInfo, token).ConfigureAwait(false);
                var node = command.SelectedNodeTag ?? command.Result.OperationNodeTag;
                return new Operation(_requestExecutor, () => _store.Changes(_databaseName, node), _requestExecutor.Conventions, command.Result.OperationId,
                    command.SelectedNodeTag ?? command.Result.OperationNodeTag);
            }
        }
    }
}
