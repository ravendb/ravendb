using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Session;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations
{
    public partial class OperationExecutor
    {
        public PatchStatus Send(PatchOperation operation)
        {
            return AsyncHelpers.RunSync(() => SendAsync(operation));
        }

        public async Task<PatchStatus> SendAsync(PatchOperation operation, SessionInfo sessionInfo = null, CancellationToken token = default(CancellationToken))
        {
            JsonOperationContext context;
            using (GetContext(out context))
            {
                var command = operation.GetCommand(_store, _requestExecutor.Conventions, context, _requestExecutor.Cache);

                await _requestExecutor.ExecuteAsync(command, context, sessionInfo, token).ConfigureAwait(false);

                if (command.StatusCode == HttpStatusCode.NotModified)
                    return PatchStatus.NotModified;

                if (command.StatusCode == HttpStatusCode.NotFound)
                    return PatchStatus.DocumentDoesNotExist;

                return command.Result.Status;
            }
        }

        public PatchOperation.Result<TEntity> Send<TEntity>(PatchOperation<TEntity> operation, SessionInfo sessionInfo = null)
        {
            return AsyncHelpers.RunSync(() => SendAsync(operation, sessionInfo));
        }

        public async Task<PatchOperation.Result<TEntity>> SendAsync<TEntity>(PatchOperation<TEntity> operation, SessionInfo sessionInfo = null, CancellationToken token = default(CancellationToken))
        {
            using (GetContext(out var context))
            {
                var command = operation.GetCommand(_store, _requestExecutor.Conventions, context, _requestExecutor.Cache);

                await _requestExecutor.ExecuteAsync(command, context, sessionInfo, token).ConfigureAwait(false);

                var result = new PatchOperation.Result<TEntity>();

                if (command.StatusCode == HttpStatusCode.NotModified)
                {
                    result.Status = PatchStatus.NotModified;
                    return result;
                }

                if (command.StatusCode == HttpStatusCode.NotFound)
                {
                    result.Status = PatchStatus.DocumentDoesNotExist;
                    return result;
                }

                result.Status = command.Result.Status;
                result.Document = _requestExecutor.Conventions.Serialization.DeserializeEntityFromBlittable<TEntity>(command.Result.ModifiedDocument);
                return result;
            }
        }
    }
}
