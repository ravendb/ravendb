using System.Threading.Tasks;
using JetBrains.Annotations;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Documents;

internal abstract class AbstractDocumentHandlerProcessorForPut
    <TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    where TOperationContext : JsonOperationContext
{
    protected AbstractDocumentHandlerProcessorForPut([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    public override async ValueTask ExecuteAsync()
    {
        using (ContextPool.AllocateOperationContext(out TOperationContext context))
        {
            var id = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("id");
            // We HAVE to read the document in full, trying to parallelize the doc read
            // and the identity generation needs to take into account that the identity
            // generation can fail and will leave the reading task hanging if we abort
            // easier to just do in synchronously
            var doc = await context.ReadForDiskAsync(RequestHandler.RequestBodyStream(), id).ConfigureAwait(false);

            if (id[^1] == '|')
            {
                // note that we use the _overall_ database for this, not the specific shards
                var (_, clusterId, _) = await ServerStore.GenerateClusterIdentityAsync(id, RequestHandler.IdentityPartsSeparator, RequestHandler.DatabaseName, RequestHandler.GetRaftRequestIdFromQuery());
                id = clusterId;
            }

            var changeVector = RequestHandler.GetStringFromHeaders("If-Match");

            await HandleDocumentPutAsync(id, changeVector, doc, context);
        }
    }

    protected abstract ValueTask HandleDocumentPutAsync(string id, string changeVector, BlittableJsonReaderObject doc, TOperationContext context);
}
