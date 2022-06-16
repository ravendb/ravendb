using JetBrains.Annotations;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Revisions
{
    internal abstract class AbstractRevisionsHandlerProcessorForGetRevisionsDebug<TRequestHandler, TOperationContext> : AbstractHandlerProxyReadProcessor<BlittableJsonReaderObject ,TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        protected AbstractRevisionsHandlerProcessorForGetRevisionsDebug([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected (long Start, int PageSize) GetParameters()
        {
            return (RequestHandler.GetLongQueryString("etag", false) ?? 0, RequestHandler.GetPageSize());
        }
    }
}
