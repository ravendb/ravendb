using JetBrains.Annotations;
using Raven.Client.Http;
using Raven.Server.Documents.Commands.Revisions;
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
            return (RequestHandler.GetLongQueryString("start", false) ?? 0, RequestHandler.GetPageSize());
        }

        protected override RavenCommand<BlittableJsonReaderObject> CreateCommandForNode(string nodeTag)
        {
            var (start, pageSize) = GetParameters();
            return new GetRevisionsDebugCommand(nodeTag, start, pageSize);
        }
    }
}
