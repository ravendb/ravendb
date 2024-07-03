using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Commands;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Sparrow.Json;


namespace Raven.Server.Documents.Handlers.Processors.Revisions
{
    internal abstract class AbstractRevisionsHandlerProcessorForRevertRevisionsForDocument<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
        where TOperationContext : JsonOperationContext
    {
        protected AbstractRevisionsHandlerProcessorForRevertRevisionsForDocument([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected abstract Task RevertDocumentsAsync(Dictionary<string, string> idToChangeVector, OperationCancelToken token);

        public override async ValueTask ExecuteAsync()
        {
            RevertDocumentsToRevisionsRequest request;

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var json = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), "revisions/revert");

                request = JsonDeserializationServer.RevertDocumentToRevision(json);
            }

            using (var token = RequestHandler.CreateHttpRequestBoundOperationToken())
            {
                await RevertDocumentsAsync(request.IdToChangeVector, token);
            }
        }
    }
}
