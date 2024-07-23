using System.Collections.Generic;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Commands;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Sparrow.Json;


namespace Raven.Server.Documents.Handlers.Processors.Revisions
{
    internal abstract class AbstractRevisionsHandlerProcessorForDeleteRevisions<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
        where TOperationContext : JsonOperationContext
    {
        protected AbstractRevisionsHandlerProcessorForDeleteRevisions([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected abstract Task DeleteRevisions(DeleteRevisionsIntrenalRequest request, OperationCancelToken token);

        public override async ValueTask ExecuteAsync()
        {
            DeleteRevisionsIntrenalRequest request;

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var json = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), "revisions/delete");

                request = JsonDeserializationServer.DeleteRevisions(json);
            }

            using (var token = RequestHandler.CreateHttpRequestBoundOperationToken())
            {
                await DeleteRevisions(request, token);
            }
        }
    }
}
