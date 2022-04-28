using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Session.Operations;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Revisions
{
    internal abstract class AbstractRevisionsHandlerProcessorForGetRevisionsCount<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext 
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        public AbstractRevisionsHandlerProcessorForGetRevisionsCount([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected abstract ValueTask<GetRevisionsCountOperation.DocumentRevisionsCount> GetRevisionsCountAsync(string docId);

        public override async ValueTask ExecuteAsync()
        {
            var docId = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("id");
            var documentRevisionsDetails = await GetRevisionsCountAsync(docId);
            
            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                {
                    context.Write(writer, documentRevisionsDetails.ToJson());
                }
            }
        }
    }
}
