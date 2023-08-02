using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Admin.Processors.Revisions
{
    internal abstract class AbstractAdminRevisionsHandlerProcessorForDeleteRevisions<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext 
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        protected AbstractAdminRevisionsHandlerProcessorForDeleteRevisions([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }
        
        protected abstract ValueTask DeleteRevisionsAsync(TOperationContext context, string[] documentIds, bool includeForceCreated,
            OperationCancelToken token);

        public override async ValueTask ExecuteAsync()
        {
            bool includeForceCreated = RequestHandler.GetBoolValueQueryString("includeForceCreated", required: false) ?? false;

            using (ContextPool.AllocateOperationContext(out TOperationContext context))
            {
                var json = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), "admin/revisions/delete");
                var parameters = JsonDeserializationServer.Parameters.DeleteRevisionsParameters(json);

                using (var token = RequestHandler.CreateHttpRequestBoundTimeLimitedOperationToken())
                {
                    await DeleteRevisionsAsync(context, parameters.DocumentIds, includeForceCreated, token);
                }
            }
            RequestHandler.NoContentStatus();
        }
    }
}
