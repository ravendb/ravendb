using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Exceptions.Documents.Revisions;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Json;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Admin.Processors.Revisions
{
    internal abstract class AbstractAdminRevisionsHandlerProcessorForDeleteRevisions<TRequestHandler, TOperationContext> : AbstractHandlerProcessor<TRequestHandler, TOperationContext>
        where TRequestHandler : RequestHandler
        where TOperationContext : JsonOperationContext
    {
        public AbstractAdminRevisionsHandlerProcessorForDeleteRevisions([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> contextPool) : base(requestHandler, contextPool)
        {
        }

        protected abstract bool IsRevisionsConfigured();

        protected abstract ValueTask DeleteRevisionsAsync(TOperationContext context, string[] documentIds);

        public override async ValueTask ExecuteAsync()
        {
            if(IsRevisionsConfigured() == false)
                throw new RevisionsDisabledException();

            using (ContextPool.AllocateOperationContext(out TOperationContext context))
            {
                var json = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), "admin/revisions/delete");
                var parameters = JsonDeserializationServer.Parameters.DeleteRevisionsParameters(json);

                await DeleteRevisionsAsync(context, parameters.DocumentIds);
            }
            RequestHandler.NoContentStatus();
        }
    }
}
