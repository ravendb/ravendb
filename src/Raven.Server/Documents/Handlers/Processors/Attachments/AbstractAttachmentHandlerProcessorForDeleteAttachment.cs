using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Attachments
{
    internal abstract class AbstractAttachmentHandlerProcessorForDeleteAttachment<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext 
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        protected AbstractAttachmentHandlerProcessorForDeleteAttachment([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected abstract ValueTask DeleteAttachmentAsync(TOperationContext context, string docId, string name, LazyStringValue changeVector);

        public override async ValueTask ExecuteAsync()
        {
            var id = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("id");
            var name = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            using (ContextPool.AllocateOperationContext(out TOperationContext context))
            {
                var changeVector = context.GetLazyString(RequestHandler.GetStringFromHeaders(Constants.Headers.IfMatch));

                await DeleteAttachmentAsync(context, id, name, changeVector);
            }

            RequestHandler.NoContentStatus();
        }
    }
}
