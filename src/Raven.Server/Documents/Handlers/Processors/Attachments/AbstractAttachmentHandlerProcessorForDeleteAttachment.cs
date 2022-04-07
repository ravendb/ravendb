using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Attachments
{
    internal abstract class AbstractAttachmentHandlerProcessorForDeleteAttachment<TRequestHandler, TOperationContext> : AbstractHandlerProcessor<TRequestHandler, TOperationContext>
        where TRequestHandler : RequestHandler
        where TOperationContext : JsonOperationContext
    {
        protected AbstractAttachmentHandlerProcessorForDeleteAttachment([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> contextPool) : base(requestHandler, contextPool)
        {
        }

        protected abstract ValueTask DeleteAttachmentAsync(TOperationContext context, string docId, string name, LazyStringValue changeVector);

        public override async ValueTask ExecuteAsync()
        {
            var id = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("id");
            var name = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            using (ContextPool.AllocateOperationContext(out TOperationContext context))
            {
                var changeVector = context.GetLazyString(RequestHandler.GetStringFromHeaders("If-Match"));

                await DeleteAttachmentAsync(context, id, name, changeVector);
            }

            RequestHandler.NoContentStatus();
        }
    }
}
