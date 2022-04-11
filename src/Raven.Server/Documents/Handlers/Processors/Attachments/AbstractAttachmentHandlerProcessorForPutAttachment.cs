using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Attachments
{
    internal abstract class AbstractAttachmentHandlerProcessorForPutAttachment<TRequestHandler, TOperationContext> : AbstractHandlerProcessor<TRequestHandler, TOperationContext>
        where TRequestHandler : RequestHandler
        where TOperationContext : JsonOperationContext
    {
        protected AbstractAttachmentHandlerProcessorForPutAttachment([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> contextPool) : base(requestHandler, contextPool)
        {
        }
        
        protected abstract ValueTask PutAttachmentsAsync(TOperationContext context, string id, string name, Stream requestBodyStream, string contentType, string changeVector, CancellationToken token); 

        public override async ValueTask ExecuteAsync()
        {
            using (ContextPool.AllocateOperationContext(out TOperationContext context))
            using (var token = RequestHandler.CreateOperationToken())
            {
                var id = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("id");
                var name = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
                var contentType = RequestHandler.GetStringQueryString("contentType", false) ?? "";
                var requestBodyStream = RequestHandler.RequestBodyStream();
                var changeVector = RequestHandler.GetStringFromHeaders("If-Match");

                await PutAttachmentsAsync(context, id, name, requestBodyStream, contentType, changeVector, token.Token);
            }
        }
    }
}
