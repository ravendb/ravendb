using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Attachments
{
    internal abstract class AbstractAttachmentHandlerProcessorForPutAttachment<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext 
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        protected AbstractAttachmentHandlerProcessorForPutAttachment([NotNull] TRequestHandler requestHandler) : base(requestHandler)
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
                var changeVector = RequestHandler.GetStringFromHeaders(Constants.Headers.IfMatch);

                await PutAttachmentsAsync(context, id, name, requestBodyStream, contentType, changeVector, token.Token);
            }
        }
    }
}
