using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Server.ServerWide;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Documents.Handlers.Processors.Attachments
{
    internal abstract class AbstractAttachmentHandlerProcessorForGetAttachment<TRequestHandler, TOperationContext> : AbstractHandlerProcessor<TRequestHandler, TOperationContext>
        where TRequestHandler : RequestHandler
        where TOperationContext : JsonOperationContext
    {
        protected Logger Logger;
        private readonly bool _isDocument;

        protected AbstractAttachmentHandlerProcessorForGetAttachment([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> contextPool, Logger logger, bool isDocument) : base(requestHandler, contextPool)
        {
            Logger = logger;
            _isDocument = isDocument;
        }

        protected abstract ValueTask GetAttachmentAsync(TOperationContext context, string documentId, string name, AttachmentType type, string changeVector, CancellationToken token);

        protected abstract RavenTransaction OpenReadTransaction(TOperationContext context);

        public override async ValueTask ExecuteAsync()
        {
            var documentId = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("id");
            var name = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            using (ContextPool.AllocateOperationContext(out TOperationContext context))
            using (OpenReadTransaction(context))
            using (var token = RequestHandler.CreateOperationToken())
            {
                var type = AttachmentType.Document;
                string changeVector = null;
                if (_isDocument == false)
                {
                    var stream = RequestHandler.TryGetRequestFromStream("ChangeVectorAndType") ?? RequestHandler.RequestBodyStream();
                    var request = await context.ReadForDiskAsync(stream, "GetAttachment");

                    if (request.TryGet("Type", out string typeString) == false ||
                        Enum.TryParse(typeString, out type) == false)
                        throw new ArgumentException("The 'Type' field in the body request is mandatory");

                    if (request.TryGet("ChangeVector", out changeVector) == false && changeVector != null)
                        throw new ArgumentException("The 'ChangeVector' field in the body request is mandatory");
                }
                
                await GetAttachmentAsync(context, documentId, name, type, changeVector, token.Token);
            }
        }
    }
}
