using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Server.Documents.Handlers.Processors.TimeSeries;
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
        
        protected abstract ValueTask PutAttachmentsAsync(TOperationContext context, string id, string name, Stream requestBodyStream, string contentType, string changeVector, RetireAttachmentParameters retireAttachmentParameters, CancellationToken token); 

        public override async ValueTask ExecuteAsync()
        {
            using (ContextPool.AllocateOperationContext(out TOperationContext context))
            using (var token = RequestHandler.CreateHttpRequestBoundOperationToken())
            {
                var id = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("id");
                var name = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
                var contentType = RequestHandler.GetStringQueryString("contentType", false) ?? "";
                var retireAtStr = RequestHandler.GetStringQueryString("retireAt", false) ?? "";
                var retireIdentifierStr = RequestHandler.GetStringQueryString("retireIdentifier", false) ?? "";

                if (string.IsNullOrEmpty(retireAtStr) ^ string.IsNullOrEmpty(retireIdentifierStr))
                {
                    throw new ArgumentException("Both 'retireAt' and 'retireIdentifier' must be specified together.");
                }

                RetireAttachmentParameters retireAttachmentParameters = null;
                if (string.IsNullOrEmpty(retireAtStr) == false)
                {
                    var retireAtDt = TimeSeriesHandlerProcessorForGetTimeSeries.ParseDate(retireAtStr, "retireAt");
                    retireAttachmentParameters = new RetireAttachmentParameters(retireIdentifierStr, retireAtDt);
                }

                var requestBodyStream = RequestHandler.RequestBodyStream();
                var changeVector = RequestHandler.GetStringFromHeaders(Constants.Headers.IfMatch);

                await PutAttachmentsAsync(context, id, name, requestBodyStream, contentType, changeVector, retireAttachmentParameters, token.Token);
            }
        }
    }
}
