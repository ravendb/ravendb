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
        
        protected abstract ValueTask PutAttachmentsAsync(TOperationContext context, string id, string name, Stream requestBodyStream, string contentType, string changeVector, RemoteAttachmentParameters remoteAttachmentParameters, CancellationToken token); 

        public override async ValueTask ExecuteAsync()
        {
            using (ContextPool.AllocateOperationContext(out TOperationContext context))
            using (var token = RequestHandler.CreateHttpRequestBoundOperationToken())
            {
                var id = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("id");
                var name = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
                var contentType = RequestHandler.GetStringQueryString("contentType", false) ?? "";
                var remoteAtStr = RequestHandler.GetStringQueryString("remoteAt", false) ?? "";
                var remoteIdentifierStr = RequestHandler.GetStringQueryString("remoteIdentifier", false) ?? "";

                if (string.IsNullOrEmpty(remoteAtStr) ^ string.IsNullOrEmpty(remoteIdentifierStr))
                {
                    throw new ArgumentException("Both 'remoteAt' and 'remoteIdentifier' must be specified together.");
                }

                RemoteAttachmentParameters remoteAttachmentParameters = null;
                if (string.IsNullOrEmpty(remoteAtStr) == false)
                {
                    var remoteAtDt = TimeSeriesHandlerProcessorForGetTimeSeries.ParseDate(remoteAtStr, "remoteAt");
                    remoteAttachmentParameters = new RemoteAttachmentParameters(remoteIdentifierStr, remoteAtDt);
                }

                var requestBodyStream = RequestHandler.RequestBodyStream();
                var changeVector = RequestHandler.GetStringFromHeaders(Constants.Headers.IfMatch);

                await PutAttachmentsAsync(context, id, name, requestBodyStream, contentType, changeVector, remoteAttachmentParameters, token.Token);
            }
        }
    }
}
