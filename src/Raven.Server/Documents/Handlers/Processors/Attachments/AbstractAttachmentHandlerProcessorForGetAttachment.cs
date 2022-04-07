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

        protected abstract ValueTask<AttachmentResult> GetAttachmentAsync(TOperationContext context, string documentId, string name, AttachmentType type, string changeVector);

        protected abstract CancellationToken GetDataBaseShutDownToken();

        protected abstract RavenTransaction OpenReadTransaction(TOperationContext context);

        public override async ValueTask ExecuteAsync()
        {
            var documentId = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("id");
            var name = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            using (ContextPool.AllocateOperationContext(out TOperationContext context))
            using (OpenReadTransaction(context))
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

                var attachment = await GetAttachmentAsync(context, documentId, name, type, changeVector);
                if (attachment == null)
                    return;

                try
                {
                    var fileName = Path.GetFileName(attachment.Details.Name);
                    fileName = Uri.EscapeDataString(fileName);
                    HttpContext.Response.Headers["Content-Disposition"] = $"attachment; filename=\"{fileName}\"; filename*=UTF-8''{fileName}";
                }
                catch (ArgumentException e)
                {
                    if (Logger.IsInfoEnabled)
                        Logger.Info($"Skip Content-Disposition header because of not valid file name: {attachment.Details.Name}", e);
                }
                try
                {
                    HttpContext.Response.Headers["Content-Type"] = attachment.Details.ContentType;
                }
                catch (InvalidOperationException e)
                {
                    if (Logger.IsInfoEnabled)
                        Logger.Info($"Skip Content-Type header because of not valid content type: {attachment.Details.ContentType}", e);
                    if (HttpContext.Response.Headers.ContainsKey("Content-Type"))
                        HttpContext.Response.Headers.Remove("Content-Type");
                }
                HttpContext.Response.Headers["Attachment-Hash"] = attachment.Details.Hash;
                HttpContext.Response.Headers["Attachment-Size"] = attachment.Details.Size.ToString();
                HttpContext.Response.Headers[Constants.Headers.Etag] = $"\"{attachment.Details.ChangeVector}\"";

                using (context.GetMemoryBuffer(out var buffer))
                await using (var stream = attachment.Stream)
                {
                    var responseStream = RequestHandler.ResponseBodyStream();
                    var count = stream.Read(buffer.Memory.Memory.Span); // can never wait, so no need for async
                    while (count > 0)
                    {
                        await responseStream.WriteAsync(buffer.Memory.Memory.Slice(0, count), GetDataBaseShutDownToken());
                        // we know that this can never wait, so no need to do async i/o here
                        count = stream.Read(buffer.Memory.Memory.Span);
                    }
                }
            }
        }
    }
}
