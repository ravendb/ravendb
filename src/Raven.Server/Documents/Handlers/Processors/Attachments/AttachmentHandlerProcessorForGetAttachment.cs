using System;
using System.IO;
using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Client.Documents.Attachments;
using Raven.Client.Extensions;
using Raven.Server.Documents.Handlers.Processors.Attachments.Strategies;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Extensions;

namespace Raven.Server.Documents.Handlers.Processors.Attachments;

internal sealed class AttachmentHandlerProcessorForGetAttachment : AbstractAttachmentHandlerProcessorForGetAttachment<DatabaseRequestHandler, DocumentsOperationContext>
{
    internal AttachmentHandlerProcessorForGetAttachment([NotNull] DatabaseRequestHandler requestHandler, bool isDocument) : base(requestHandler, isDocument)
    {
    }

    protected override async ValueTask GetAttachmentAsync(DocumentsOperationContext context, string documentId, string name, AttachmentType type, string changeVector, OperationCancelToken tcs)
    {
        using (var tx = context.OpenReadTransaction())
        {
            var attachment = RequestHandler.Database.DocumentsStorage.AttachmentsStorage.GetAttachment(context, documentId, name, type, changeVector);

            if (attachment == null)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                return;
            }

            IGetAttachmentStrategy strategy = attachment.RemoteParameters.IsRemoteStorageAttachment()
                  ? new RemoteGetAttachmentStrategyProcessor(RequestHandler)
                  : new RegularGetAttachmentStrategyProcessor(RequestHandler);

            strategy.CheckAttachmentFlagAndConfigurationAndThrowIfNeeded(context, attachment, documentId, name);

            var attachmentChangeVector = RequestHandler.GetStringFromHeaders(Constants.Headers.IfNoneMatch);
            if (attachmentChangeVector == attachment.ChangeVector)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                return;
            }

            try
            {
                var fileName = Path.GetFileName(attachment.Name);
                fileName = Uri.EscapeDataString(fileName);
                HttpContext.Response.Headers[Constants.Headers.ContentDisposition] = $"attachment; filename=\"{fileName}\"; filename*=UTF-8''{fileName}";
            }
            catch (ArgumentException e)
            {
                if (Logger.IsDebugEnabled)
                    Logger.Debug($"Skip Content-Disposition header because of not valid file name: {attachment.Name}", e);
            }

            try
            {
                HttpContext.Response.Headers[Constants.Headers.ContentType] = attachment.ContentType.ToString();
            }
            catch (InvalidOperationException e)
            {
                if (Logger.IsDebugEnabled)
                    Logger.Debug($"Skip Content-Type header because of not valid content type: {attachment.ContentType}", e);
                if (HttpContext.Response.Headers.ContainsKey(Constants.Headers.ContentType))
                    HttpContext.Response.Headers.Remove(Constants.Headers.ContentType);
            }

            HttpContext.Response.Headers[Constants.Headers.AttachmentHash] = attachment.Base64Hash.ToString();
            HttpContext.Response.Headers[Constants.Headers.AttachmentSize] = attachment.Size.ToString();
            HttpContext.Response.Headers[Constants.Headers.Etag] = $"\"{attachment.ChangeVector}\"";
            if (attachment.RemoteParameters != null)
            {
                HttpContext.Response.Headers[Constants.Headers.AttachmentRemoteParametersAt] = attachment.RemoteParameters.At.GetDefaultRavenFormat(isUtc: true);
                HttpContext.Response.Headers[Constants.Headers.AttachmentRemoteParametersIdentifier] = Uri.EscapeDataString(attachment.RemoteParameters.Identifier);
                HttpContext.Response.Headers[Constants.Headers.AttachmentRemoteParametersFlags] = attachment.RemoteParameters.Flags.ToString();
            }

            strategy.DisposeReadTransactionIfNeeded(tx);

            await strategy.WriteResponseStream(context, attachment, tcs);
        }
    }
}
