using System;
using System.IO;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Client.Documents.Attachments;
using Raven.Server.Documents.Handlers.Processors.Attachments.Retired;
using Raven.Server.ServerWide.Context;
using Sparrow.Extensions;

namespace Raven.Server.Documents.Handlers.Processors.Attachments;
public interface IAttachmentGetProcessor : IDisposable
{
    Task ExecuteAsync();
}

internal sealed class AttachmentHandlerDispatcherProcessorForGetAttachment
    : AttachmentHandlerBaseProcessorForGetAttachment
{
    public AttachmentHandlerDispatcherProcessorForGetAttachment(DatabaseRequestHandler requestHandler, bool isDocument)
        : base(requestHandler, isDocument) { }

    // Provide default implementations (these won't be called directly in the dispatcher)
    public override void DisposeReadTransactionIfNeeded(DocumentsTransaction tx) { }
    public override string CheckAttachmentFlagAndConfigurationAndThrowIfNeeded(DocumentsOperationContext context, Attachment attachment, string documentId, string name) => null;
    protected override Task WriteResponseStream(DocumentsOperationContext context, Attachment attachment, string collection, CancellationToken token) => Task.CompletedTask;
}

internal abstract class AttachmentHandlerBaseProcessorForGetAttachment : AbstractAttachmentHandlerProcessorForGetAttachment<DatabaseRequestHandler, DocumentsOperationContext>
{
    internal AttachmentHandlerBaseProcessorForGetAttachment([NotNull] DatabaseRequestHandler requestHandler, bool isDocument) : base(requestHandler, isDocument)
    {
    }

    public abstract void DisposeReadTransactionIfNeeded(DocumentsTransaction tx);
    public abstract string CheckAttachmentFlagAndConfigurationAndThrowIfNeeded(DocumentsOperationContext context, Attachment attachment, string documentId, string name);
    protected abstract Task WriteResponseStream(DocumentsOperationContext context, Attachment attachment, string collection, CancellationToken token);

    protected override async ValueTask GetAttachmentAsync(DocumentsOperationContext context, string documentId, string name, AttachmentType type, string changeVector, CancellationToken token)
    {
        using (var tx = context.OpenReadTransaction())
        {
            var attachment = RequestHandler.Database.DocumentsStorage.AttachmentsStorage.GetAttachment(context, documentId, name, type, changeVector);

            if (attachment == null)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                return;
            }

            if (attachment.Flags.HasFlag(AttachmentFlags.Retired))
            {
                var handler = new RetiredAttachmentHandlerProcessorForGetAttachment(RequestHandler, _isDocument);
                await handler.GetAttachmentInternalAsync(context, documentId, name, attachment, tx, token);
            }
            else
            {
                var handler = new AttachmentHandlerProcessorForGetAttachment(RequestHandler, _isDocument);
                await handler.GetAttachmentInternalAsync(context, documentId, name, attachment, tx, token);
            }
        }
    }

    protected async ValueTask GetAttachmentInternalAsync(DocumentsOperationContext context, string documentId, string name, Attachment attachment,
        DocumentsTransaction tx, CancellationToken token)
    {
        var collection = CheckAttachmentFlagAndConfigurationAndThrowIfNeeded(context, attachment, documentId, name);

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
            if (Logger.IsInfoEnabled)
                Logger.Info($"Skip Content-Disposition header because of not valid file name: {attachment.Name}", e);
        }

        try
        {
            HttpContext.Response.Headers[Constants.Headers.ContentType] = attachment.ContentType.ToString();
        }
        catch (InvalidOperationException e)
        {
            if (Logger.IsInfoEnabled)
                Logger.Info($"Skip Content-Type header because of not valid content type: {attachment.ContentType}", e);
            if (HttpContext.Response.Headers.ContainsKey(Constants.Headers.ContentType))
                HttpContext.Response.Headers.Remove(Constants.Headers.ContentType);
        }

        HttpContext.Response.Headers[Constants.Headers.AttachmentHash] = attachment.Base64Hash.ToString();
        HttpContext.Response.Headers[Constants.Headers.AttachmentSize] = attachment.Size.ToString();
        HttpContext.Response.Headers[Constants.Headers.Etag] = $"\"{attachment.ChangeVector}\"";
        HttpContext.Response.Headers[Constants.Headers.AttachmentRetireAt] = attachment.RetireAt?.GetDefaultRavenFormat();
        HttpContext.Response.Headers[Constants.Headers.AttachmentFlags] = ((int)attachment.Flags).ToString();
        HttpContext.Response.Headers[Constants.Headers.AttachmentCollection] = attachment.Collection.ToString();
        DisposeReadTransactionIfNeeded(tx);

        await WriteResponseStream(context, attachment, collection, token);
    }

    protected async Task WriteAttachmentToResponseStream(DocumentsOperationContext context, Stream stream, CancellationToken token)
    {
        using (context.GetMemoryBuffer(out var buffer))
        {
            var responseStream = RequestHandler.ResponseBodyStream();
            var count = stream.Read(buffer.Memory.Memory.Span); // can never wait, so no need for async
            while (count > 0)
            {
                await responseStream.WriteAsync(buffer.Memory.Memory.Slice(0, count), token);
                // we know that this can never wait, so no need to do async i/o here
                count = stream.Read(buffer.Memory.Memory.Span);
            }
        }
    }
}
