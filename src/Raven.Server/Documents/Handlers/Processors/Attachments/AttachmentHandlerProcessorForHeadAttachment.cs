﻿using System;
using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Client.Documents.Attachments;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Attachments;

internal class AttachmentHandlerProcessorForHeadAttachment : AbstractAttachmentHandlerProcessorForHeadAttachment<DatabaseRequestHandler, DocumentsOperationContext>
{
    public AttachmentHandlerProcessorForHeadAttachment([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    public virtual string CheckAttachmentFlagAndConfigurationAndThrowIfNeeded(DocumentsOperationContext context, Attachment attachment, string documentId, string name)
    {
        if (attachment.Flags.HasFlag(AttachmentFlags.Retired))
        {
            throw new InvalidOperationException($"Cannot get attachment '{name}' on document '{documentId}' because it is retired. Please use dedicated API.");
        }

        return null;
    }

    protected override ValueTask HandleHeadAttachmentAsync(string documentId, string name, string changeVector)
    {
        using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (context.OpenReadTransaction())
        {
            var attachment = RequestHandler.Database.DocumentsStorage.AttachmentsStorage.GetAttachment(context, documentId, name, AttachmentType.Document, null);
            if (attachment == null)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                return ValueTask.CompletedTask;
            }

            CheckAttachmentFlagAndConfigurationAndThrowIfNeeded(context, attachment, documentId, name);

            if (changeVector == attachment.ChangeVector)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                return ValueTask.CompletedTask;
            }

            HttpContext.Response.Headers[Constants.Headers.Etag] = $"\"{attachment.ChangeVector}\"";

            return ValueTask.CompletedTask;
        }
    }
}
