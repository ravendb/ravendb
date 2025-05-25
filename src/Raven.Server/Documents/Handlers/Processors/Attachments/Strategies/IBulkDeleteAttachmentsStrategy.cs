using System.Collections.Generic;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Attachments.Strategies;

public interface IBulkDeleteAttachmentsStrategy
{
    public MergedDeleteAttachmentsCommand MergedDeleteAttachmentsCommand(List<AttachmentRequest> attachmentRequests);
    public void CheckAttachmentFlagAndThrowIfNeeded(DocumentsOperationContext context, Attachment attachment, string docId, string name);
}
