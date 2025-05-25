using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Attachments.Strategies;

public interface IDeleteAttachmentStrategy
{
    public AttachmentHandler.MergedDeleteAttachmentCommand CreateMergedDeleteAttachmentCommand(string docId, string name, LazyStringValue changeVector);
    public void CheckAttachmentFlagAndThrowIfNeeded(DocumentsOperationContext context, Attachment attachment, string docId, string name);
}
