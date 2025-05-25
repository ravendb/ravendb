using System.Collections.Generic;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Attachments.Strategies;

internal abstract class AbstractBulkDeleteAttachmentsStrategyProcessor<TRequestHandler, TOperationContext> : AbstractAttachmentStrategyProcessor<TRequestHandler, TOperationContext>, IBulkDeleteAttachmentsStrategy
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractBulkDeleteAttachmentsStrategyProcessor([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }
    public abstract MergedDeleteAttachmentsCommand MergedDeleteAttachmentsCommand(List<AttachmentRequest> attachmentRequests);
    public abstract void CheckAttachmentFlagAndThrowIfNeeded(DocumentsOperationContext context, Attachment attachment, string docId, string name);
}
