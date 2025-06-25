using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using static Raven.Server.Documents.Handlers.AttachmentHandler;

namespace Raven.Server.Documents.Handlers.Processors.Attachments.Strategies;

internal abstract class AbstractDeleteAttachmentStrategyProcessor<TRequestHandler, TOperationContext> : AbstractAttachmentStrategyProcessor<TRequestHandler, TOperationContext>, IDeleteAttachmentStrategy
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractDeleteAttachmentStrategyProcessor([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    public abstract MergedDeleteAttachmentCommand CreateMergedDeleteAttachmentCommand(string docId, string name, LazyStringValue changeVector);
    public abstract void CheckAttachmentFlagAndThrowIfNeeded(DocumentsOperationContext context, Attachment attachment, string docId, string name);
}
