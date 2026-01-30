using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Attachments.Strategies;

internal abstract class AbstractGetAttachmentStrategyProcessor<TRequestHandler, TOperationContext> : AbstractAttachmentStrategyProcessor<TRequestHandler, TOperationContext>, IGetAttachmentStrategy
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractGetAttachmentStrategyProcessor([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    public abstract void CheckAttachmentFlagAndConfigurationAndThrowIfNeeded(DocumentsOperationContext context, Attachment attachment, string documentId, string name);
    public abstract Task WriteResponseStream(DocumentsOperationContext context, DocumentsTransaction tx, Attachment attachment, OperationCancelToken token);
}
