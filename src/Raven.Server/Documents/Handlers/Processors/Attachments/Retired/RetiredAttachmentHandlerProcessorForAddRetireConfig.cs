using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Attachments.Retired;

internal sealed class RetiredAttachmentHandlerProcessorForAddRetireConfig : AbstractRetiredAttachmentHandlerProcessorForAddRetireConfig<DatabaseRequestHandler, DocumentsOperationContext>
{
    public RetiredAttachmentHandlerProcessorForAddRetireConfig([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }
}
