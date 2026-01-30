using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Attachments.Remote;

internal sealed class RemoteAttachmentHandlerProcessorForAddRemoteConfig : AbstractRemoteAttachmentHandlerProcessorForAddRemoteConfig<DatabaseRequestHandler, DocumentsOperationContext>
{
    public RemoteAttachmentHandlerProcessorForAddRemoteConfig([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }
}
