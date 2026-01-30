using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.Attachments.Remote;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Attachments;

internal sealed class ShardedAttachmentHandlerProcessorForAddRemoteConfig : AbstractRemoteAttachmentHandlerProcessorForAddRemoteConfig<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedAttachmentHandlerProcessorForAddRemoteConfig([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }
}
