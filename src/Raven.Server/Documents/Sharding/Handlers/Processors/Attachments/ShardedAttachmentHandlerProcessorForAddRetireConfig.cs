using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.Attachments.Retired;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Attachments;

internal sealed class ShardedAttachmentHandlerProcessorForAddRetireConfig : AbstractRetiredAttachmentHandlerProcessorForAddRetireConfig<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedAttachmentHandlerProcessorForAddRetireConfig([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }
}
