using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.Revisions;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Revisions;

internal sealed class ShardedRevisionsBinCleanerHandlerProcessorForPostConfiguration : AbstractRevisionsBinCleanerHandlerProcessorForPostConfiguration<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedRevisionsBinCleanerHandlerProcessorForPostConfiguration([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }
}
