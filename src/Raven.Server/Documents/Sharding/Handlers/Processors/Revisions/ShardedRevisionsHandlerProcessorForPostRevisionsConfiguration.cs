using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.Revisions;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Revisions;

internal sealed class ShardedRevisionsHandlerProcessorForPostRevisionsConfiguration : AbstractRevisionsHandlerProcessorForPostRevisionsConfiguration<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedRevisionsHandlerProcessorForPostRevisionsConfiguration([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }
}
