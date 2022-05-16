using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Operations.Processors;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Operations;

internal class ShardedOperationsHandlerProcessorForGetNextOperationId : AbstractOperationsHandlerProcessorForGetNextOperationId<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedOperationsHandlerProcessorForGetNextOperationId([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override long GetNextOperationId() => RequestHandler.DatabaseContext.Operations.GetNextOperationId();
}
