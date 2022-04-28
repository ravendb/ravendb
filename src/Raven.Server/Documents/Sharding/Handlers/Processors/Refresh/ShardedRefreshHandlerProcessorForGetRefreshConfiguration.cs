using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Refresh;
using Raven.Server.Documents.Handlers.Processors.Refresh;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Refresh;

internal class ShardedRefreshHandlerProcessorForGetRefreshConfiguration : AbstractRefreshHandlerProcessorForGetRefreshConfiguration<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedRefreshHandlerProcessorForGetRefreshConfiguration([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override RefreshConfiguration GetRefreshConfiguration() => RequestHandler.DatabaseContext.DatabaseRecord.Refresh;
}
