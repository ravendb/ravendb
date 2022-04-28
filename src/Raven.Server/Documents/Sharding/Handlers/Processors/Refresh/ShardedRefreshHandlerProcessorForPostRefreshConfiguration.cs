using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.Refresh;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Refresh;

internal class ShardedRefreshHandlerProcessorForPostRefreshConfiguration : AbstractRefreshHandlerProcessorForPostRefreshConfiguration<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedRefreshHandlerProcessorForPostRefreshConfiguration([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }
}
