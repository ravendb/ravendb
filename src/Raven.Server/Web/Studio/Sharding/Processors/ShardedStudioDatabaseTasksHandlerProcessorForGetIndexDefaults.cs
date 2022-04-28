using JetBrains.Annotations;
using Raven.Server.Config;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Studio.Processors;

namespace Raven.Server.Web.Studio.Sharding.Processors;

internal class ShardedStudioDatabaseTasksHandlerProcessorForGetIndexDefaults : AbstractStudioDatabaseTasksHandlerProcessorForGetIndexDefaults<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedStudioDatabaseTasksHandlerProcessorForGetIndexDefaults([NotNull] ShardedDatabaseRequestHandler requestHandler)
        : base(requestHandler)
    {
    }

    protected override RavenConfiguration GetDatabaseConfiguration() => RequestHandler.DatabaseContext.Configuration;
}
