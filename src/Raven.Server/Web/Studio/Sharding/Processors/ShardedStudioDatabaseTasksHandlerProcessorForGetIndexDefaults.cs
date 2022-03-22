using JetBrains.Annotations;
using Raven.Server.Config;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Studio.Processors;

namespace Raven.Server.Web.Studio.Sharding.Processors;

internal class ShardedStudioDatabaseTasksHandlerProcessorForGetIndexDefaults : AbstractStudioDatabaseTasksHandlerProcessorForGetIndexDefaults<ShardedRequestHandler, TransactionOperationContext>
{
    public ShardedStudioDatabaseTasksHandlerProcessorForGetIndexDefaults([NotNull] ShardedRequestHandler requestHandler)
        : base(requestHandler, requestHandler.ContextPool)
    {
    }

    protected override RavenConfiguration GetDatabaseConfiguration() => RequestHandler.ShardedContext.Configuration;
}
