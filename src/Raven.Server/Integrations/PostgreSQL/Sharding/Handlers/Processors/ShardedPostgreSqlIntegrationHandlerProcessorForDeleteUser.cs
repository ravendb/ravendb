using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.Integrations.PostgreSQL.Handlers.Processors;

namespace Raven.Server.Integrations.PostgreSQL.Sharding.Handlers.Processors;

internal class ShardedPostgreSqlIntegrationHandlerProcessorForDeleteUser : AbstractPostgreSqlIntegrationHandlerProcessorForDeleteUser<ShardedDatabaseRequestHandler>
{
    public ShardedPostgreSqlIntegrationHandlerProcessorForDeleteUser([NotNull] ShardedDatabaseRequestHandler requestHandler) 
        : base(requestHandler)
    {
    }

    protected override string GetDatabaseName() => RequestHandler.DatabaseContext.DatabaseName;

    protected override ValueTask WaitForIndexNotificationAsync(long index) => RequestHandler.DatabaseContext.Cluster.WaitForExecutionOnAllNodesAsync(index);
}
