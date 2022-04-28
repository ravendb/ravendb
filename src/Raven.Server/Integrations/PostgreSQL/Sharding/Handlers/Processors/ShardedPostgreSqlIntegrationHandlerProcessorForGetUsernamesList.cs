using JetBrains.Annotations;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.Integrations.PostgreSQL.Handlers.Processors;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Integrations.PostgreSQL.Sharding.Handlers.Processors;

internal class ShardedPostgreSqlIntegrationHandlerProcessorForGetUsernamesList : AbstractPostgreSqlIntegrationHandlerProcessorForGetUsernamesList<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedPostgreSqlIntegrationHandlerProcessorForGetUsernamesList([NotNull] ShardedDatabaseRequestHandler requestHandler)
        : base(requestHandler)
    {
    }
}
