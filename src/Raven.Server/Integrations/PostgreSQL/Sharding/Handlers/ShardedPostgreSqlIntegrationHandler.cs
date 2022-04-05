using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.Integrations.PostgreSQL.Handlers.Processors;
using Raven.Server.Integrations.PostgreSQL.Sharding.Handlers.Processors;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Integrations.PostgreSQL.Sharding.Handlers;

public class ShardedPostgreSqlIntegrationHandler : ShardedDatabaseRequestHandler
{
    [RavenShardedAction("/databases/*/admin/integrations/postgresql/server/status", "GET")]
    public async Task GetServerStatus()
    {
        using (var processor = new PostgreSqlIntegrationHandlerProcessorForGetServerStatus<TransactionOperationContext>(this, ContextPool))
            await processor.ExecuteAsync();
    }

    [RavenShardedAction("/databases/*/admin/integrations/postgresql/users", "GET")]
    public async Task GetUsernamesList()
    {
        using (var processor = new ShardedPostgreSqlIntegrationHandlerProcessorForGetUsernamesList(this))
            await processor.ExecuteAsync();
    }

    [RavenShardedAction("/databases/*/admin/integrations/postgresql/user", "PUT")]
    public async Task AddUser()
    {
        using (var processor = new ShardedPostgreSqlIntegrationHandlerProcessorForAddUser(this))
            await processor.ExecuteAsync();
    }
}
