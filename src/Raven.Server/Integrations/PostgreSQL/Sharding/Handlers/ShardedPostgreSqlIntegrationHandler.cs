using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.Documents.Sharding.Handlers.Processors;
using Raven.Server.Integrations.PostgreSQL.Handlers.Processors;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Integrations.PostgreSQL.Sharding.Handlers;

public sealed class ShardedPostgreSqlIntegrationHandler : ShardedDatabaseRequestHandler
{
    [RavenShardedAction("/databases/*/admin/integrations/postgresql/server/status", "GET")]
    public async Task GetServerStatus()
    {
        using (var processor = new PostgreSqlIntegrationHandlerProcessorForGetServerStatus<ShardedDatabaseRequestHandler, TransactionOperationContext>(this))
            await processor.ExecuteAsync();
    }

    [RavenShardedAction("/databases/*/admin/integrations/postgresql/users", "GET")]
    public async Task GetUsernamesList()
    {
        using (var processor = new NotSupportedInShardingProcessor(this, $"Database '{DatabaseName}' is a sharded database and does not support PostgreSQL."))
            await processor.ExecuteAsync();
    }

    [RavenShardedAction("/databases/*/admin/integrations/postgresql/user", "PUT")]
    public async Task AddUser()
    {
        using (var processor = new NotSupportedInShardingProcessor(this, $"Database '{DatabaseName}' is a sharded database and does not support PostgreSQL."))
            await processor.ExecuteAsync();
    }

    [RavenShardedAction("/databases/*/admin/integrations/postgresql/user", "DELETE")]
    public async Task DeleteUser()
    {
        using (var processor = new NotSupportedInShardingProcessor(this, $"Database '{DatabaseName}' is a sharded database and does not support PostgreSQL."))
            await processor.ExecuteAsync();
    }

    [RavenShardedAction("/databases/*/admin/integrations/postgresql/config", "POST")]
    public async Task PostPostgreSqlConfiguration()
    {
        using (var processor = new NotSupportedInShardingProcessor(this, $"Database '{DatabaseName}' is a sharded database and does not support PostgreSQL."))
            await processor.ExecuteAsync();
    }
}
