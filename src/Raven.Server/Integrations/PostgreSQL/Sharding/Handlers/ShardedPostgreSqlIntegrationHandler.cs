using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.Integrations.PostgreSQL.Sharding.Handlers.Processors;
using Raven.Server.Routing;

namespace Raven.Server.Integrations.PostgreSQL.Sharding.Handlers;

public class ShardedPostgreSqlIntegrationHandler : ShardedDatabaseRequestHandler
{

    [RavenShardedAction("/databases/*/admin/integrations/postgresql/users", "GET")]
    public async Task GetUsernamesList()
    {
        using (var processor = new ShardedPostgreSqlIntegrationHandlerProcessorForGetUsernamesList(this))
            await processor.ExecuteAsync();
    }
}
