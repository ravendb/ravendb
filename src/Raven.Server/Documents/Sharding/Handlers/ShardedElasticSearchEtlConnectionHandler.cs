using System.Threading.Tasks;
using Raven.Server.Documents.ETL.Providers.ElasticSearch.Handlers.Processors;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers;

public class ShardedElasticSearchEtlConnectionHandler : ShardedDatabaseRequestHandler
{
    [RavenShardedAction("/databases/*/admin/etl/elasticsearch/test-connection", "POST")]
    public async Task TestConnection()
    {
        using (var processor = new ElasticSearchEtlConnectionHandlerForTestConnection<TransactionOperationContext>(this))
            await processor.ExecuteAsync();
    }
}
