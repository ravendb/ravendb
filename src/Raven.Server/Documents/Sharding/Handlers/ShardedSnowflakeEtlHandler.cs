using System.Threading.Tasks;
using Raven.Server.Documents.ETL.Providers.Snowflake.Handlers.Processors;
using Raven.Server.Documents.Sharding.Handlers.Processors.ETL;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers;

public sealed class ShardedSnowflakeEtlHandler : ShardedDatabaseRequestHandler
{
    [RavenShardedAction("/databases/*/admin/etl/snowflake/test-connection", "POST")]
    public async Task TestConnection()
    {
        using (var processor = new SnowflakeEtlHandlerProcessorForTestConnection<TransactionOperationContext>(this))
            await processor.ExecuteAsync();
    }

    [RavenShardedAction("/databases/*/admin/etl/snowflake/test", "POST")]
    public async Task Test()
    {
        using (var processor = new ShardedSnowflakeEtlHandlerProcessorForTest(this))
            await processor.ExecuteAsync();
    }
}
