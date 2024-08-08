using System.Threading.Tasks;
using Raven.Server.Documents.ETL.Providers.RelationalDatabase.SQL.Handlers.Processors;
using Raven.Server.Documents.Sharding.Handlers.Processors.ETL;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public sealed class ShardedSqlEtlHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/admin/etl/sql/test-connection", "POST")]
        public async Task TestConnection()
        {
            using (var processor = new SqlEtlHandlerProcessorForTestConnection<TransactionOperationContext>(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/etl/sql/test", "POST")]
        public async Task Test()
        {
            using (var processor = new ShardedSqlEtlHandlerProcessorForTest(this))
                await processor.ExecuteAsync();
        }
    }
}
