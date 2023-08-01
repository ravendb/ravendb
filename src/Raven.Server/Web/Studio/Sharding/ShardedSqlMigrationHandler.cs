using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.Documents.Sharding.Handlers.Processors;
using Raven.Server.Routing;

namespace Raven.Server.Web.Studio.Sharding
{
    public sealed class ShardedSqlMigrationHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/smuggler/import/csv", "POST")]
        public async Task ImportFromCsv()
        {
            using (var processor = new NotSupportedInShardingProcessor(this, $"Database '{DatabaseName}' is a sharded database and does not support Admin sql-migration operations."))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/sql-migration/import", "POST")]
        public async Task ImportSql()
        {
            using (var processor = new NotSupportedInShardingProcessor(this, $"Database '{DatabaseName}' is a sharded database and does not support Admin sql-migration operations."))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/sql-migration/test", "POST")]
        public async Task TestSql()
        {
            using (var processor = new NotSupportedInShardingProcessor(this, $"Database '{DatabaseName}' is a sharded database and does not support Admin sql-migration operations."))
                await processor.ExecuteAsync();
        }
    }
}
