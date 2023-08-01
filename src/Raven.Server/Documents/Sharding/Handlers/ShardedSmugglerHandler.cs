using System;
using System.IO;
using System.IO.Compression;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Client.Exceptions.Security;
using Raven.Server.Documents.Handlers.Processors.Smuggler;
using Raven.Server.Documents.Sharding.Handlers.Processors;
using Raven.Server.Documents.Sharding.Handlers.Processors.Smuggler;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Smuggler.Documents.Data;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public sealed class ShardedSmugglerHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/smuggler/validate-options", "POST")]
        public async Task ValidateOptions()
        {
            using (var processor = new SmugglerHandlerProcessorForValidateOptions<TransactionOperationContext>(this))
            {
                await processor.ExecuteAsync();
            }
        }

        [RavenShardedAction("/databases/*/smuggler/export", "POST")]
        public async Task PostExport()
        {
            using (var processor = new ShardedSmugglerHandlerProcessorForExport(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/smuggler/import", "POST")]
        public async Task PostImportAsync()
        {
            using (var processor = new ShardedSmugglerHandlerProcessorForImport(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/smuggler/import-dir", "GET")]
        public async Task PostImportDirectory()
        {
            using (var processor = new ShardedSmugglerHandlerProcessorForImportDir(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/smuggler/import", "GET")]
        public async Task GetImport()
        {
            using (var processor = new ShardedSmugglerHandlerProcessorForImportGet(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/smuggler/import-s3-dir", "GET")]
        public async Task PostImportFromS3Directory()
        {
            using (var processor = new NotSupportedInShardingProcessor(this, $"Database '{DatabaseName}' is a sharded database and does not support Admin Smuggler operations."))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/smuggler/migrate/ravendb", "POST")]
        public async Task MigrateFromRavenDB()
        {
            using (var processor = new NotSupportedInShardingProcessor(this, $"Database '{DatabaseName}' is a sharded database and does not support Admin Smuggler operations."))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/migrate/get-migrated-server-urls", "GET")]
        public async Task GetMigratedServerUrls()
        {
            using (var processor = new NotSupportedInShardingProcessor(this, $"Database '{DatabaseName}' is a sharded database and does not support Get migrated server urls operation."))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/smuggler/migrate", "POST")]
        public async Task MigrateFromAnotherDatabase()
        {
            using (var processor = new NotSupportedInShardingProcessor(this, $"Database '{DatabaseName}' is a sharded database and does not support migrate from another database operation."))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/smuggler/import/csv", "POST")]
        public async Task ImportFromCsv()
        {
            using (var processor = new NotSupportedInShardingProcessor(this, $"Database '{DatabaseName}' is a sharded database and does not support Smuggler import CSV operation."))
                await processor.ExecuteAsync();
        }
    }
}
