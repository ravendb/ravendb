using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Processors.Smuggler;
using Raven.Server.Documents.Sharding.Handlers.Processors.Smuggler;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public class ShardedSmugglerHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/smuggler/validate-options", "POST")]
        public async Task ValidateOptions()
        {
            using (var processor = new SmugglerHandlerProcessorForValidateOptions<TransactionOperationContext>(this, DatabaseContext.Configuration))
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
    }
}
