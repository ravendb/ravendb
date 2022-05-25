using System;
using System.IO;
using System.IO.Compression;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide;
using Raven.Server.Documents.Handlers.Processors.Smuggler;
using Raven.Server.Documents.Sharding.Handlers.Processors.Smuggler;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Smuggler.Documents.Data;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public class ShardedSmugglerHandler : ShardedDatabaseRequestHandler
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

        public async Task DoImportInternalAsync(
            JsonOperationContext jsonOperationContext,
            Stream stream,
            DatabaseSmugglerOptionsServerSide options,
            SmugglerResult result,
            Action<IOperationProgress> onProgress,
            OperationCancelToken token)
        {
            using (var source = new StreamSource(stream, jsonOperationContext, DatabaseContext.DatabaseName, options))
            {
                DatabaseRecord record;
                using (ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    record = Server.ServerStore.Cluster.ReadDatabase(ctx, DatabaseContext.DatabaseName);
                }

                var smuggler = new ShardedDatabaseSmuggler(source, jsonOperationContext, record,
                    Server.ServerStore, DatabaseContext, this, options, result,
                    onProgress, token: token.Token);

                await smuggler.ExecuteAsync();
            }
        }
    }
}
