using System;
using System.IO;
using System.IO.Compression;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Documents.Handlers.Processors.Smuggler;
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
            using (var processor = new SmugglerHandlerProcessorForValidateOptions<TransactionOperationContext>(this, ContextPool))
            {
                await processor.ExecuteAsync();
            }
        }

        [RavenShardedAction("/databases/*/smuggler/export", "POST")]
        public async Task PostExport()
        {
            var result = new SmugglerResult();

            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var operationId = GetLongQueryString("operationId", false) ?? ServerStore.Operations.GetNextOperationId();

                try
                {
                    await Export(context, DatabaseContext.DatabaseName, ExportShardedDatabaseInternalAsync, ServerStore.Operations, operationId);
                }
                catch (Exception e)
                {
                    if (Logger.IsOperationsEnabled)
                        Logger.Operations("Export failed .", e);

                    result.AddError($"Error occurred during export. Exception: {e}");
                    await WriteResultAsync(context, result, ResponseBodyStream());

                    HttpContext.Abort();
                }
            }
        }

        public async Task<IOperationResult> ExportShardedDatabaseInternalAsync(
            DatabaseSmugglerOptionsServerSide options,
            long startDocumentEtag,
            long startRaftIndex,
            Action<IOperationProgress> onProgress,
            JsonOperationContext jsonOperationContext,
            OperationCancelToken token)
        {
            // we use here a negative number to avoid possible collision between the server and database ids
            var operationId = -ServerStore.Operations.GetNextOperationId();
            options.IsShard = true;

            await using (var outputStream = GetOutputStream(ResponseBodyStream(), options))
            await using (var writer = new AsyncBlittableJsonTextWriter(jsonOperationContext, new GZipStream(outputStream, CompressionMode.Compress)))
            {
                writer.WriteStartObject();
                writer.WritePropertyName("BuildVersion");
                writer.WriteInteger(ServerVersion.Build);

                var exportOperation = new ShardedExportOperation(this, operationId, options, writer);
                // we execute one by one so requests will not timeout since the export can take long
                await ShardExecutor.ExecuteOneByOneForAllAsync(exportOperation);

                writer.WriteEndObject();
            }

            var getStateOperation = new GetShardedOperationStateOperation(operationId);
            return (await ShardExecutor.ExecuteParallelForAllAsync(getStateOperation)).Result;
        }

        [RavenShardedAction("/databases/*/smuggler/import", "POST")]
        public async Task PostImportAsync()
        {
            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var operationId = GetLongQueryString("operationId", false) ?? Server.ServerStore.Operations.GetNextOperationId();

                await Import(context, DatabaseContext.DatabaseName, DoImportInternalAsync, Server.ServerStore.Operations, operationId);
            }
        }

        private async Task DoImportInternalAsync(
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
