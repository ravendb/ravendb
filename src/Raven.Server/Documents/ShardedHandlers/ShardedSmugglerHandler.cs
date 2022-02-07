using System;
using System.IO;
using System.IO.Compression;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide;
using Raven.Server.Documents.ShardedHandlers.ShardedCommands;
using Raven.Server.Documents.Sharding;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Smuggler.Documents.Data;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ShardedHandlers
{
    public class ShardedSmugglerHandler : ShardedRequestHandler
    {
        [RavenShardedAction("/databases/*/smuggler/export", "POST")]
        public async Task PostExport()
        {
            var result = new SmugglerResult();

            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var operationId = GetLongQueryString("operationId", false) ?? ServerStore.Operations.GetNextOperationId();

                try
                {
                    await Export(context, ShardedContext.DatabaseName, (options, blittableOptions, startDocumentEtag, startRaftIndex, onProgress, _, token) =>
                            ExportShardedDatabaseInternalAsync(options, blittableOptions, startDocumentEtag, startRaftIndex, onProgress, context, token),
                        ServerStore.Operations, operationId);
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
            BlittableJsonReaderObject blittableJson,
            long startDocumentEtag,
            long startRaftIndex,
            Action<IOperationProgress> onProgress,
            JsonOperationContext jsonOperationContext,
            OperationCancelToken token)
        {
            blittableJson = CreateNewOptionBlittableJsonReaderObject(blittableJson, jsonOperationContext, nameof(options.IsShard), true);
            await using (var outputStream = GetOutputStream(ResponseBodyStream(), options))
            await using(var writer = new AsyncBlittableJsonTextWriter(jsonOperationContext, new GZipStream(outputStream, CompressionMode.Compress)))
            {
                writer.WriteStartObject();
                writer.WritePropertyName("BuildVersion");
                writer.WriteInteger(ServerVersion.Build);
                for (int i = 0; i < ShardedContext.ShardCount; i++)
                {
                    var cmd = new ShardedStreamCommand(this, async stream =>
                    {
                        await using (var gzipStream = new GZipStream(GetInputStream(stream, options), CompressionMode.Decompress))
                        {
                            writer.WriteStream(gzipStream);
                        }

                    }, blittableJson);

                    await ShardedContext.RequestExecutors[i].ExecuteAsync(cmd, jsonOperationContext);
                }
                writer.WriteEndObject();
            }
            return null;
        }

        private static BlittableJsonReaderObject CreateNewOptionBlittableJsonReaderObject(BlittableJsonReaderObject blittableJson, JsonOperationContext context,
            string key, object value)
        {
            blittableJson.Modifications = new DynamicJsonValue(blittableJson) {[key] = value};
            using (var old = blittableJson)
            {
                blittableJson = context.ReadObject(blittableJson, "convert/entityToBlittable");
            }

            return blittableJson;
        }

        [RavenShardedAction("/databases/*/smuggler/import", "POST")]
        public async Task PostImportAsync()
        {
            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var operationId = GetLongQueryString("operationId", false) ?? Server.ServerStore.Operations.GetNextOperationId();

                await Import(context, ShardedContext.DatabaseName, (_, stream, options, result, onProgress, token, blittableJson) =>
                    DoImportInternalAsync(context, stream, options, result, onProgress, token, blittableJson), Server.ServerStore.Operations, operationId);
            }
        }

        private async Task DoImportInternalAsync(JsonOperationContext jsonOperationContext, Stream stream,
            DatabaseSmugglerOptionsServerSide options,
            SmugglerResult result, Action<IOperationProgress> onProgress,
            OperationCancelToken token, BlittableJsonReaderObject optionsAsBlittable)
        {
            using (ContextPool.AllocateOperationContext(out TransactionOperationContext tContext))
            using (var source = new StreamSource(stream, jsonOperationContext, ShardedContext.DatabaseName, options))
            {
                DatabaseRecord record;
                using (tContext.OpenReadTransaction())
                {
                    record = Server.ServerStore.Cluster.ReadDatabase(tContext, ShardedContext.DatabaseName);
                }

                var smuggler = new ShardedDatabaseSmuggler(ContextPool, source, jsonOperationContext, tContext, record,
                    Server.ServerStore, ShardedContext, this, optionsAsBlittable, options, result,
                    onProgress, token: token.Token);

                await smuggler.ExecuteAsync();
            }
        }
    }
}
