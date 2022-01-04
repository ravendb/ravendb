using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.IO.Pipelines;
using System.Text;
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

                    result.AddError($"Error occurred during export. Exception: {e.Message}");
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
            var tcs = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);

            var oldOperateOnType = options.OperateOnTypes;

            var operateOnTypes = options.OperateOnTypes &= ~(DatabaseItemType.DatabaseRecord |
                                                             DatabaseItemType.Subscriptions |
                                                             DatabaseItemType.Identities |
                                                             DatabaseItemType.Indexes |
                                                             DatabaseItemType.ReplicationHubCertificates);

            blittableJson = CreateNewOptionBlittableJsonReaderObject(blittableJson, jsonOperationContext, nameof(options.OperateOnTypes), operateOnTypes);

            using (var outputStream = GetOutputStream(ResponseBodyStream(), options))
            using (var exportOutputStream = new GZipStream(outputStream, CompressionMode.Compress))
            {
                for (int i = 0; i < ShardedContext.ShardCount; i++)
                {
                    if (i > 0) //Only first shard write build version
                    {
                        blittableJson = CreateNewOptionBlittableJsonReaderObject(blittableJson, jsonOperationContext, nameof(options.SkipBuildVersion), true);
                    }

                    if (i == ShardedContext.ShardCount - 1) // Last shard need to bring all server wide information
                    {
                        blittableJson = CreateNewOptionBlittableJsonReaderObject(blittableJson, jsonOperationContext, nameof(options.OperateOnTypes), oldOperateOnType);

                    }

                    var cmd = new ShardedStreamCommand(this, async stream =>
                    {
                        using (var gzipStream = new GZipStream(GetInputStream(stream, options), CompressionMode.Decompress))
                        {
                            var reader = PipeReader.Create(gzipStream);
                            var firstLoop = true;
                            while (true)
                            {
                                var result = await reader.ReadAsync();
                                var buffer = result.Buffer;
                                reader.AdvanceTo(buffer.Start, buffer.End);

                                if (result.IsCompleted)
                                    break;

                                await exportOutputStream.WriteAsync(new ReadOnlyMemory<byte>(result.Buffer.ToArray(), 0, (int)result.Buffer.Length - 1));

                                if (firstLoop)
                                {
                                    reader.AdvanceTo(buffer.GetPosition(result.Buffer.First.Length - 1), buffer.End);
                                    firstLoop = false;
                                    continue;
                                }

                                reader.AdvanceTo(buffer.GetPosition(result.Buffer.Length - 1), buffer.End);
                            }
                        }

                    }, tcs, blittableJson);

                    await ShardedContext.RequestExecutors[i].ExecuteAsync(cmd, jsonOperationContext);
                }
                await exportOutputStream.WriteAsync(Encoding.UTF8.GetBytes("}"));
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

        private async Task DoImportInternalAsync(JsonOperationContext context, Stream stream,
            DatabaseSmugglerOptionsServerSide options,
            SmugglerResult result, Action<IOperationProgress> onProgress,
            OperationCancelToken token, BlittableJsonReaderObject optionsAsBlittable)
        {
            var contextList = new List<JsonOperationContext>();
            var destinationList = new List<ISmugglerDestination>();
            try
            {
                for (int i = 0; i < ShardedContext.ShardCount; i++)
                {
                    ContextPool.AllocateOperationContext(out JsonOperationContext jsonOperationContext);
                    contextList.Add(jsonOperationContext);
                }

                var tcs = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);

                var streamList = new List<Stream>();
                for (int i = 0; i < ShardedContext.ShardCount; i++)
                {
                    streamList.Add(GetOutputStream(new MemoryStream(), options));
                }

                using (ContextPool.AllocateOperationContext(out TransactionOperationContext tContext))
                using (var source = new StreamSource(stream, context, ShardedContext.DatabaseName, options))
                {
                    DatabaseRecord record;
                    using (tContext.OpenReadTransaction())
                    {
                        record = Server.ServerStore.Cluster.ReadDatabase(tContext, ShardedContext.DatabaseName);
                    }

                    for (int i = 0; i < ShardedContext.ShardCount; i++)
                    {
                        destinationList.Add(new StreamDestination(streamList[i], contextList[i], source));
                    }

                    var smuggler = new ShardedDatabaseSmuggler(source, destinationList, context, tContext, record,
                        Server.ServerStore, Server.Time, ShardedContext, this, contextList, optionsAsBlittable, streamList, tcs, options, result,
                        onProgress, token: token.Token);

                    await smuggler.ExecuteAsync();
                }

            }
            finally
            {
                for (int i = 0; i < ShardedContext.ShardCount; i++)
                {
                    contextList[i].Dispose();
                }
            }
        }
    }
}
