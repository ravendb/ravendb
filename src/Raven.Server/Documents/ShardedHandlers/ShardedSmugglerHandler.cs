using System;
using System.Buffers;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.IO.Pipelines;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.WebUtilities;
using Microsoft.Net.Http.Headers;
using Raven.Client;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide;
using Raven.Client.Util;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Documents.ShardedHandlers.ShardedCommands;
using Raven.Server.Documents.Sharding;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Smuggler.Documents.Data;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

namespace Raven.Server.Documents.ShardedHandlers
{
    public class ShardedSmugglerHandler : ShardedRequestHandler
    {
        [RavenShardedAction("/databases/*/smuggler/export", "POST")]
        public async Task PostExport()
        {
            Logger logger = LoggingSource.Instance.GetLogger("PostExport", typeof(ShardedSmugglerHandler).FullName);
            var result = new SmugglerResult();
            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                BlittableJsonReaderObject blittableJson = null;
                DatabaseSmugglerOptionsServerSide options;

                var stream = TryGetRequestFromStream("DownloadOptions") ?? RequestBodyStream();

                using (context.GetMemoryBuffer(out var buffer))
                {
                    var firstRead = await stream.ReadAsync(buffer.Memory.Memory);
                    buffer.Used = 0;
                    buffer.Valid = firstRead;
                    if (firstRead != 0)
                    {
                        blittableJson = await context.ParseToMemoryAsync(stream, "DownloadOptions", BlittableJsonDocumentBuilder.UsageMode.None, buffer);
                        options = JsonDeserializationServer.DatabaseSmugglerOptions(blittableJson);
                    }
                    else
                    {
                        // no content, we'll use defaults
                        options = new DatabaseSmugglerOptionsServerSide();
                    }

                    var fileName = options.FileName;
                    if (string.IsNullOrEmpty(fileName))
                    {
                        fileName = $"Dump of {ShardedContext.DatabaseName} {SystemTime.UtcNow.ToString("yyyy-MM-dd HH-mm", CultureInfo.InvariantCulture)}";
                    }

                    var contentDisposition = "attachment; filename=" + Uri.EscapeDataString(fileName) + ".ravendbdump";
                    HttpContext.Response.Headers["Content-Disposition"] = contentDisposition;
                    HttpContext.Response.Headers["Content-Type"] = "application/octet-stream";

                    try
                    {
                        var operationId = GetLongQueryString("operationId", false) ?? ServerStore.Operations.GetNextOperationId();
                        var token = CreateOperationToken();

                        await ServerStore.Operations.AddOperation(
                            null,
                            "Export database: " + ShardedContext.DatabaseName,
                            Operations.Operations.OperationType.DatabaseExport,
                            onProgress => ExportShardedDatabaseInternalAsync(options, fileName, blittableJson, context, logger, token),
                            operationId, token: token);
                    }
                    catch (Exception e)
                    {
                        if (logger.IsOperationsEnabled)
                            logger.Operations("Export failed .", e);

                        result.AddError($"Error occurred during export. Exception: {e.Message}");
                        await WriteResultAsync(context, result, ResponseBodyStream());

                        HttpContext.Abort();
                    }
                }
            }
        }

        public async Task<IOperationResult> ExportShardedDatabaseInternalAsync(
            DatabaseSmugglerOptionsServerSide options,
            string fileName,
            BlittableJsonReaderObject blittableJson,
            JsonOperationContext context,
            Logger logger,
            OperationCancelToken token)
        {
            var tcs = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);

            var oldOperateOnType = options.OperateOnTypes;

            var operateOnTypes = options.OperateOnTypes &= ~(DatabaseItemType.DatabaseRecord |
                                                             DatabaseItemType.Subscriptions |
                                                             DatabaseItemType.Identities |
                                                             DatabaseItemType.Indexes |
                                                             DatabaseItemType.ReplicationHubCertificates);

            blittableJson = CreateNewOptionBlittableJsonReaderObject(blittableJson, context, "OperateOnTypes", operateOnTypes);

            using (var outputStream = GetOutputStream(ResponseBodyStream(), options))
            using (var exportOutputStream = new GZipStream(outputStream, CompressionMode.Compress))
            {
                for (int i = 0; i < ShardedContext.ShardCount; i++)
                {
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

                    await ShardedContext.RequestExecutors[i].ExecuteAsync(cmd, context);
                    if (i == ShardedContext.ShardCount - 1)
                    {
                        await exportOutputStream.WriteAsync(Encoding.UTF8.GetBytes("}"));
                    }
                    else
                    {
                        if (i == 0) //Only first shard write build version
                        {
                            blittableJson = CreateNewOptionBlittableJsonReaderObject(blittableJson, context, "SkipBuildVersion", true);
                        }

                        if (i == ShardedContext.ShardCount - 2) // Last shard need to bring all server wide information
                        {
                            blittableJson = CreateNewOptionBlittableJsonReaderObject(blittableJson, context, "OperateOnTypes", oldOperateOnType);

                        }
                    }
                }
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

        private Stream GetOutputStream(Stream fileStream, DatabaseSmugglerOptionsServerSide options)
        {
            if (options.EncryptionKey == null)
                return fileStream;

            var key = options?.EncryptionKey;
            return new EncryptingXChaCha20Poly1305Stream(fileStream,
                Convert.FromBase64String(key));
        }

        private Stream GetInputStream(Stream fileStream, DatabaseSmugglerOptionsServerSide options)
        {
            if (options.EncryptionKey != null)
                return new DecryptingXChaCha20Oly1305Stream(fileStream, Convert.FromBase64String(options.EncryptionKey));

            return fileStream;
        }

        [RavenShardedAction("/databases/*/smuggler/import", "POST")]
        public async Task PostImportAsync()
        {
            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                if (HttpContext.Request.HasFormContentType == false)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.BadRequest; // Bad request
                    await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        context.Write(writer, new DynamicJsonValue {["Type"] = "Error", ["Error"] = "This endpoint requires form content type"});
                        return;
                    }
                }

                var operationId = GetLongQueryString("operationId", false) ?? Server.ServerStore.Operations.GetNextOperationId();
                var token = CreateOperationToken();

                var result = new SmugglerResult();
                BlittableJsonReaderObject blittableJson = null;
                await Server.ServerStore.Operations.AddOperation(null, "Import to: ",
                    Operations.Operations.OperationType.DatabaseImport,
                    onProgress =>
                    {
                        return Task.Run(async () =>
                        {
                            try
                            {
                                var boundary = MultipartRequestHelper.GetBoundary(
                                    MediaTypeHeaderValue.Parse(HttpContext.Request.ContentType),
                                    MultipartRequestHelper.MultipartBoundaryLengthLimit);
                                var reader = new MultipartReader(boundary, HttpContext.Request.Body);
                                DatabaseSmugglerOptionsServerSide options = null;

                                while (true)
                                {
                                    var section = await reader.ReadNextSectionAsync().ConfigureAwait(false);
                                    if (section == null)
                                        break;

                                    if (ContentDispositionHeaderValue.TryParse(section.ContentDisposition, out ContentDispositionHeaderValue contentDisposition) == false)
                                        continue;

                                    if (MultipartRequestHelper.HasFormDataContentDisposition(contentDisposition))
                                    {
                                        var key = HeaderUtilities.RemoveQuotes(contentDisposition.Name);
                                        if (key != Constants.Smuggler.ImportOptions)
                                            continue;


                                        if (section.Headers.ContainsKey("Content-Encoding") && section.Headers["Content-Encoding"] == "gzip")
                                        {
                                            await using (var gzipStream = new GZipStream(section.Body, CompressionMode.Decompress))
                                            {
                                                blittableJson = await context.ReadForMemoryAsync(gzipStream, Constants.Smuggler.ImportOptions);
                                            }
                                        }
                                        else
                                        {
                                            blittableJson = await context.ReadForMemoryAsync(section.Body, Constants.Smuggler.ImportOptions);
                                        }

                                        options = JsonDeserializationServer.DatabaseSmugglerOptions(blittableJson);
                                        continue;
                                    }

                                    if (MultipartRequestHelper.HasFileContentDisposition(contentDisposition) == false)
                                        continue;

                                    var inputStream = GetInputStream(section.Body, options);
                                    var stream = new GZipStream(inputStream, CompressionMode.Decompress);
                                    await DoImportInternalAsync(context, stream, blittableJson, options, result, onProgress, token);
                                }
                            }
                            catch (Exception e)
                            {
                                result.AddError($"Error occurred during import. Exception: {e}");
                                onProgress.Invoke(result.Progress);
                                throw;
                            }

                            return (IOperationResult)result;
                        });
                    }, operationId, token: token).ConfigureAwait(false);

                await WriteResultAsync(context, result, ResponseBodyStream());
            }
        }

        private async Task DoImportInternalAsync(JsonOperationContext context, Stream stream, BlittableJsonReaderObject optionsAsBlittable,
            DatabaseSmugglerOptionsServerSide options, SmugglerResult result, Action<IOperationProgress> onProgress,
            OperationCancelToken token)
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



        private static async ValueTask WriteResultAsync(JsonOperationContext context, SmugglerResult result, Stream stream)
        {
            await using (var writer = new AsyncBlittableJsonTextWriter(context, stream))
            {
                var json = result.ToJson();
                context.Write(writer, json);
            }
        }
    }
}
