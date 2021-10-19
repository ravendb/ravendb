using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.WebUtilities;
using Microsoft.Net.Http.Headers;
using Nito.AsyncEx;
using Raven.Client;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Json;
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
            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                BlittableJsonReaderObject blittableJson = null;
                DatabaseSmugglerOptionsServerSide options;

                //TODO - startEtag and startRaftIndex 
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
                            onProgress => ExportShardedDatabaseInternalAsync(options, fileName, onProgress, blittableJson, context, logger, token),
                            operationId, token: token);
                    }
                    catch (Exception)
                    {
                        HttpContext.Abort();
                    }
                }
            }
        }

        public async Task<IOperationResult> ExportShardedDatabaseInternalAsync(
            DatabaseSmugglerOptionsServerSide options,
            string fileName,
            Action<IOperationProgress> onProgress,
            BlittableJsonReaderObject blittableJson,
            JsonOperationContext context,
            Logger logger,
            OperationCancelToken token)
        {
            var tcs = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);

            var first = true;
            using (var outputStream = GetOutputStream(ResponseBodyStream(), options))
            using (var fileStream = new GZipStream(outputStream, CompressionMode.Compress))
            {
                for (int i = 0; i < ShardedContext.ShardCount; i++)
                {
                    var cmd = new ShardedStreamCommand(this, async stream =>
                    {
                        try
                        {
                            //Todo - Handler build version and change to pipreader
                            var tempStream = new byte[8193];
                            using (var gzipStream = new GZipStream(GetInputStream(stream, options), CompressionMode.Decompress))
                            {
                                if (first == false)
                                {
                                    var t = new byte[18];
                                    await gzipStream.ReadAsync(t, new CancellationToken());
                                }

                                var result = await FillBuffer(gzipStream, tempStream, isFirst: true, 0);

                                while (result.HasMore)
                                {
                                    await fileStream.WriteAsync(new ReadOnlyMemory<byte>(tempStream, 0, result.Read - 1), new CancellationToken());
                                    result = await FillBuffer(gzipStream, tempStream, isFirst: false, result.Read - 1);
                                }
                                await fileStream.WriteAsync(tempStream, 0, result.Read - 1);
                            }
                        }
                        catch (Exception e)
                        {
                            if (logger.IsOperationsEnabled)
                                logger.Operations("Could not save export file.", e);

                            tcs.TrySetException(e);

                            if (e is UnauthorizedAccessException || e is DirectoryNotFoundException || e is IOException)
                                throw new InvalidOperationException($"Cannot export to selected path {fileName}, please ensure you selected proper filename.", e);

                            throw new InvalidOperationException($"Could not save export file {fileName}.", e);
                        }
                    }, tcs, blittableJson);

                    await ShardedContext.RequestExecutors[i].ExecuteAsync(cmd, context);
                    if (i == ShardedContext.ShardCount - 1)
                    {
                        await fileStream.WriteAsync(Encoding.UTF8.GetBytes("}"));
                    }
                    else
                    {
                        if (first)
                        {
                            //DatabaseRecord,  Subscriptions, Identities ... need to export only from one shard
                            var operateOnTypes = options.OperateOnTypes &= ~(DatabaseItemType.DatabaseRecord |
                                                                             DatabaseItemType.Subscriptions |
                                                                             DatabaseItemType.Identities |
                                                                             DatabaseItemType.Indexes |
                                                                             DatabaseItemType.ReplicationHubCertificates);

                            blittableJson.Modifications = new DynamicJsonValue(blittableJson)
                            {
                                ["OperateOnTypes"] = operateOnTypes
                            };
                            using (var old = blittableJson)
                            {
                                blittableJson = context.ReadObject(blittableJson, "convert/entityToBlittable");
                            }

                            first = false;
                        }
                    }
                }
            }
            return null;
        }

        private static async Task<(bool HasMore, int Read)> FillBuffer(GZipStream gzipStream, byte[] tempStream, bool isFirst, int end)
        {
            var hasMore = false;
            var start = isFirst ? 0 : 1;
            var add = 0;
            if (isFirst == false)
            {
                tempStream[0] = tempStream[end];
                add++;
            }

            var num = await gzipStream.ReadAsync(new Memory<byte>(tempStream, start, 8192 - start), new CancellationToken());
            num += add;
            if (await gzipStream.ReadAsync(new Memory<byte>(tempStream, num, 1), new CancellationToken()) == 1)
            {
                num++;
                hasMore = true;
            }
            
            

            return (hasMore, num);
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
                        context.Write(writer, new DynamicJsonValue { ["Type"] = "Error", ["Error"] = "This endpoint requires form content type" });
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
                                result.AddError($"Error occurred during import. Exception: {e.Message}");
                                onProgress.Invoke(result.Progress);
                                throw;
                            }

                            return (IOperationResult)result;
                        });
                    }, operationId, token: token).ConfigureAwait(false);

                await WriteImportResultAsync(context, result, ResponseBodyStream());
            }
        }

        private async Task DoImportInternalAsync(JsonOperationContext context, Stream stream, BlittableJsonReaderObject optionsAsBlittable,DatabaseSmugglerOptionsServerSide options, SmugglerResult result, Action<IOperationProgress> onProgress,
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
                using (tContext.OpenReadTransaction())
                {
                    var record = Server.ServerStore.Cluster.ReadDatabase(tContext, ShardedContext.DatabaseName);

                    for (int i = 0; i < ShardedContext.ShardCount; i++)
                    {
                        destinationList.Add(new StreamDestination(streamList[i], contextList[i], source));
                    }

                    var smuggler = new ShardedDatabaseSmuggler(source, destinationList, context, tContext, record, 
                        Server.ServerStore, Server.Time, options, result,
                        onProgress, token: token.Token);


                    smuggler.ImportBatch = new ImportBatch(context, optionsAsBlittable, streamList, tcs, contextList, this, ShardedContext);
                    await smuggler.ExecuteAsync();
                    var task =  smuggler.ImportBatch.SendImportBatch();
                    task.Wait();

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

        private static async ValueTask WriteImportResultAsync(JsonOperationContext context, SmugglerResult result, Stream stream)
        {
            await using (var writer = new AsyncBlittableJsonTextWriter(context, stream))
            {
                var json = result.ToJson();
                context.Write(writer, json);
            }
        }

        public class ImportBatch
        {
            private readonly JsonOperationContext _context;
            private readonly BlittableJsonReaderObject _optionsAsBlittable;
            private readonly List<Stream> _streamList;
            private readonly TaskCompletionSource<object> _tcs;
            private readonly List<JsonOperationContext> _contextList;
            private readonly ShardedSmugglerHandler _handler;
            private readonly ShardedContext _shardedContext;

            public ImportBatch(JsonOperationContext context,
                BlittableJsonReaderObject optionsAsBlittable,
                List<Stream> streamList,
                TaskCompletionSource<object> tcs,
                List<JsonOperationContext> contextList,
                ShardedSmugglerHandler handler,
                ShardedContext shardedContext)
            {
                _context = context;
                _optionsAsBlittable = optionsAsBlittable;
                _streamList = streamList;
                _tcs = tcs;
                _contextList = contextList;
                _handler = handler;
                _shardedContext = shardedContext;
            }

            public async Task SendImportBatch()
            {
                var tasks = new List<Task>();
                for (int i = 0; i < _shardedContext.ShardCount; i++)
                {
                    _streamList[i].Position = 0;

                    var multi = new MultipartFormDataContent
                    {
                        {
                            new BlittableJsonContent(async stream2 => await _context.WriteAsync(stream2, _optionsAsBlittable).ConfigureAwait(false)), Constants.Smuggler.ImportOptions
                        },
                        {new Client.Documents.Smuggler.DatabaseSmuggler.StreamContentWithConfirmation(_streamList[i], _tcs), "file", "name"}
                    };
                    var cmd = new ShardedImportCommand(_handler, Headers.None, multi);

                    var task = _shardedContext.RequestExecutors[i].ExecuteAsync(cmd, _contextList[i]);

                    tasks.Add(task);
                }
                await tasks.WhenAll();
                for (int i = 0; i < _shardedContext.ShardCount; i++)
                {
                    _streamList[i].Position = 0;
                }
            }
        }
    }
}
