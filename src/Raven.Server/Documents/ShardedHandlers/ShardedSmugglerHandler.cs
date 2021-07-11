using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Net.Http;
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
using Raven.Server.Smuggler;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Smuggler.Documents.Data;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using DatabaseSmuggler = Raven.Server.Smuggler.Documents.DatabaseSmuggler;

namespace Raven.Server.Documents.ShardedHandlers
{
    public class ShardedSmugglerHandler : ShardedRequestHandler
    {

        [RavenShardedAction("/databases/*/smuggler/export", "POST")]
        public async Task PostExport()
        {
            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                BlittableJsonReaderObject blittableJson = null;
                DatabaseSmugglerOptionsServerSide options;

                //TODO - EFRAT - From where we are getting startEtag and  startRaftIndex to export? do we need it
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
                            onProgress => ExportShardedDatabaseInternalAsync(options, onProgress, blittableJson, context, token),
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
            Action<IOperationProgress> onProgress,
            BlittableJsonReaderObject blittableJson,
            JsonOperationContext context,
            OperationCancelToken token)
        {
            var tasks = new List<Task>();
            var contextList = new List<JsonOperationContext>();

            for (int i = 0; i < ShardedContext.ShardCount; i++)
            {
                JsonOperationContext jsonOperationContext;
                ContextPool.AllocateOperationContext(out jsonOperationContext);
                contextList.Add(jsonOperationContext);
            }

            RelativeShardUrl = RelativeShardUrl.Split('?')[0];
            var tempFiles = new List<string>();
            for (int i = 0; i < ShardedContext.ShardCount; i++)
            {
                var tempFileName = $"{Server.Configuration.Backup.ShardTempPath}\\{Guid.NewGuid()}";
                tempFiles.Add(tempFileName);

                var tcs = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
                var cmd = new ShardedExportCommand(this, async stream =>
                {
                    try
                    {
                        var fileInfo = new FileInfo(tempFileName);
                        var directoryInfo = fileInfo.Directory;
                        if (directoryInfo != null && directoryInfo.Exists == false)
                            directoryInfo.Create();

                        using (var fileStream = fileInfo.OpenWrite())
                            await stream.CopyToAsync(fileStream, 8192, new CancellationToken()).ConfigureAwait(false);
                        tcs.TrySetResult(null);

                    }
                    catch (Exception e)
                    {
                        //if (Logger.IsOperationsEnabled)
                        //     Logger.Operations("Could not save export file.", e);

                        tcs.TrySetException(e);

                        if (e is UnauthorizedAccessException || e is DirectoryNotFoundException || e is IOException)
                            throw new InvalidOperationException($"Cannot export to selected path {tempFileName}, please ensure you selected proper filename.", e);

                        throw new InvalidOperationException($"Could not save export file {tempFileName}.", e);
                    }
                }, tcs, blittableJson);

                var task = ShardedContext.RequestExecutors[i].ExecuteAsync(cmd, contextList[i]);
                tasks.Add(task);

            }

            await tasks.WhenAll();

            var sources = new List<ISmugglerSource>();
            var fileStreamlist = new List<GZipStream>();
            StreamDestination destination = null;
            try
            {
                for (var i = 0; i < tempFiles.Count; i++)
                {
                    var file = tempFiles[i];
                    var fileStream = new GZipStream(new BufferedStream(GetInputStream(File.OpenRead(file), options), 128 * Voron.Global.Constants.Size.Kilobyte),
                        CompressionMode.Decompress);
                    fileStreamlist.Add(fileStream);

                    var source = new StreamSource(fileStream, contextList[i], ShardedContext.DatabaseName);
                    sources.Add(source);
                }

                using (token)
                {
                    await using (var outputStream = GetOutputStream(ResponseBodyStream(), options))
                    {
                        destination = new StreamDestination(outputStream, context, sources[0]);
                        var smuggler = new DatabaseSmuggler(sources, destination, new SystemTime(), context, options, onProgress: onProgress, token: token.Token);

                        return await smuggler.ExecuteAsync();
                    }
                }
            }
            finally
            {
                if (destination?.ToDispose != null)
                {
                    foreach (var tDisposable in destination.ToDispose)
                    {
                        tDisposable.Dispose();
                    }
                }

                for (int i = 0; i < ShardedContext.ShardCount; i++)
                {
                    await fileStreamlist[i].DisposeAsync();
                    ((StreamSource)sources[i]).Dispose();
                    contextList[i].Dispose();
                }

                foreach (var file in tempFiles)
                {
                    File.Delete(file);
                }
            }
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
                                    await DoImportInternalAsync(context, inputStream, blittableJson, result, onProgress, token);
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

                // await WriteImportResultAsync(context, result, ResponseBodyStream());


            }
        }

        private async Task DoImportInternalAsync(JsonOperationContext context, Stream stream, BlittableJsonReaderObject options, SmugglerResult result, object onProgress,
            OperationCancelToken token)
        {
            var tempFileName = $"{Server.Configuration.Backup.ShardTempPath}\\{Guid.NewGuid()}";
            var contextList = new List<JsonOperationContext>();
            try
            {
                var tasks = new List<Task>();
                for (int i = 0; i < ShardedContext.ShardCount; i++)
                {
                    ContextPool.AllocateOperationContext(out JsonOperationContext jsonOperationContext);
                    contextList.Add(jsonOperationContext);
                }

                var tcs = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);

                var fileInfo = new FileInfo(tempFileName);
                var directoryInfo = fileInfo.Directory;
                if (directoryInfo != null && directoryInfo.Exists == false)
                    directoryInfo.Create();

                using (var fileStream = fileInfo.OpenWrite())
                {
                    await stream.CopyToAsync(fileStream, 8192, new CancellationToken());
                }

                for (int i = 0; i < ShardedContext.ShardCount; i++)
                {
                    var file = fileInfo.OpenRead();
                    {
                        var multi = new MultipartFormDataContent
                        {
                            {
                                new BlittableJsonContent(async stream2 => await context.WriteAsync(stream2, options).ConfigureAwait(false)),
                                Constants.Smuggler.ImportOptions
                            },
                            {new Client.Documents.Smuggler.DatabaseSmuggler.StreamContentWithConfirmation(file, tcs), "file", "name"}
                        };
                        var cmd = new ShardedImportCommand(this, Headers.None, multi);
                        var task = ShardedContext.RequestExecutors[i].ExecuteAsync(cmd, contextList[i]);
                        tasks.Add(task);
                    }
                }

                await tasks.WhenAll();
            }
            finally
            {
                for (int i = 0; i < ShardedContext.ShardCount; i++)
                {
                    contextList[i].Dispose();
                }
                File.Delete(tempFileName);
            }
        }
    }
}
