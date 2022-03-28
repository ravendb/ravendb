// -----------------------------------------------------------------------
//  <copyright file="SmugglerHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Xml.Linq;
using Microsoft.AspNetCore.WebUtilities;
using Microsoft.Net.Http.Headers;
using Raven.Client;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Exceptions.Security;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Documents.Handlers.Processors.Smuggler;
using Raven.Server.Documents.Operations;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents.Data;
using Raven.Server.Smuggler.Migration;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Platform;
using Sparrow.Utils;

namespace Raven.Server.Smuggler.Documents.Handlers
{
    public class SmugglerHandler : DatabaseRequestHandler
    {
        private static readonly HttpClient HttpClient = new HttpClient();

        [RavenAction("/databases/*/smuggler/validate-options", "POST", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task ValidateOptions()
        {
            using (var processor = new SmugglerHandlerProcessorForValidateOptions<DocumentsOperationContext>(this, ContextPool))
            {
                await processor.ExecuteAsync();
            }
        }

        [RavenAction("/databases/*/smuggler/export", "POST", AuthorizationStatus.ValidUser, EndpointType.Read, DisableOnCpuCreditsExhaustion = true)]
        public async Task PostExport()
        {
            var result = new SmugglerResult();
            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var operationId = GetLongQueryString("operationId", false) ?? Database.Operations.GetNextOperationId();
                try
                {
                    await Export(context, Database.Name, ExportDatabaseInternalAsync, Database.Operations, operationId, Database);
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

        public async Task<IOperationResult> ExportDatabaseInternalAsync(
            DatabaseSmugglerOptionsServerSide options,
            long startDocumentEtag,
            long startRaftIndex,
            Action<IOperationProgress> onProgress,
            JsonOperationContext jsonOperationContext,
            OperationCancelToken token)
        {
            using (token)
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var source = new DatabaseSource(Database, startDocumentEtag, startRaftIndex, Logger);
                await using (var outputStream = GetOutputStream(ResponseBodyStream(), options))
                {
                    var destination = new StreamDestination(outputStream, context, source);
                    var smuggler = SmugglerBase.GetDatabaseSmuggler(Database, source, destination, Database.Time, 
                        jsonOperationContext, options, onProgress: onProgress, token: token.Token);
                    return await smuggler.ExecuteAsync();
                }
            }
        }

        [RavenAction("/databases/*/admin/smuggler/import", "GET", AuthorizationStatus.DatabaseAdmin, DisableOnCpuCreditsExhaustion = true)]
        public async Task GetImport()
        {
            if (HttpContext.Request.Query.ContainsKey("file") == false &&
                HttpContext.Request.Query.ContainsKey("url") == false)
            {
                throw new ArgumentException("'file' or 'url' are mandatory when using GET /smuggler/import");
            }

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var options = DatabaseSmugglerOptionsServerSide.Create(HttpContext);

                await using (var stream = new GZipStream(new BufferedStream(await GetImportStream(), 128 * Voron.Global.Constants.Size.Kilobyte), CompressionMode.Decompress))
                using (var token = CreateOperationToken())
                using (var source = new StreamSource(stream, context, Database.Name))
                {
                    var destination = new DatabaseDestination(Database);

                    var smuggler = SmugglerBase.GetDatabaseSmuggler(Database, source, destination, Database.Time, context, options, token: token.Token);

                    var result = await smuggler.ExecuteAsync();

                    await WriteImportResultAsync(context, result, ResponseBodyStream());
                }
            }
        }

        [RavenAction("/databases/*/admin/smuggler/import-s3-dir", "GET", AuthorizationStatus.DatabaseAdmin, DisableOnCpuCreditsExhaustion = true)]
        public async Task PostImportFromS3Directory()
        {
            var url = GetQueryStringValueAndAssertIfSingleAndNotEmpty("url");
            var result = await HttpClient.GetAsync(url);
            var dirTextXml = await result.Content.ReadAsStringAsync();
            var filesListing = XElement.Parse(dirTextXml);
            var ns = XNamespace.Get("http://s3.amazonaws.com/doc/2006-03-01/");
            var urls = from content in filesListing.Elements(ns + "Contents")
                       let requestUri = url.TrimEnd('/') + "/" + content.Element(ns + "Key").Value
                       select (Func<Task<Stream>>)(async () =>
                      {
                          var response = await HttpClient.GetAsync(requestUri);
                          if (response.IsSuccessStatusCode == false)
                              throw new InvalidOperationException("Request failed on " + requestUri + " with " +
                                                                  await response.Content.ReadAsStreamAsync());
                          return await response.Content.ReadAsStreamAsync();
                      });

            var files = new BlockingCollection<Func<Task<Stream>>>(new ConcurrentQueue<Func<Task<Stream>>>(urls));
            files.CompleteAdding();
            await BulkImport(files, Path.GetTempPath());
        }

        [RavenAction("/databases/*/admin/smuggler/import-dir", "GET", AuthorizationStatus.DatabaseAdmin, DisableOnCpuCreditsExhaustion = true)]
        public async Task PostImportDirectory()
        {
            var directory = GetQueryStringValueAndAssertIfSingleAndNotEmpty("dir");
            var files = new BlockingCollection<Func<Task<Stream>>>(new ConcurrentQueue<Func<Task<Stream>>>(
                    Directory.GetFiles(directory, "*.dump")
                        .Select(x => (Func<Task<Stream>>)(() => Task.FromResult<Stream>(File.OpenRead(x)))))
            );
            files.CompleteAdding();
            await BulkImport(files, directory);
        }

        private async Task BulkImport(BlockingCollection<Func<Task<Stream>>> files, string directory)
        {
            var maxTasks = GetIntValueQueryString("maxTasks", required: false) ?? ProcessorInfo.ProcessorCount / 2;
            var results = new ConcurrentQueue<SmugglerResult>();
            var tasks = new Task[Math.Max(1, maxTasks)];

            var finalResult = new SmugglerResult();

            for (int i = 0; i < tasks.Length; i++)
            {
                tasks[i] = Task.Run(async () =>
                {
                    while (files.IsCompleted == false)
                    {
                        Func<Task<Stream>> getFile;
                        try
                        {
                            getFile = files.Take();
                        }
                        catch (Exception)
                        {
                            continue;
                        }

                        using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                        await using (var file = await getFile())
                        await using (var stream = new GZipStream(new BufferedStream(file, 128 * Voron.Global.Constants.Size.Kilobyte), CompressionMode.Decompress))
                        using (var source = new StreamSource(stream, context, Database.Name))
                        {
                            var destination = new DatabaseDestination(Database);

                            var smuggler = SmugglerBase.GetDatabaseSmuggler(Database, source, destination, Database.Time, context);

                            var result = await smuggler.ExecuteAsync();
                            results.Enqueue(result);
                        }
                    }
                });
            }

            await Task.WhenAll(tasks);

            while (results.TryDequeue(out SmugglerResult importResult))
            {
                GetOperationStateOperation.GetOperationStateCommand.CombineSmugglerResults(finalResult, importResult);
            }

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext finalContext))
            {
                var memoryStream = new MemoryStream();
                await WriteImportResultAsync(finalContext, finalResult, memoryStream);
                memoryStream.Position = 0;
                try
                {
                    await using (var output = File.Create(Path.Combine(directory, "smuggler.results.txt")))
                    {
                        await memoryStream.CopyToAsync(output);
                    }
                }
                catch (Exception)
                {
                    // ignore any failure here
                }
                memoryStream.Position = 0;
                await memoryStream.CopyToAsync(ResponseBodyStream());
            }
        }

        [RavenAction("/databases/*/admin/smuggler/migrate/ravendb", "POST", AuthorizationStatus.DatabaseAdmin, DisableOnCpuCreditsExhaustion = true)]
        public async Task MigrateFromRavenDB()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var migrationConfiguration = await context.ReadForMemoryAsync(RequestBodyStream(), "migration-configuration");
                var migrationConfigurationJson = JsonDeserializationServer.SingleDatabaseMigrationConfiguration(migrationConfiguration);

                if (string.IsNullOrWhiteSpace(migrationConfigurationJson.ServerUrl))
                    throw new ArgumentException("Url cannot be null or empty");

                if (migrationConfigurationJson.MigrationSettings == null)
                    throw new ArgumentException("MigrationSettings cannot be null");

                if (string.IsNullOrWhiteSpace(migrationConfigurationJson.MigrationSettings.DatabaseName))
                    throw new ArgumentException("Database name cannot be null or empty");

                var migrator = new Migrator(migrationConfigurationJson, ServerStore);
                await migrator.UpdateBuildInfoIfNeeded();
                var operationId = migrator.StartMigratingSingleDatabase(migrationConfigurationJson.MigrationSettings, Database);

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteOperationIdAndNodeTag(context, operationId, ServerStore.NodeTag);
                }
            }
        }

        [RavenAction("/databases/*/migrate/get-migrated-server-urls", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetMigratedServerUrls()
        {
            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var documents = Database.DocumentsStorage.GetDocumentsStartingWith(
                    context, Migrator.MigrationStateKeyBase, null, null, null, 0, 64);

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    writer.WritePropertyName(nameof(MigratedServerUrls.List));
                    writer.WriteStartArray();

                    var urls = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                    foreach (var document in documents)
                    {
                        if (document.Data.TryGet(nameof(ImportInfo.ServerUrl), out string serverUrl) == false)
                        {
                            // server url used to be saved only in the document id
                            // document id: Raven/Migration/Status/{server-version}/{database-name}/{url}
                            var splitted = document.Id.ToString()
                                .Replace(Migrator.MigrationStateKeyBase, string.Empty)
                                .Split("/http");

                            if (splitted.Length != 2)
                                continue;

                            serverUrl = $"http{splitted.Last()}";
                            if (string.IsNullOrWhiteSpace(serverUrl))
                                continue;
                        }

                        urls.Add(serverUrl);
                    }

                    var first = true;
                    foreach (var url in urls)
                    {
                        if (first == false)
                            writer.WriteComma();
                        first = false;

                        writer.WriteString(url);
                    }

                    writer.WriteEndArray();
                    writer.WriteEndObject();
                }
            }
        }

        [RavenAction("/databases/*/admin/smuggler/migrate", "POST", AuthorizationStatus.DatabaseAdmin, DisableOnCpuCreditsExhaustion = true)]
        public async Task MigrateFromAnotherDatabase()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var blittable = await context.ReadForMemoryAsync(RequestBodyStream(), "migration-configuration");
                var migrationConfiguration = JsonDeserializationServer.MigrationConfiguration(blittable);

                var migratorFullPath = Server.Configuration.Migration.MigratorPath?.FullPath ?? migrationConfiguration.MigratorFullPath;

                if (string.IsNullOrWhiteSpace(migratorFullPath))
                    throw new ArgumentException("MigratorFullPath cannot be null or empty");

                if (migrationConfiguration.InputConfiguration == null)
                    throw new ArgumentException("InputConfiguration cannot be null");

                if (migrationConfiguration.InputConfiguration.TryGet("Command", out string command) == false)
                    throw new ArgumentException("Cannot find the Command property in the InputConfiguration");

                var migratorFile = ResolveMigratorPath(migratorFullPath);
                if (command == "validateMigratorPath")
                {
                    NoContentStatus();
                    return;
                }

                if (string.IsNullOrWhiteSpace(migrationConfiguration.DatabaseTypeName))
                    throw new ArgumentException("DatabaseTypeName cannot be null or empty");

                var processStartInfo = new ProcessStartInfo
                {
                    FileName = migratorFile.FullName,
                    Arguments = $"{migrationConfiguration.DatabaseTypeName}",
                    WorkingDirectory = migratorFile.Directory.FullName,
                    CreateNoWindow = true,
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    RedirectStandardInput = true,
                    UseShellExecute = false
                };

                Process process = null;
                try
                {
                    process = Process.Start(processStartInfo);
                }
                catch (Exception e)
                {
                    var killed = ProcessExtensions.TryKill(process);
                    throw new InvalidOperationException($"Unable to execute Migrator. Process killed: {killed}" + Environment.NewLine +
                                                        "Command was: " + Environment.NewLine +
                                                        (processStartInfo.WorkingDirectory ?? Directory.GetCurrentDirectory()) + "> "
                                                        + processStartInfo.FileName + " " + processStartInfo.Arguments, e);
                }

                await process.StandardInput.WriteLineAsync(migrationConfiguration.InputConfiguration.ToString());

                var isExportCommand = command == "export";
                if (isExportCommand == false)
                {
                    var errorReadTask = ProcessExtensions.ReadOutput(process.StandardError);
                    var outputReadTask = ProcessExtensions.ReadOutput(process.StandardOutput);

                    await Task.WhenAll(new Task[] { errorReadTask, outputReadTask }).ConfigureAwait(false);

                    Debug.Assert(process.HasExited, "Migrator is still running!");

                    if (process.ExitCode == -1)
                    {
                        await ExitWithException(errorReadTask, null).ConfigureAwait(false);
                    }

                    try
                    {
                        var line = await outputReadTask.ConfigureAwait(false);
                        await using (var sw = new StreamWriter(ResponseBodyStream()))
                        {
                            await sw.WriteAsync(line);
                        }

                        return;
                    }
                    catch (Exception e)
                    {
                        await ExitWithException(errorReadTask, e).ConfigureAwait(false);
                    }
                }

                var operationId = GetLongQueryString("operationId", false) ?? Database.Operations.GetNextOperationId();
                var token = CreateOperationToken();
                var transformScript = migrationConfiguration.TransformScript;

                var t = Database.Operations.AddOperation(Database, $"Migration from: {migrationConfiguration.DatabaseTypeName}",
                    Operations.OperationType.DatabaseMigration,
                    onProgress =>
                    {
                        return Task.Run(async () =>
                        {
                            var result = new SmugglerResult();

                            try
                            {
                                using (ContextPool.AllocateOperationContext(out DocumentsOperationContext migrateContext))
                                {
                                    var options = new DatabaseSmugglerOptionsServerSide
                                    {
                                        TransformScript = transformScript
                                    };
                                    await DoImportInternalAsync(migrateContext, process.StandardOutput.BaseStream, options, result, onProgress, token);
                                }
                            }
                            catch (OperationCanceledException)
                            {
                                ProcessExtensions.TryKill(process);
                                throw;
                            }
                            catch (ObjectDisposedException)
                            {
                                ProcessExtensions.TryKill(process);
                                throw;
                            }
                            catch (Exception e)
                            {
                                var errorString = await ProcessExtensions.ReadOutput(process.StandardError).ConfigureAwait(false);
                                result.AddError($"Error occurred during migration. Process error: {errorString}, exception: {e}");
                                onProgress.Invoke(result.Progress);
                                var killed = ProcessExtensions.TryKill(process);
                                throw new InvalidOperationException($"{errorString}, process pid: {process.Id} killed: {killed}");
                            }

                            return (IOperationResult)result;
                        });
                    }, operationId, token: token).ConfigureAwait(false);

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteOperationIdAndNodeTag(context, operationId, ServerStore.NodeTag);
                }
            }
        }

        private static async Task ExitWithException(Task<string> errorReadTask, Exception exception)
        {
            var errorString = await errorReadTask.ConfigureAwait(false);
            if (exception == null)
            {
                throw new InvalidOperationException(errorString);
            }

            throw new InvalidOperationException($"Process error: {errorString}, exception: {exception}");
        }

        private FileInfo ResolveMigratorPath(string migratorPath)
        {
            var migratorDirectory = new DirectoryInfo(migratorPath);
            if (migratorDirectory.Exists == false)
                throw new InvalidOperationException($"Directory {migratorPath} doesn't exist");

            var migratorFileName = PlatformDetails.RunningOnPosix
                ? "Raven.Migrator"
                : "Raven.Migrator.exe";

            var path = Path.Combine(migratorDirectory.FullName, migratorFileName);
            var migratorFile = new FileInfo(path);
            if (migratorFile.Exists == false)
                throw new InvalidOperationException($"The file '{migratorFileName}' doesn't exist in path: {migratorPath}");

            return migratorFile;
        }

        [RavenAction("/databases/*/smuggler/import", "POST", AuthorizationStatus.ValidUser, EndpointType.Write, DisableOnCpuCreditsExhaustion = true)]
        public async Task PostImportAsync()
        {
            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var operationId = GetLongQueryString("operationId", false) ?? Database.Operations.GetNextOperationId();
                await Import(context, Database.Name, DoImportInternalAsync, Database.Operations, operationId, Database);
            }
        }

        [RavenAction("/databases/*/smuggler/import/csv", "POST", AuthorizationStatus.ValidUser, EndpointType.Write, DisableOnCpuCreditsExhaustion = true)]
        public async Task ImportFromCsv()
        {
            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                if (HttpContext.Request.HasFormContentType == false)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.BadRequest;
                    await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        context.Write(writer, new DynamicJsonValue
                        {
                            ["Type"] = "Error",
                            ["Error"] = "Import from csv requires form content type"
                        });
                        return;
                    }
                }

                var token = CreateOperationToken();
                var result = new SmugglerResult();
                var operationId = GetLongQueryString("operationId", false) ?? Database.Operations.GetNextOperationId();
                var collection = GetStringQueryString("collection", false);
                var operationDescription = collection != null ? "Import collection: " + collection : "Import collection from CSV";

                await Database.Operations.AddOperation(Database, operationDescription, Raven.Server.Documents.Operations.Operations.OperationType.CollectionImportFromCsv,
                    onProgress =>
                    {
                        return Task.Run(async () =>
                        {
                            try
                            {
                                var reader = new MultipartReader(MultipartRequestHelper.GetBoundary(MediaTypeHeaderValue.Parse(HttpContext.Request.ContentType),
                                    MultipartRequestHelper.MultipartBoundaryLengthLimit), HttpContext.Request.Body);

                                CsvImportOptions csvConfig = new CsvImportOptions();

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
                                        if (key != Constants.Smuggler.CsvImportOptions)
                                            continue;

                                        BlittableJsonReaderObject blittableJson;
                                        if (section.Headers.ContainsKey("Content-Encoding") && section.Headers["Content-Encoding"] == "gzip")
                                        {
                                            await using (var gzipStream = new GZipStream(section.Body, CompressionMode.Decompress))
                                            {
                                                blittableJson = await context.ReadForMemoryAsync(gzipStream, Constants.Smuggler.CsvImportOptions);
                                            }
                                        }
                                        else
                                        {
                                            blittableJson = await context.ReadForMemoryAsync(section.Body, Constants.Smuggler.CsvImportOptions);
                                        }

                                        try
                                        {
                                            csvConfig = JsonDeserializationServer.CsvImportOptions(blittableJson);
                                        }
                                        catch (Exception e)
                                        {
                                            var msg = "Failed to parse the CSV configuration parameters.";
                                            if (e.InnerException.Message.Contains("Could not convert"))
                                            {
                                                msg += " Please verify that only one character is used for 'Comment' & 'Quote'";
                                            }

                                            throw new InvalidOperationException(msg, e);
                                        }

                                        continue;
                                    }

                                    if (MultipartRequestHelper.HasFileContentDisposition(contentDisposition))
                                    {
                                        if (ContentDispositionHeaderValue.TryParse(section.ContentDisposition, out contentDisposition) == false)
                                            continue;

                                        if (string.IsNullOrEmpty(collection))
                                        {
                                            var fileName = contentDisposition.FileName.ToString().Trim('\"');
                                            collection = Inflector.Pluralize(CSharpClassName.ConvertToValidClassName(Path.GetFileNameWithoutExtension(fileName)));
                                        }

                                        var options = new DatabaseSmugglerOptionsServerSide();

                                        if (section.Headers.ContainsKey("Content-Encoding") && section.Headers["Content-Encoding"] == "gzip")
                                        {
                                            await using (var gzipStream = new GZipStream(section.Body, CompressionMode.Decompress))
                                            {
                                                await ImportDocumentsFromCsvStreamAsync(gzipStream, context, collection, options, result, onProgress, token, csvConfig);
                                            }
                                        }
                                        else
                                        {
                                            await ImportDocumentsFromCsvStreamAsync(section.Body, context, collection, options, result, onProgress, token, csvConfig);
                                        }
                                    }
                                }
                            }
                            catch (Exception e)
                            {
                                result.AddError($"Error occurred during csv import. Exception: {e.Message}");
                                throw;
                            }
                            return (IOperationResult)result;
                        });
                    }, operationId, token: token);

                await WriteImportResultAsync(context, result, ResponseBodyStream());
            }
        }

        private async Task ImportDocumentsFromCsvStreamAsync(Stream stream, DocumentsOperationContext context, string entity, DatabaseSmugglerOptionsServerSide options,
                                                  SmugglerResult result, Action<IOperationProgress> onProgress, OperationCancelToken token, CsvImportOptions csvConfig)
        {
            if (string.IsNullOrEmpty(entity) == false && char.IsLower(entity[0]))
                entity = char.ToUpper(entity[0]) + entity.Substring(1);

            result.AddInfo($"Import collection: {entity}");

            using (var source = new CsvStreamSource(Database, stream, context, entity, csvConfig))
            {
                var destination = new DatabaseDestination(Database);
                var smuggler = SmugglerBase.GetDatabaseSmuggler(Database, source, destination, Database.Time, context, options, result, onProgress, token.Token);

                await smuggler.ExecuteAsync();
            }
        }

        private async Task DoImportInternalAsync(JsonOperationContext jsonOperationContext, Stream stream, DatabaseSmugglerOptionsServerSide options, 
            SmugglerResult result, Action<IOperationProgress> onProgress, OperationCancelToken token)
        {
            ContextPool.AllocateOperationContext(out DocumentsOperationContext context);
            await using (stream)
            using (token)
            using (var source = new StreamSource(stream, context, Database.Name))
            {
                var destination = new DatabaseDestination(Database, token.Token);
                var smuggler = SmugglerBase.GetDatabaseSmuggler(Database, source, destination, Database.Time, jsonOperationContext, options, result, onProgress, token.Token);

                await smuggler.ExecuteAsync();
            }
        }

        private async Task<Stream> GetImportStream()
        {
            var file = GetStringQueryString("file", required: false);
            if (string.IsNullOrEmpty(file) == false)
            {
                if (await IsOperatorAsync() == false)
                    throw new AuthorizationException("The use of the 'file' query string parameters is limited operators and above");
                return File.OpenRead(file);
            }

            var url = GetStringQueryString("url", required: false);
            if (string.IsNullOrEmpty(url) == false)
            {
                if (await IsOperatorAsync() == false)
                    throw new AuthorizationException("The use of the 'url' query string parameters is limited operators and above");

                if (HttpContext.Request.Method == "POST")
                {
                    var msg = await HttpClient.PostAsync(url, new StreamContent(HttpContext.Request.Body)
                    {
                        Headers =
                        {
                            ContentType =  new System.Net.Http.Headers.MediaTypeHeaderValue(HttpContext.Request.ContentType)
                        }
                    });
                    return await msg.Content.ReadAsStreamAsync();
                }

                return await HttpClient.GetStreamAsync(url);
            }

            return HttpContext.Request.Body;
        }
    }
}
