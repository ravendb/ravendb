// -----------------------------------------------------------------------
//  <copyright file="SmugglerHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Xml.Linq;
using Microsoft.AspNetCore.Http.Features.Authentication;
using Microsoft.AspNetCore.WebUtilities;
using Microsoft.Net.Http.Headers;
using Raven.Client;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Exceptions.Security;
using Raven.Client.Properties;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Documents.Operations;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.PeriodicBackup;
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

        [RavenAction("/databases/*/smuggler/validate-options", "POST", AuthorizationStatus.ValidUser)]
        public async Task PostValidateOptions()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var blittableJson = await context.ReadForMemoryAsync(RequestBodyStream(), "");
                var options = JsonDeserializationServer.DatabaseSmugglerOptions(blittableJson);

                if (!string.IsNullOrEmpty(options.FileName) && options.FileName.IndexOfAny(Path.GetInvalidFileNameChars()) >= 0)
                    throw new InvalidOperationException($"{options.FileName} is invalid file name");

                if (string.IsNullOrEmpty(options.TransformScript))
                {
                    NoContentStatus();
                    return;
                }

                try
                {
                    var scriptRunner = new ScriptRunner(Database, Database.Configuration, false);
                    scriptRunner.TryCompileScript(string.Format(@"
                    function execute(){{
                        {0}
                    }};", options.TransformScript));
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException("Incorrect transform script", e);
                }

                NoContentStatus();
            }
        }

        [RavenAction("/databases/*/smuggler/export", "POST", AuthorizationStatus.ValidUser, DisableOnCpuCreditsExhaustion = true)]
        public async Task PostExport()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var operationId = GetLongQueryString("operationId", false) ?? Database.Operations.GetNextOperationId();
                var startDocumentEtag = GetLongQueryString("startEtag", false) ?? 0;
                var startRaftIndex = GetLongQueryString("startRaftIndex", false) ?? 0;

                var stream = TryGetRequestFromStream("DownloadOptions") ?? RequestBodyStream();

                DatabaseSmugglerOptionsServerSide options;
                using (context.GetMemoryBuffer(out var buffer))
                {
                    var firstRead = await stream.ReadAsync(buffer.Memory.Memory);
                    buffer.Used = 0;
                    buffer.Valid = firstRead;
                    if (firstRead != 0)
                    {
                        var blittableJson = await context.ParseToMemoryAsync(stream, "DownloadOptions", BlittableJsonDocumentBuilder.UsageMode.None, buffer);
                        options = JsonDeserializationServer.DatabaseSmugglerOptions(blittableJson);
                    }
                    else
                    {
                        // no content, we'll use defaults
                        options = new DatabaseSmugglerOptionsServerSide();
                    }
                }

                if (string.IsNullOrWhiteSpace(options.EncryptionKey) == false)
                    ServerStore.LicenseManager.AssertCanCreateEncryptedDatabase();

                var feature = HttpContext.Features.Get<IHttpAuthenticationFeature>() as RavenServer.AuthenticateConnection;

                if (feature == null)
                    options.AuthorizationStatus = AuthorizationStatus.DatabaseAdmin;
                else
                    options.AuthorizationStatus = feature.CanAccess(Database.Name, requireAdmin: true) ? AuthorizationStatus.DatabaseAdmin : AuthorizationStatus.ValidUser;

                ApplyBackwardCompatibility(options);

                var token = CreateOperationToken();

                var fileName = options.FileName;
                if (string.IsNullOrEmpty(fileName))
                {
                    fileName = $"Dump of {context.DocumentDatabase.Name} {SystemTime.UtcNow.ToString("yyyy-MM-dd HH-mm", CultureInfo.InvariantCulture)}";
                }

                var contentDisposition = "attachment; filename=" + Uri.EscapeDataString(fileName) + ".ravendbdump";
                HttpContext.Response.Headers["Content-Disposition"] = contentDisposition;
                HttpContext.Response.Headers["Content-Type"] = "application/octet-stream";
                
                try
                {
                    await Database.Operations.AddOperation(
                            Database,
                            "Export database: " + Database.Name,
                            Operations.OperationType.DatabaseExport,
                            onProgress => ExportDatabaseInternalAsync(options, startDocumentEtag, startRaftIndex, onProgress, context, token), operationId, token: token);
                }
                catch (Exception)
                {
                    HttpContext.Abort();
                }
            }
        }

        private void ApplyBackwardCompatibility(DatabaseSmugglerOptionsServerSide options)
        {
            if (options == null)
                return;

            if (((options.OperateOnTypes & DatabaseItemType.DatabaseRecord) != 0)
                && (options.OperateOnDatabaseRecordTypes == DatabaseRecordItemType.None))
            {
                options.OperateOnDatabaseRecordTypes = DatabaseSmugglerOptions.DefaultOperateOnDatabaseRecordTypes;
            }

            if (RequestRouter.TryGetClientVersion(HttpContext, out var version) == false)
                return;

            if (version.Major != RavenVersionAttribute.Instance.MajorVersion)
                return;

#pragma warning disable 618
            if (version.Minor < 2 && options.OperateOnTypes.HasFlag(DatabaseItemType.Counters))
#pragma warning restore 618
            {
                options.OperateOnTypes |= DatabaseItemType.CounterGroups;
            }

            // only all 4.0 and 4.1 less or equal to 41006
            if (version.Revision < 60 || version.Revision > 41006)
                return;

            if (options.OperateOnTypes.HasFlag(DatabaseItemType.Documents))
                options.OperateOnTypes |= DatabaseItemType.Attachments;
        }

        private async Task<IOperationResult> ExportDatabaseInternalAsync(
            DatabaseSmugglerOptionsServerSide options,
            long startDocumentEtag,
            long startRaftIndex,
            Action<IOperationProgress> onProgress,
            DocumentsOperationContext context,
            OperationCancelToken token)
        {
            using (token)
            {
                var source = new DatabaseSource(Database, startDocumentEtag, startRaftIndex, Logger);
                await using (var outputStream = GetOutputStream(ResponseBodyStream(), options))
                {
                    var destination = new StreamDestination(outputStream, context, source);
                    var smuggler = new DatabaseSmuggler(Database, source, destination, Database.Time, options, onProgress: onProgress, token: token.Token);
                    return await smuggler.ExecuteAsync();
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

        [RavenAction("/databases/*/admin/smuggler/import", "GET", AuthorizationStatus.Operator, DisableOnCpuCreditsExhaustion = true)]
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
                using (var source = new StreamSource(stream, context, Database))
                {
                    var destination = new DatabaseDestination(Database);

                    var smuggler = new DatabaseSmuggler(Database, source, destination, Database.Time, options, token: token.Token);

                    var result = await smuggler.ExecuteAsync();

                    await WriteImportResultAsync(context, result, ResponseBodyStream());
                }
            }
        }

        [RavenAction("/databases/*/admin/smuggler/import-s3-dir", "GET", AuthorizationStatus.Operator, DisableOnCpuCreditsExhaustion = true)]
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

        [RavenAction("/databases/*/admin/smuggler/import-dir", "GET", AuthorizationStatus.Operator, DisableOnCpuCreditsExhaustion = true)]
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
                        using (var file = await getFile())
                        using (var stream = new GZipStream(new BufferedStream(file, 128 * Voron.Global.Constants.Size.Kilobyte), CompressionMode.Decompress))
                        using (var source = new StreamSource(stream, context, Database))
                        {
                            var destination = new DatabaseDestination(Database);

                            var smuggler = new DatabaseSmuggler(Database, source, destination, Database.Time);

                            var result = await smuggler.ExecuteAsync();
                            results.Enqueue(result);
                        }
                    }
                });
            }

            await Task.WhenAll(tasks);

            while (results.TryDequeue(out SmugglerResult importResult))
            {
                finalResult.Documents.SkippedCount += importResult.Documents.SkippedCount;
                finalResult.Documents.ReadCount += importResult.Documents.ReadCount;
                finalResult.Documents.ErroredCount += importResult.Documents.ErroredCount;
                finalResult.Documents.LastEtag = Math.Max(finalResult.Documents.LastEtag, importResult.Documents.LastEtag);
                finalResult.Documents.Attachments = importResult.Documents.Attachments;

                finalResult.RevisionDocuments.ReadCount += importResult.RevisionDocuments.ReadCount;
                finalResult.RevisionDocuments.ErroredCount += importResult.RevisionDocuments.ErroredCount;
                finalResult.RevisionDocuments.LastEtag = Math.Max(finalResult.RevisionDocuments.LastEtag, importResult.RevisionDocuments.LastEtag);
                finalResult.RevisionDocuments.Attachments = importResult.RevisionDocuments.Attachments;

                finalResult.Counters.ReadCount += importResult.Counters.ReadCount;
                finalResult.Counters.ErroredCount += importResult.Counters.ErroredCount;
                finalResult.Counters.LastEtag = Math.Max(finalResult.Counters.LastEtag, importResult.Counters.LastEtag);

                finalResult.TimeSeries.ReadCount += importResult.TimeSeries.ReadCount;
                finalResult.TimeSeries.ErroredCount += importResult.TimeSeries.ErroredCount;
                finalResult.TimeSeries.LastEtag = Math.Max(finalResult.TimeSeries.LastEtag, importResult.TimeSeries.LastEtag);

                finalResult.Identities.ReadCount += importResult.Identities.ReadCount;
                finalResult.Identities.ErroredCount += importResult.Identities.ErroredCount;

                finalResult.CompareExchange.ReadCount += importResult.CompareExchange.ReadCount;
                finalResult.CompareExchange.ErroredCount += importResult.CompareExchange.ErroredCount;

                finalResult.Subscriptions.ReadCount += importResult.Subscriptions.ReadCount;
                finalResult.Subscriptions.ErroredCount += importResult.Subscriptions.ErroredCount;

                finalResult.Indexes.ReadCount += importResult.Indexes.ReadCount;
                finalResult.Indexes.ErroredCount += importResult.Indexes.ErroredCount;

                foreach (var message in importResult.Messages)
                    finalResult.AddMessage(message);
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

        [RavenAction("/databases/*/admin/smuggler/migrate/ravendb", "POST", AuthorizationStatus.Operator, DisableOnCpuCreditsExhaustion = true)]
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

        [RavenAction("/databases/*/migrate/get-migrated-server-urls", "GET", AuthorizationStatus.ValidUser)]
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

        [RavenAction("/databases/*/admin/smuggler/migrate", "POST", AuthorizationStatus.Operator, DisableOnCpuCreditsExhaustion = true)]
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
                        using (var sw = new StreamWriter(ResponseBodyStream()))
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

        [RavenAction("/databases/*/smuggler/import", "POST", AuthorizationStatus.ValidUser, DisableOnCpuCreditsExhaustion = true)]
        public async Task PostImportAsync()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                if (HttpContext.Request.HasFormContentType == false)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.BadRequest; // Bad request
                    await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        context.Write(writer, new DynamicJsonValue
                        {
                            ["Type"] = "Error",
                            ["Error"] = "This endpoint requires form content type"
                        });
                        return;
                    }
                }

                var operationId = GetLongQueryString("operationId", false) ?? Database.Operations.GetNextOperationId();
                var token = CreateOperationToken();

                var result = new SmugglerResult();
                await Database.Operations.AddOperation(Database, "Import to: " + Database.Name,
                    Operations.OperationType.DatabaseImport,
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

                                        BlittableJsonReaderObject blittableJson;
                                        if (section.Headers.ContainsKey("Content-Encoding") && section.Headers["Content-Encoding"] == "gzip")
                                        {
                                            using (var gzipStream = new GZipStream(section.Body, CompressionMode.Decompress))
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

                                    ApplyBackwardCompatibility(options);

                                    var inputStream = GetInputStream(section.Body, options);
                                    var stream = new GZipStream(inputStream, CompressionMode.Decompress);
                                    await DoImportInternalAsync(context, stream, options, result, onProgress, token);
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

        [RavenAction("/databases/*/smuggler/import/csv", "POST", AuthorizationStatus.ValidUser, DisableOnCpuCreditsExhaustion = true)]
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
                                            using (var gzipStream = new GZipStream(section.Body, CompressionMode.Decompress))
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
                var smuggler = new DatabaseSmuggler(Database, source, destination, Database.Time, options, result, onProgress, token.Token);

                await smuggler.ExecuteAsync();
            }
        }

        private async Task DoImportInternalAsync(DocumentsOperationContext context, Stream stream, DatabaseSmugglerOptionsServerSide options, SmugglerResult result, Action<IOperationProgress> onProgress, OperationCancelToken token)
        {
            using (stream)
            using (token)
            using (var source = new StreamSource(stream, context, Database))
            {
                var destination = new DatabaseDestination(Database);
                var smuggler = new DatabaseSmuggler(Database, source, destination, Database.Time, options, result, onProgress, token.Token);

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

        private static async ValueTask WriteImportResultAsync(JsonOperationContext context, SmugglerResult result, Stream stream)
        {
            await using (var writer = new AsyncBlittableJsonTextWriter(context, stream))
            {
                var json = result.ToJson();
                context.Write(writer, json);
            }
        }
    }
}
