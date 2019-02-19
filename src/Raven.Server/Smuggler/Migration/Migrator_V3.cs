using System;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Exceptions;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Smuggler.Documents.Data;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using DatabaseSmuggler = Raven.Server.Smuggler.Documents.DatabaseSmuggler;

namespace Raven.Server.Smuggler.Migration
{
    public class Migrator_V3 : AbstractLegacyMigrator
    {
        private const int RavenFsHeadersPageSize = 32;

        private readonly MajorVersion _majorVersion;
        private readonly int _buildVersion;

        public Migrator_V3(MigratorOptions options, MigratorParameters parameters, MajorVersion majorVersion, int buildVersion) : base(options, parameters)
        {
            _majorVersion = majorVersion;
            _buildVersion = buildVersion;
        }

        public override async Task Execute()
        {
            var state = GetLastMigrationState();
            var originalState = state;

            var operateOnTypes = GenerateOperateOnTypes();
            if (operateOnTypes == ItemType.None && Options.ImportRavenFs == false)
                throw new BadRequestException("No types to import");

            if (Options.ImportRavenFs)
            {
                Parameters.Result.AddInfo("Started processing RavenFS files");
                Parameters.OnProgress.Invoke(Parameters.Result.Progress);

                var lastRavenFsEtag = await MigrateRavenFs(state?.LastRavenFsEtag ?? LastEtagsInfo.EtagEmpty);
                state = GetLastMigrationState() ?? GenerateLastEtagsInfo();
                state.LastRavenFsEtag = lastRavenFsEtag;
                await SaveLastOperationState(state);
            }

            if (operateOnTypes != ItemType.None)
            {
                if (Options.ImportRavenFs && operateOnTypes.HasFlag(ItemType.Documents) == false)
                {
                    Parameters.Result.Documents.Processed = true;
                    Parameters.OnProgress.Invoke(Parameters.Result.Progress);
                }

                var databaseMigrationOptions = new DatabaseMigrationOptions
                {
                    BatchSize = 1024,
                    OperateOnTypes = operateOnTypes,
                    ExportDeletions = originalState != null,
                    StartDocsEtag = state?.LastDocsEtag ?? LastEtagsInfo.EtagEmpty,
                    StartDocsDeletionEtag = state?.LastDocDeleteEtag ?? LastEtagsInfo.EtagEmpty,
                    StartAttachmentsEtag = state?.LastAttachmentsEtag ?? LastEtagsInfo.EtagEmpty,
                    StartAttachmentsDeletionEtag = state?.LastAttachmentsDeleteEtag ?? LastEtagsInfo.EtagEmpty
                };

                // getting a new operation id was added in v3.5
                var operationId = _majorVersion == MajorVersion.V30 ? 0 : await GetOperationId();

                object exportData;
                if (_majorVersion == MajorVersion.V30)
                {
                    exportData = new ExportDataV3
                    {
                        SmugglerOptions = JsonConvert.SerializeObject(databaseMigrationOptions)
                    };
                }
                else
                {
                    exportData = new ExportDataV35
                    {
                        DownloadOptions = JsonConvert.SerializeObject(databaseMigrationOptions),
                        ProgressTaskId = operationId
                    };
                }

                var exportOptions = JsonConvert.SerializeObject(exportData);
                var canGetLastStateByOperationId = _buildVersion >= 35215;

                await MigrateDatabase(exportOptions, readLegacyEtag: canGetLastStateByOperationId == false);

                var lastState = await GetLastState(canGetLastStateByOperationId, operationId);
                if (lastState != null)
                {
                    // refresh the migration state, in case we are running here with a RavenFS concurrently
                    lastState.LastRavenFsEtag = GetLastMigrationState()?.LastRavenFsEtag ?? LastEtagsInfo.EtagEmpty;
                    await SaveLastOperationState(lastState);
                }
            }
            else
            {
                if (Options.ImportRavenFs)
                    Parameters.Result.Documents.Processed = true;

                DatabaseSmuggler.EnsureProcessed(Parameters.Result);
            } 
        }

        private async Task<string> MigrateRavenFs(string lastEtag)
        {
            var destination = new DatabaseDestination(Parameters.Database);

            using (Parameters.Database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext transactionOperationContext))
            using (Parameters.Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (var documentActions = destination.Documents())
            {
                var sp = Stopwatch.StartNew();

                while (true)
                {
                    var ravenFsHeadersArray = await GetRavenFsHeadersArray(lastEtag, transactionOperationContext);
                    if (ravenFsHeadersArray.Length == 0)
                    {
                        var count = Parameters.Result.Documents.Attachments.ReadCount;
                        if (count > 0)
                        {
                            var message = $"Read {count:#,#;;0} RavenFS file{(count > 1 ? "s" : string.Empty)}.";
                            Parameters.Result.AddInfo(message);
                            Parameters.OnProgress.Invoke(Parameters.Result.Progress);
                        }

                        return lastEtag;
                    }

                    foreach (var headerObject in ravenFsHeadersArray)
                    {
                        var blittable = headerObject as BlittableJsonReaderObject;
                        if (blittable == null)
                            throw new InvalidDataException("headerObject isn't a BlittableJsonReaderObject");

                        if (blittable.TryGet("FullPath", out string fullPath) == false)
                            throw new InvalidDataException("FullPath doesn't exist");

                        if (blittable.TryGet("Metadata", out BlittableJsonReaderObject metadata) == false)
                            throw new InvalidDataException("Metadata doesn't exist");

                        var key = fullPath.TrimStart('/');
                        metadata = GetCleanMetadata(metadata, context);

                        var dataStream = await GetRavenFsStream(key);
                        if (dataStream == null)
                        {
                            Parameters.Result.Tombstones.ReadCount++;
                            var id = StreamSource.GetLegacyAttachmentId(key);
                            documentActions.DeleteDocument(id);
                            continue;
                        }

                        WriteDocumentWithAttachment(documentActions, context, dataStream, key, metadata);

                        Parameters.Result.Documents.ReadCount++;
                        if (Parameters.Result.Documents.Attachments.ReadCount % 50 == 0 || sp.ElapsedMilliseconds > 3000)
                        {
                            var message = $"Read {Parameters.Result.Documents.Attachments.ReadCount:#,#;;0} " +
                                          $"RavenFS file{(Parameters.Result.Documents.Attachments.ReadCount > 1 ? "s" : string.Empty)}.";
                            Parameters.Result.AddInfo(message);
                            Parameters.OnProgress.Invoke(Parameters.Result.Progress);
                            sp.Restart();
                        }
                    }

                    var lastFile = ravenFsHeadersArray.Last() as BlittableJsonReaderObject;
                    Debug.Assert(lastFile != null, "lastAttachment != null");
                    if (lastFile.TryGet("Etag", out string etag))
                        lastEtag = etag;
                }
            }
        }

        private async Task<Stream> GetRavenFsStream(string key)
        {
            var response = await RunWithAuthRetry(async () =>
            {
                var url = $"{Options.ServerUrl}/fs/{Options.DatabaseName}/files/{Uri.EscapeDataString(key)}";
                var request = new HttpRequestMessage(HttpMethod.Get, url);
                var responseMessage = await Parameters.HttpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, Parameters.CancelToken.Token);
                return responseMessage;
            });
            
            if (response.StatusCode == HttpStatusCode.NotFound)
            {
                // the file was deleted
                return null;
            }

            if (response.IsSuccessStatusCode == false)
            {
                var responseString = await response.Content.ReadAsStringAsync();
                throw new InvalidOperationException($"Failed to get file, key: {key}, from server: {Options.ServerUrl}, " +
                                                    $"status code: {response.StatusCode}, " +
                                                    $"error: {responseString}");
            }

            return await response.Content.ReadAsStreamAsync();
        }

        private BlittableJsonReaderObject GetCleanMetadata(BlittableJsonReaderObject metadata, DocumentsOperationContext context)
        {
            metadata.Modifications = new DynamicJsonValue(metadata);
            metadata.Modifications.Remove("Origin");
            metadata.Modifications.Remove("Raven-Synchronization-Version");
            metadata.Modifications.Remove("Raven-Synchronization-Source");
            metadata.Modifications.Remove("Creation-Date");
            metadata.Modifications.Remove("Raven-Creation-Date");
            metadata.Modifications.Remove("Raven-Synchronization-History");
            metadata.Modifications.Remove("RavenFS-Size");
            metadata.Modifications.Remove("Last-Modified");
            metadata.Modifications.Remove("Raven-Last-Modified");
            metadata.Modifications.Remove("Content-MD5");
            metadata.Modifications.Remove("ETag");
            return context.ReadObject(metadata, Options.MigrationStateKey);
        }

        private async Task<BlittableJsonReaderArray> GetRavenFsHeadersArray(string lastEtag, TransactionOperationContext context)
        {
            var response = await RunWithAuthRetry(async () =>
            {
                var url = $"{Options.ServerUrl}/fs/{Options.DatabaseName}/streams/files?pageSize={RavenFsHeadersPageSize}&etag={lastEtag}";
                var request = new HttpRequestMessage(HttpMethod.Get, url);
                var responseMessage = await Parameters.HttpClient.SendAsync(request, Parameters.CancelToken.Token);
                return responseMessage;
            });

            if (response.IsSuccessStatusCode == false)
            {
                var responseString = await response.Content.ReadAsStringAsync();
                throw new InvalidOperationException($"Failed to get RavenFS headers list from server: {Options.ServerUrl}, " +
                                                    $"status code: {response.StatusCode}, " +
                                                    $"error: {responseString}");
            }

            using (var responseStream = await response.Content.ReadAsStreamAsync())
            {
                var headersList = await context.ReadForMemoryAsync(responseStream, "ravenfs-headers-list");
                if (headersList.TryGet("Results", out BlittableJsonReaderArray headers) == false)
                    throw new InvalidDataException("Response is invalid");

                return headers;
            }
        }

        private ItemType GenerateOperateOnTypes()
        {
            var itemType = ItemType.None;
            if (Options.OperateOnTypes.HasFlag(DatabaseItemType.Documents))
            {
                itemType |= ItemType.Documents;
            }

            if (Options.OperateOnTypes.HasFlag(DatabaseItemType.LegacyAttachments))
            {
                itemType |= ItemType.Attachments;
            }

            if (Options.OperateOnTypes.HasFlag(DatabaseItemType.Indexes))
            {
                itemType |= ItemType.Indexes;
            }

            if (Options.RemoveAnalyzers)
            {
                itemType |= ItemType.RemoveAnalyzers;
            }

            return itemType;
        }

        private async Task<SmugglerResult> MigrateDatabase(string json, bool readLegacyEtag)
        {
            var response = await RunWithAuthRetry(async () =>
            {
                var url = $"{Options.ServerUrl}/databases/{Options.DatabaseName}/studio-tasks/exportDatabase";
                var content = new StringContent(json, Encoding.UTF8, "application/json");
                var request = new HttpRequestMessage(HttpMethod.Post, url)
                {
                    Content = content
                };

                var responseMessage = await Parameters.HttpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, Parameters.CancelToken.Token);
                return responseMessage;
            });

            if (response.IsSuccessStatusCode == false)
            {
                var responseString = await response.Content.ReadAsStringAsync();
                throw new InvalidOperationException($"Failed to export database from server: {Options.ServerUrl}, " +
                                                    $"status code: {response.StatusCode}, " +
                                                    $"error: {responseString}");
            }

            using (var responseStream = await response.Content.ReadAsStreamAsync())
            using (var stream = new GZipStream(responseStream, mode: CompressionMode.Decompress))
            using (Parameters.Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (var source = new StreamSource(stream, context, Parameters.Database))
            {
                var destination = new DatabaseDestination(Parameters.Database);
                var options = new DatabaseSmugglerOptionsServerSide
                {
#pragma warning disable 618
                    ReadLegacyEtag = readLegacyEtag,
#pragma warning restore 618
                    RemoveAnalyzers = Options.RemoveAnalyzers,
                    TransformScript = Options.TransformScript,
                    OperateOnTypes = Options.OperateOnTypes
                };

                var smuggler = new DatabaseSmuggler(Parameters.Database, source, destination, Parameters.Database.Time, options, Parameters.Result, Parameters.OnProgress, Parameters.CancelToken.Token);

                return smuggler.Execute();
            }
        }

        private async Task<long> GetOperationId()
        {
            var response = await RunWithAuthRetry(async () =>
            {
                var url = $"{Options.ServerUrl}/databases/{Options.DatabaseName}/studio-tasks/next-operation-id";
                var request = new HttpRequestMessage(HttpMethod.Get, url);
                var responseMessage = await Parameters.HttpClient.SendAsync(request, Parameters.CancelToken.Token);
                return responseMessage;
            });

            if (response.IsSuccessStatusCode == false)
            {
                var responseString = await response.Content.ReadAsStringAsync();
                throw new InvalidOperationException($"Failed to get operation id from server: {Options.ServerUrl}, " +
                                                    $"status code: {response.StatusCode}, " +
                                                    $"error: {responseString}");
            }

            var bytes = await response.Content.ReadAsByteArrayAsync();
            var str = Encoding.UTF8.GetString(bytes, 0, bytes.Length);
            return long.Parse(str);
        }

        private async Task<LastEtagsInfo> GetLastState(bool canGetLastStateByOperationId, long operationId)
        {
            using (Parameters.Database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var lastEtagsInfo = canGetLastStateByOperationId
                    ? await GetLastStateByOperationId(operationId, context)
                    : GenerateLastEtagsInfo();

                return lastEtagsInfo;
            }
        }

        private async Task<LastEtagsInfo> GetLastStateByOperationId(long operationId, TransactionOperationContext context)
        {
            var retries = 0;
            while (true)
            {
                if (++retries > 15)
                    return null;

                var operationStatus = await GetOperationStatus(Options.DatabaseName, operationId, context);
                if (operationStatus == null)
                    return null;

                if (operationStatus.TryGet("Completed", out bool completed) == false)
                    return null;

                if (completed == false)
                {
                    await Task.Delay(1000, Parameters.CancelToken.Token);
                    continue;
                }

                if (operationStatus.TryGet("OperationState", out BlittableJsonReaderObject operationStateBlittable) == false)
                {
                    // OperationState was added in the latest release of v3.5
                    return null;
                }

                operationStateBlittable.TryGet(nameof(LastEtagsInfo.LastDocsEtag), out string lastDocsEtag);
                operationStateBlittable.TryGet(nameof(LastEtagsInfo.LastDocDeleteEtag), out string lastDocsDeleteEtag);
                operationStateBlittable.TryGet(nameof(LastEtagsInfo.LastAttachmentsEtag), out string lastAttachmentsEtag);
                operationStateBlittable.TryGet(nameof(LastEtagsInfo.LastAttachmentsDeleteEtag), out string lastAttachmentsDeleteEtag);

                var lastEtagsInfo = new LastEtagsInfo
                {
                    ServerUrl = Options.ServerUrl,
                    DatabaseName = Options.DatabaseName,
                    LastDocsEtag = lastDocsEtag,
                    LastDocDeleteEtag = lastDocsDeleteEtag,
                    LastAttachmentsEtag = lastAttachmentsEtag,
                    LastAttachmentsDeleteEtag = lastAttachmentsDeleteEtag
                };

                return lastEtagsInfo;
            }
        }

        private async Task<BlittableJsonReaderObject> GetOperationStatus(
            string databaseName, long operationId, TransactionOperationContext context)
        {
            var response = await RunWithAuthRetry(async () =>
            {
                var url = $"{Options.ServerUrl}/databases/{databaseName}/operation/status?id={operationId}";
                var request = new HttpRequestMessage(HttpMethod.Get, url);
                var responseMessage = await Parameters.HttpClient.SendAsync(request, Parameters.CancelToken.Token);
                return responseMessage;
            });

            if (response.StatusCode == HttpStatusCode.NotFound)
            {
                // the operation status was deleted before we could get it
                return null;
            }

            if (response.IsSuccessStatusCode == false)
            {
                var responseString = await response.Content.ReadAsStringAsync();
                throw new InvalidOperationException($"Failed to get operation status from server: {Options.ServerUrl}, " +
                                                    $"status code: {response.StatusCode}, " +
                                                    $"error: {responseString}");
            }

            var responseStream = await response.Content.ReadAsStreamAsync();
            return await context.ReadForMemoryAsync(responseStream, "migration-operation-state");
        }
    }
}
