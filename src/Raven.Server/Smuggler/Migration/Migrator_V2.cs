using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Client.Documents.Smuggler;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Smuggler.Documents.Data;
using Sparrow.Json;
using DatabaseSmuggler = Raven.Server.Smuggler.Documents.DatabaseSmuggler;

namespace Raven.Server.Smuggler.Migration
{
    public class Migrator_V2 : AbstractLegacyMigrator
    {
        private const int AttachmentsPageSize = 32;

        public Migrator_V2(MigratorOptions options, MigratorParameters parameters) : base(options, parameters)
        {
        }

        public override async Task Execute()
        {
            var state = GetLastMigrationState();

            var migratedDocumentsOrAttachments = false;
            if (Options.OperateOnTypes.HasFlag(DatabaseItemType.Documents))
            {
                await MigrateDocuments(state?.LastDocsEtag ?? LastEtagsInfo.EtagEmpty);
                migratedDocumentsOrAttachments = true;
            }

            if (Options.OperateOnTypes.HasFlag(DatabaseItemType.LegacyAttachments))
            {
                await MigrateAttachments(state?.LastAttachmentsEtag ?? LastEtagsInfo.EtagEmpty, Parameters.Result);
                migratedDocumentsOrAttachments = true;
            }

            if (migratedDocumentsOrAttachments)
            {
                Parameters.Result.Documents.Processed = true;
                Parameters.OnProgress.Invoke(Parameters.Result.Progress);
                await SaveLastOperationState(GenerateLastEtagsInfo());
            }

            if (Options.OperateOnTypes.HasFlag(DatabaseItemType.Indexes))
                await MigrateIndexes();

            DatabaseSmuggler.EnsureProcessed(Parameters.Result);
        }

        private async Task MigrateDocuments(string lastEtag)
        {
            var response = await RunWithAuthRetry(async () =>
            {
                var url = $"{Options.ServerUrl}/databases/{Options.DatabaseName}/streams/docs?etag={lastEtag}";
                var request = new HttpRequestMessage(HttpMethod.Get, url);

                var responseMessage = await Parameters.HttpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, Parameters.CancelToken.Token);
                return responseMessage;
            });
            
            if (response.IsSuccessStatusCode == false)
            {
                var responseString = await response.Content.ReadAsStringAsync();
                throw new InvalidOperationException($"Failed to export documents from server: {Options.ServerUrl}, " +
                                                    $"status code: {response.StatusCode}, " +
                                                    $"error: {responseString}");
            }

            using (var responseStream = await response.Content.ReadAsStreamAsync())
            using (Parameters.Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (var source = new StreamSource(responseStream, context, Parameters.Database))
            {
                var destination = new DatabaseDestination(Parameters.Database);
                var options = new DatabaseSmugglerOptionsServerSide
                {
#pragma warning disable 618
                    ReadLegacyEtag = true,
#pragma warning restore 618
                    TransformScript = Options.TransformScript,
                    OperateOnTypes = Options.OperateOnTypes
                };
                var smuggler = new DatabaseSmuggler(Parameters.Database, source, destination, Parameters.Database.Time, options, Parameters.Result, Parameters.OnProgress, Parameters.CancelToken.Token);

                // since we will be migrating indexes as separate task don't ensureStepsProcessed at this point
                await smuggler.ExecuteAsync(ensureStepsProcessed: false);
            }
        }

        private async Task MigrateAttachments(string lastEtag, SmugglerResult parametersResult)
        {
            var destination = new DatabaseDestination(Parameters.Database);
            var options = new DatabaseSmugglerOptionsServerSide
            {
                OperateOnTypes = DatabaseItemType.Attachments,
                SkipRevisionCreation = true
            };

            destination.InitializeAsync(options, parametersResult, buildVersion: default);

            using (Parameters.Database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext transactionOperationContext))
            await using (var documentActions = destination.Documents())
            {
                var sp = Stopwatch.StartNew();

                while (true)
                {
                    var attachmentsArray = await GetAttachmentsList(lastEtag, transactionOperationContext);
                    if (attachmentsArray.Length == 0)
                    {
                        var count = Parameters.Result.Documents.ReadCount;
                        if (count > 0)
                        {
                            var message = $"Read {count:#,#;;0} legacy attachment{(count > 1 ? "s" : string.Empty)}.";
                            Parameters.Result.AddInfo(message);
                            Parameters.OnProgress.Invoke(Parameters.Result.Progress);
                        }

                        return;
                    }

                    foreach (var attachmentObject in attachmentsArray)
                    {
                        var blittable = attachmentObject as BlittableJsonReaderObject;
                        if (blittable == null)
                            throw new InvalidDataException("attachmentObject isn't a BlittableJsonReaderObject");

                        if (blittable.TryGet("Key", out string key) == false)
                            throw new InvalidDataException("Key doesn't exist");

                        if (blittable.TryGet("Metadata", out BlittableJsonReaderObject metadata) == false)
                            throw new InvalidDataException("Metadata doesn't exist");

                        var dataStream = await GetAttachmentStream(key);
                        if (dataStream == null)
                        {
                            Parameters.Result.Tombstones.ReadCount++;
                            var id = StreamSource.GetLegacyAttachmentId(key);
                            await documentActions.DeleteDocumentAsync(id);
                            continue;
                        }

                        var contextToUse = documentActions.GetContextForNewDocument();
                        using (var old = metadata)
                            metadata = metadata.Clone(contextToUse);

                        await WriteDocumentWithAttachmentAsync(documentActions, contextToUse, dataStream, key, metadata);

                        Parameters.Result.Documents.ReadCount++;
                        if (Parameters.Result.Documents.ReadCount % 50 == 0 || sp.ElapsedMilliseconds > 3000)
                        {
                            var message = $"Read {Parameters.Result.Documents.ReadCount:#,#;;0} legacy attachments.";
                            Parameters.Result.AddInfo(message);
                            Parameters.OnProgress.Invoke(Parameters.Result.Progress);
                            sp.Restart();
                        }
                    }

                    var lastAttachment = attachmentsArray.Last() as BlittableJsonReaderObject;
                    Debug.Assert(lastAttachment != null, "lastAttachment != null");
                    if (lastAttachment.TryGet("Etag", out string etag))
                        lastEtag = Parameters.Result.LegacyLastAttachmentEtag = etag;
                }
            }
        }

        private async Task<BlittableJsonReaderArray> GetAttachmentsList(string lastEtag, TransactionOperationContext context)
        {
            var response = await RunWithAuthRetry(async () =>
            {
                var url = $"{Options.ServerUrl}/databases/{Options.DatabaseName}/static?pageSize={AttachmentsPageSize}&etag={lastEtag}";
                var request = new HttpRequestMessage(HttpMethod.Get, url);
                var responseMessage = await Parameters.HttpClient.SendAsync(request, Parameters.CancelToken.Token);
                return responseMessage;
            });
            
            if (response.IsSuccessStatusCode == false)
            {
                var responseString = await response.Content.ReadAsStringAsync();
                throw new InvalidOperationException($"Failed to get attachments list from server: {Options.ServerUrl}, " +
                                                    $"status code: {response.StatusCode}, " +
                                                    $"error: {responseString}");
            }

            const string propertyName = "Attachments";
            using (var responseStream = await response.Content.ReadAsStreamAsync())
            using (var attachmentsListStream = new ArrayStream(responseStream, propertyName))
            {
                var attachmentsList = await context.ReadForMemoryAsync(attachmentsListStream, "attachments-list");
                if (attachmentsList.TryGet(propertyName, out BlittableJsonReaderArray attachments) == false)
                    throw new InvalidDataException("Response is invalid");

                return attachments;
            }
        }

        private async Task<Stream> GetAttachmentStream(string attachmentKey)
        {
            var response = await RunWithAuthRetry(async () =>
            {
                var url = $"{Options.ServerUrl}/databases/{Options.DatabaseName}/static/{Uri.EscapeDataString(attachmentKey)}";
                var request = new HttpRequestMessage(HttpMethod.Get, url);
                var responseMessage = await Parameters.HttpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, Parameters.CancelToken.Token);
                return responseMessage;
            });

            if (response.StatusCode == HttpStatusCode.NotFound)
            {
                // the attachment was deleted
                return null;
            }

            if (response.IsSuccessStatusCode == false)
            {
                var responseString = await response.Content.ReadAsStringAsync();
                throw new InvalidOperationException($"Failed to get attachment, key: {attachmentKey}, from server: {Options.ServerUrl}, " +
                                                    $"status code: {response.StatusCode}, " +
                                                    $"error: {responseString}");
            }

            return await response.Content.ReadAsStreamAsync();
        }

        private async Task MigrateIndexes()
        {
            var response = await RunWithAuthRetry(async () =>
            {
                var url = $"{Options.ServerUrl}/databases/{Options.DatabaseName}/indexes";
                var request = new HttpRequestMessage(HttpMethod.Get, url);
                var responseMessage = await Parameters.HttpClient.SendAsync(request, Parameters.CancelToken.Token);
                return responseMessage;
            });
            
            if (response.IsSuccessStatusCode == false)
            {
                var responseString = await response.Content.ReadAsStringAsync();
                throw new InvalidOperationException($"Failed to export indexes from server: {Options.ServerUrl}, " +
                                                    $"status code: {response.StatusCode}, " +
                                                    $"error: {responseString}");
            }

            using (var responseStream = await response.Content.ReadAsStreamAsync())
            using (var indexesStream = new ArrayStream(responseStream, "Indexes")) // indexes endpoint returns an array
            using (Parameters.Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (var source = new StreamSource(indexesStream, context, Parameters.Database))
            {
                var destination = new DatabaseDestination(Parameters.Database);
                var options = new DatabaseSmugglerOptionsServerSide
                {
                    RemoveAnalyzers = Options.RemoveAnalyzers,
                };
                var smuggler = new DatabaseSmuggler(Parameters.Database, source, destination, Parameters.Database.Time, options, Parameters.Result, Parameters.OnProgress, Parameters.CancelToken.Token);

                await smuggler.ExecuteAsync();
            }
        }
    }
}
