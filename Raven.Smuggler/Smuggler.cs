// -----------------------------------------------------------------------
//  <copyright file="Smuggler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Smuggler;
using Raven.Abstractions.Util;
using Raven.Client.Connection.Async;
using Raven.Client.Document;
using Raven.Client.Extensions;
using Raven.Json.Linq;

namespace Raven.Smuggler
{
    public static class SmugglerOp
    {
        const int RetriesCount = 5;

        public static async Task Between(SmugglerBetweenOptions options)
        {
            SetDatabaseNameIfEmpty(options.From);
            SetDatabaseNameIfEmpty(options.To);

            using (var exportStore = CreateStore(options.From))
            using (var importStore = CreateStore(options.To))
            {
                await EnsureDatabaseExists(importStore, options.To.DefaultDatabase);

                var exportStoreSupportedFeatures = await DetectServerSupportedFeatures(exportStore);
                var importStoreSupportedFeatures = await DetectServerSupportedFeatures(importStore);

                if (options.OperateOnTypes.HasFlag(ItemType.Indexes))
                {
                    await ExportIndexes(exportStore, importStore, options.BatchSize);
                }
                if (options.OperateOnTypes.HasFlag(ItemType.Transformers) && exportStoreSupportedFeatures.IsTransformersSupported && importStoreSupportedFeatures.IsTransformersSupported)
                {
                    await ExportTransformers(exportStore, importStore, options.BatchSize);
                }
                if (options.OperateOnTypes.HasFlag(ItemType.Documents))
                {
                    await ExportDocuments(exportStore, importStore, options, exportStoreSupportedFeatures);
                }
                if (options.OperateOnTypes.HasFlag(ItemType.Attachments))
                {
                    await ExportAttachments(exportStore, importStore, options);
                }
            }
        }

        private static async Task EnsureDatabaseExists(DocumentStore store, string databaseName)
        {
            if (string.IsNullOrWhiteSpace(databaseName))
                return;

            var doc = MultiDatabase.CreateDatabaseDocument(databaseName);

			var get = await store.AsyncDatabaseCommands.GetAsync(doc.Id);
			if (get != null)
				return;

            await store.AsyncDatabaseCommands.GlobalAdmin.CreateDatabaseAsync(doc);
        }

        private static void SetDatabaseNameIfEmpty(RavenConnectionStringOptions connection)
        {
            if (string.IsNullOrWhiteSpace(connection.DefaultDatabase) == false)
                return;
            
            var index = connection.Url.IndexOf("/databases/", StringComparison.OrdinalIgnoreCase);
            if (index != -1)
            {
                connection.DefaultDatabase = connection.Url.Substring(index + "/databases/".Length).Trim(new[] {'/'});
            }
        }

        private static async Task ExportIndexes(DocumentStore exportStore, DocumentStore importStore, int batchSize)
        {
            var totalCount = 0;
            while (true)
            {
                var indexes = await exportStore.AsyncDatabaseCommands.GetIndexesAsync(totalCount, batchSize);
                if (indexes.Length == 0)
                {
                    ShowProgress("Done with reading indexes, total: {0}", totalCount);
                    break;
                }
                totalCount += indexes.Length;
                ShowProgress("Reading batch of {0,3} indexes, read so far: {1,10:#,#;;0}", indexes.Length, totalCount);
                foreach (var index in indexes)
                {
                    var indexName = await importStore.AsyncDatabaseCommands.PutIndexAsync(index.Name, index, true);
                    ShowProgress("Successfully PUT index '{0}'", indexName);
                }
            }
        }

        private static async Task<Etag> ExportDocuments(DocumentStore exportStore, DocumentStore importStore, SmugglerOptionsBase options, ServerSupportedFeatures exportStoreSupportedFeatures)
        {
            string lastEtag = options.StartDocsEtag;
            var totalCount = 0;
            var lastReport = SystemTime.UtcNow;
            var reportInterval = TimeSpan.FromSeconds(2);
            ShowProgress("Exporting Documents");

            var bulkInsertOperation = importStore.BulkInsert(null, new BulkInsertOptions
            {
                BatchSize = options.BatchSize,
                CheckForUpdates = true,
            });
            bulkInsertOperation.Report += text => ShowProgress(text);

            try
            {
                while (true)
                {
                    if (exportStoreSupportedFeatures.IsDocsStreamingSupported)
                    {
                        ShowProgress("Streaming documents from " + lastEtag);
                        using (var documentsEnumerator = await exportStore.AsyncDatabaseCommands.StreamDocsAsync(lastEtag))
                        {
                            while (await documentsEnumerator.MoveNextAsync())
                            {
                                var document = documentsEnumerator.Current;

                                if (!options.MatchFilters(document))
                                    continue;
                                if (options.ShouldExcludeExpired && options.ExcludeExpired(document))
                                    continue;

                                var metadata = document.Value<RavenJObject>("@metadata");
                                var id = metadata.Value<string>("@id");
                                var etag = Etag.Parse(metadata.Value<string>("@etag"));
                                document.Remove("@metadata");
                                bulkInsertOperation.Store(document, metadata, id);
                                totalCount++;

                                if (totalCount%1000 == 0 || SystemTime.UtcNow - lastReport > reportInterval)
                                {
                                    ShowProgress("Exported {0} documents", totalCount);
                                    lastReport = SystemTime.UtcNow;
                                }

                                lastEtag = etag;
                            }
                        }
                    }
                    else
                    {
                        int retries = RetriesCount;
                        var originalRequestTimeout = exportStore.JsonRequestFactory.RequestTimeout;
                        var timeout = options.Timeout.Seconds;
                        if (timeout < 30)
                            timeout = 30;
                        try
                        {
                            while (true)
                            {
                                try
                                {
                                    ShowProgress("Get documents from " + lastEtag);
                                    var documents = await ((AsyncServerClient)exportStore.AsyncDatabaseCommands).GetDocumentsInternalAsync(null, lastEtag, options.BatchSize);
                                    foreach (RavenJObject document in documents)
                                    {
                                        var metadata = document.Value<RavenJObject>("@metadata");
                                        var id = metadata.Value<string>("@id");
                                        var etag = Etag.Parse(metadata.Value<string>("@etag"));
                                        document.Remove("@metadata");
                                        metadata.Remove("@id");
                                        metadata.Remove("@etag");

                                        if (!options.MatchFilters(document))
                                            continue;
                                        if (options.ShouldExcludeExpired && options.ExcludeExpired(document))
                                            continue;

                                        bulkInsertOperation.Store(document, metadata, id);
                                        totalCount++;

                                        if (totalCount%1000 == 0 || SystemTime.UtcNow - lastReport > reportInterval)
                                        {
                                            ShowProgress("Exported {0} documents", totalCount);
                                            lastReport = SystemTime.UtcNow;
                                        }
                                        lastEtag = etag;
                                    }
                                    break;
                                }
                                catch (Exception e)
                                {
                                    if (retries-- == 0)
                                        throw;
                                    exportStore.JsonRequestFactory.RequestTimeout = TimeSpan.FromSeconds(timeout *= 2);
                                    importStore.JsonRequestFactory.RequestTimeout = TimeSpan.FromSeconds(timeout *= 2);
                                    ShowProgress("Error reading from database, remaining attempts {0}, will retry. Error: {1}", retries, e);
                                }
                            }
                        }
                        finally
                        {
                            exportStore.JsonRequestFactory.RequestTimeout = originalRequestTimeout;
                        }
                    }

                    // In a case that we filter all the results, the formEtag hasn't updaed to the latest, 
                    // but we still need to continue until we finish all the docs.
                    var databaseStatistics = await exportStore.AsyncDatabaseCommands.GetStatisticsAsync();
                    var lastEtagComparable = new ComparableByteArray(lastEtag);
                    if (lastEtagComparable.CompareTo(databaseStatistics.LastDocEtag) < 0)
                    {
                        lastEtag = EtagUtil.Increment(lastEtag, options.BatchSize);
                        ShowProgress("Got no results but didn't get to the last doc etag, trying from: {0}", lastEtag);
                        continue;
                    }

                    ShowProgress("Done with reading documents, total: {0}", totalCount);
                    return lastEtag;
                }
            }
            finally
            {
                bulkInsertOperation.Dispose();
            }
        }

        private async static Task<Etag> ExportAttachments(DocumentStore exportStore, DocumentStore importStore, SmugglerOptionsBase options)
        {
            Etag lastEtag = options.StartAttachmentsEtag;
            int totalCount = 0;
            while (true)
            {
                var attachments = await exportStore.AsyncDatabaseCommands.GetAttachmentsAsync(lastEtag, options.BatchSize);
                if (attachments.Length == 0)
                {
                    var databaseStatistics = await exportStore.AsyncDatabaseCommands.GetStatisticsAsync();
                    var lastEtagComparable = new ComparableByteArray(lastEtag);
                    if (lastEtagComparable.CompareTo(databaseStatistics.LastAttachmentEtag) < 0)
                    {
                        lastEtag = EtagUtil.Increment(lastEtag, options.BatchSize);
                        ShowProgress("Got no results but didn't get to the last attachment etag, trying from: {0}", lastEtag);
                        continue;
                    }
                    ShowProgress("Done with reading attachments, total: {0}", totalCount);
                    return lastEtag;
                }

                totalCount += attachments.Length;
                ShowProgress("Reading batch of {0,3} attachments, read so far: {1,10:#,#;;0}", attachments.Length, totalCount);
                foreach (var attachmentInformation in attachments)
                {
                    ShowProgress("Downloading attachment: {0}", attachmentInformation.Key);

                    var attachment = await exportStore.AsyncDatabaseCommands.GetAttachmentAsync(attachmentInformation.Key);
                    await importStore.AsyncDatabaseCommands.PutAttachmentAsync(attachment.Key, null, attachment.Data(), attachment.Metadata);
                }

                lastEtag = Etag.Parse(attachments.Last().Etag);
            }
        }

        private static async Task ExportTransformers(DocumentStore exportStore, DocumentStore importStore, int batchSize)
        {
            var totalCount = 0;
            while (true)
            {
                var transformers = await exportStore.AsyncDatabaseCommands.GetTransformersAsync(totalCount, batchSize);
                if (transformers.Length == 0)
                {
                    ShowProgress("Done with reading transformers, total: {0}", totalCount);
                    break;
                }
                totalCount += transformers.Length;
                ShowProgress("Reading batch of {0,3} transformers, read so far: {1,10:#,#;;0}", transformers.Length, totalCount);
                foreach (var transformer in transformers)
                {
                    var transformerName = await importStore.AsyncDatabaseCommands.PutTransformerAsync(transformer.Name, transformer);
                    ShowProgress("Successfully PUT transformer '{0}'", transformerName);
                }
            }
        }

        private static DocumentStore CreateStore(RavenConnectionStringOptions connection)
        {
            var store = new DocumentStore
            {
                Url = connection.Url,
                ApiKey = connection.ApiKey,
                Credentials = connection.Credentials,
                DefaultDatabase = connection.DefaultDatabase,
                Conventions =
                {
                    FailoverBehavior = FailoverBehavior.FailImmediately,
                    ShouldCacheRequest = s => false,
                    ShouldAggressiveCacheTrackChanges = false,
                    ShouldSaveChangesForceAggressiveCacheCheck = false,
                }
            };
            store.Initialize();
            store.JsonRequestFactory.DisableAllCaching();
            return store;
        }

        private static async Task<ServerSupportedFeatures> DetectServerSupportedFeatures(DocumentStore store)
        {
#if !SILVERLIGHT
            var buildNumber = await store.AsyncDatabaseCommands.GetBuildNumberAsync();
            if (buildNumber == null || string.IsNullOrEmpty(buildNumber.ProductVersion))
            {
                ShowProgress("Server version is not available. Running in legacy mode which does not support transformers and documents streaming.");
                return new ServerSupportedFeatures
                {
                    IsTransformersSupported = false,
                    IsDocsStreamingSupported = false,
                };
            }

            var smugglerVersion = FileVersionInfo.GetVersionInfo(typeof(SmugglerApiBase).Assembly.Location).ProductVersion;
            var subSmugglerVersion = smugglerVersion.Substring(0, 3);

            var subServerVersion = buildNumber.ProductVersion.Substring(0, 3);
            var intServerVersion = int.Parse(subServerVersion.Replace(".", string.Empty));

            if (intServerVersion < 25)
            {
                ShowProgress("Running in legacy mode, importing/exporting transformers is not supported. Server version: {0}. Smuggler version: {1}.", subServerVersion, subSmugglerVersion);
                return new ServerSupportedFeatures
                {
                    IsTransformersSupported = false,
                    IsDocsStreamingSupported = false,
                };
            }
#endif

            return new ServerSupportedFeatures
            {
                IsTransformersSupported = true,
                IsDocsStreamingSupported = true,
            };
        }

        [StringFormatMethod("format")]
        private static void ShowProgress(string format, params object[] args)
        {
            Console.WriteLine(format, args);
        }
    }

    public class ServerSupportedFeatures
    {
        public bool IsTransformersSupported { get; set; }
        public bool IsDocsStreamingSupported { get; set; }
    }
}