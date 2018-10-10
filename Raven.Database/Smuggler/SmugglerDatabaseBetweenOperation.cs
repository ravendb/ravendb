using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Smuggler;
using Raven.Abstractions.Smuggler.Data;
using Raven.Abstractions.Util;
using Raven.Json.Linq;

namespace Raven.Smuggler
{
    public class SmugglerBetweenOperations
    {
        public ISmugglerDatabaseOperations From { get; set; }

        public ISmugglerDatabaseOperations To { get; set; }

        /// <summary>
        /// You can give a key to the incremental last etag, in order to make incremental imports from a few export sources.
        /// </summary>
        public string IncrementalKey { get; set; }
    }

    internal class SmugglerDatabaseBetweenOperation
    {
        public Action<string> OnShowProgress { get; set; } 

        public async Task Between(
            SmugglerBetweenOperations betweenOperations, 
            SmugglerDatabaseOptions databaseOptions)
        {
            var exportOperations = betweenOperations.From;
            var importOperations = betweenOperations.To;
            
            exportOperations.Configure(databaseOptions);
            exportOperations.Initialize(databaseOptions);

            importOperations.Configure(databaseOptions);
            importOperations.Initialize(databaseOptions);

            if (string.IsNullOrEmpty(betweenOperations.IncrementalKey))
            {
                betweenOperations.IncrementalKey = exportOperations.GetIdentifier();
            }

            var incremental = new ExportIncremental();
            if (databaseOptions.Incremental)
            {
                var jsonDocument = importOperations.GetDocument(SmugglerExportIncremental.RavenDocumentKey);
                if (jsonDocument != null)
                {
                    var smugglerExportIncremental = jsonDocument.DataAsJson.JsonDeserialization<SmugglerExportIncremental>();
                    ExportIncremental value;
                    if (smugglerExportIncremental.ExportIncremental.TryGetValue(betweenOperations.IncrementalKey, out value))
                    {
                        incremental = value;
                    }

                    databaseOptions.StartDocsEtag = incremental.LastDocsEtag ?? Etag.Empty;
                    databaseOptions.StartAttachmentsEtag = incremental.LastAttachmentsEtag ?? Etag.Empty;
                }
            }

            if (databaseOptions.OperateOnTypes.HasFlag(ItemType.Indexes))
            {
                await ExportIndexes(exportOperations, importOperations).ConfigureAwait(false);
            }

            if (databaseOptions.OperateOnTypes.HasFlag(ItemType.Transformers))
            {
                await ExportTransformers(exportOperations, importOperations).ConfigureAwait(false);
            }

            if (databaseOptions.OperateOnTypes.HasFlag(ItemType.Documents))
            {
                incremental.LastDocsEtag = await ExportDocuments(exportOperations, importOperations, databaseOptions).ConfigureAwait(false);
            }

            if (databaseOptions.OperateOnTypes.HasFlag(ItemType.Attachments))
            {
                incremental.LastAttachmentsEtag = await ExportAttachments(exportOperations, importOperations, databaseOptions).ConfigureAwait(false);
            }

            await ExportIdentities(exportOperations, importOperations, databaseOptions.OperateOnTypes).ConfigureAwait(false);

            if (databaseOptions.Incremental)
            {
                var smugglerExportIncremental = new SmugglerExportIncremental();
                var jsonDocument = importOperations.GetDocument(SmugglerExportIncremental.RavenDocumentKey);
                if (jsonDocument != null)
                {
                    smugglerExportIncremental = jsonDocument.DataAsJson.JsonDeserialization<SmugglerExportIncremental>();
                }
                smugglerExportIncremental.ExportIncremental[betweenOperations.IncrementalKey] = incremental;

                var smugglerDoc = RavenJObject.FromObject(smugglerExportIncremental);
                
                smugglerDoc.Add("@metadata", new RavenJObject
                {
                    { "@id", SmugglerExportIncremental.RavenDocumentKey }
                });

                await importOperations.PutDocument(smugglerDoc, (int)DocumentHelpers.GetRoughSize(smugglerDoc)).ConfigureAwait(false);
                await importOperations.PutDocument(null, -1).ConfigureAwait(false); // force flush
            }
        }

        private async Task ExportIdentities(ISmugglerDatabaseOperations exportOperations, 
            ISmugglerDatabaseOperations importOperations, 
            ItemType operateOnTypes)
        {
            ShowProgress("Exporting Identities");

            var identities = await exportOperations.GetIdentities().ConfigureAwait(false);

            ShowProgress("Got {0} following identities: {1}", identities.Count, string.Join(", ", identities.Select(x => x.Key)));

            var filteredIdentities = identities.Where(x =>
            {
                if ("Raven/Etag".Equals(x.Key, StringComparison.InvariantCultureIgnoreCase))
                    return false;

                if ("IndexId".Equals(x.Key, StringComparison.InvariantCultureIgnoreCase) && operateOnTypes.HasFlag(ItemType.Indexes))
                    return false;

                if (Constants.RavenSubscriptionsPrefix.Equals(x.Key, StringComparison.OrdinalIgnoreCase))
                    return false;

                if (operateOnTypes.HasFlag(ItemType.Documents))
                    return true;

                return false;
            }).ToList();
            
            ShowProgress("After filtering {0} the following identities need to be exported: {1}", filteredIdentities.Count, string.Join(", ", filteredIdentities.Select(x => x.Key)));
            foreach (var identityInfo in filteredIdentities)
            {
                await importOperations.SeedIdentityFor(identityInfo.Key, identityInfo.Value).ConfigureAwait(false);
                ShowProgress("Identity '{0}' exported with value {1}", identityInfo.Key, identityInfo.Value);
            }

            ShowProgress("Done with exporting identities");
        }

        private async Task ExportIndexes(ISmugglerDatabaseOperations exportOperations, ISmugglerDatabaseOperations importOperations)
        {
            var totalCount = 0;
            while (true)
            {
                var indexes = await exportOperations.GetIndexes(totalCount).ConfigureAwait(false);
                if (indexes.Length == 0)
                {
                    ShowProgress("Done with reading indexes, total: {0}", totalCount);
                    break;
                }

                totalCount += indexes.Length;
                ShowProgress("Reading batch of {0,3} indexes, read so far: {1,10:#,#;;0}", indexes.Length, totalCount);

                foreach (var index in indexes)
                {
                    var indexName = index.Value<string>("name");
                    await importOperations.PutIndex(indexName, index).ConfigureAwait(false);
                    ShowProgress("Successfully PUT index '{0}'", indexName);
                }
            }
        }

        private async Task<Etag> ExportDocuments(ISmugglerDatabaseOperations exportOperations, ISmugglerDatabaseOperations importOperations, SmugglerDatabaseOptions databaseOptions)
        {
            var now = SystemTime.UtcNow;

            string lastEtag = databaseOptions.StartDocsEtag;
            var totalCount = 0;
            var numberOfSkippedDocs = 0;
            var totalOfSkippedDocs = 0;
            var lastForcedFlush = SystemTime.UtcNow;
            var maxFlushInterval = TimeSpan.FromSeconds(1);
            var lastReport = SystemTime.UtcNow;
            var reportInterval = TimeSpan.FromSeconds(2);
            ShowProgress("Exporting Documents");

            while (true)
            {
                bool hasDocs = false;
                try
                {
                    var maxRecords = databaseOptions.Limit - totalCount;
                    if (maxRecords > 0)
                    {
                        var amountToFetchFromServer = Math.Min(databaseOptions.BatchSize, maxRecords);
                        using (var documents = await exportOperations.GetDocuments(lastEtag, amountToFetchFromServer).ConfigureAwait(false))
                        {
                            while (await documents.MoveNextAsync().ConfigureAwait(false))
                            {
                                if (numberOfSkippedDocs > 0 && (SystemTime.UtcNow - lastForcedFlush) > maxFlushInterval)
                                {
                                    totalOfSkippedDocs += numberOfSkippedDocs;
                                    ShowProgress("Skipped {0:#,#} documents", totalOfSkippedDocs);
                                    lastForcedFlush = SystemTime.UtcNow;
                                    numberOfSkippedDocs = 0;
                                }

                                hasDocs = true;
                                var document = documents.Current;

                                var tempLastEtag = Etag.Parse(document.Value<RavenJObject>("@metadata").Value<string>("@etag"));

                                Debug.Assert(!String.IsNullOrWhiteSpace(document.Value<RavenJObject>("@metadata").Value<string>("@id")));

                                lastEtag = tempLastEtag;

                                if (!databaseOptions.MatchFilters(document))
                                {
                                    numberOfSkippedDocs++;
                                    continue;
                                }

                                if (databaseOptions.ShouldExcludeExpired && databaseOptions.ExcludeExpired(document, now))
                                {
                                    numberOfSkippedDocs++;
                                    continue;
                                }

                                if (databaseOptions.StripReplicationInformation)
                                    document["@metadata"] = StripReplicationInformationFromMetadata(document["@metadata"] as RavenJObject);

                                if (databaseOptions.ShouldDisableVersioningBundle)
                                    document["@metadata"] = DisableVersioning(document["@metadata"] as RavenJObject);

                                document["@metadata"] = SmugglerHelper.HandleConflictDocuments(document["@metadata"] as RavenJObject);

                                if (string.IsNullOrEmpty(databaseOptions.TransformScript) == false)
                                    document = await exportOperations.TransformDocument(document, databaseOptions.TransformScript).ConfigureAwait(false);

                                if (document == null)
                                {
                                    numberOfSkippedDocs++;
                                    continue;
                                }

                                await importOperations.PutDocument(document, (int) DocumentHelpers.GetRoughSize(document)).ConfigureAwait(false);
                                totalCount++;

                                if (totalCount % 1000 == 0 || SystemTime.UtcNow - lastReport > reportInterval)
                                {
                                    ShowProgress("Exported {0} documents", totalCount);
                                    lastReport = SystemTime.UtcNow;
                                }
                            }
                        }

                        if (hasDocs)
                            continue;

                        // The server can filter all the results. In this case, we need to try to go over with the next batch.
                        // Note that if the ETag' server restarts number is not the same, this won't guard against an infinite loop.
                        // (This code provides support for legacy RavenDB version: 1.0)
                        var databaseStatistics = await exportOperations.GetStats().ConfigureAwait(false);
                        var lastEtagComparable = new ComparableByteArray(lastEtag);
                        if (lastEtagComparable.CompareTo(databaseStatistics.LastDocEtag) < 0)
                        {
                            lastEtag = EtagUtil.Increment(lastEtag, amountToFetchFromServer);
                            if (lastEtag.CompareTo(databaseStatistics.LastDocEtag) >= 0)
                            {
                                lastEtag = databaseStatistics.LastDocEtag;
                            }
                            ShowProgress("Got no results but didn't get to the last doc etag, trying from: {0}", lastEtag);
                            continue;
                        }
                    }
                }
                catch (Exception e)
                {
                    ShowProgress("Got Exception during smuggler export. Exception: {0}. ", e.Message);
                    ShowProgress("Done with reading documents, total: {0}, lastEtag: {1}", totalCount, lastEtag);
                    throw new SmugglerExportException(e.Message, e)
                    {
                        LastEtag = lastEtag,
                    };
                }

                // Load HiLo documents for selected collections
                databaseOptions.Filters.ForEach(filter =>
                {
                    if (string.Equals(filter.Path, "@metadata.Raven-Entity-Name", StringComparison.OrdinalIgnoreCase))
                    {
                        filter.Values.ForEach(collectionName =>
                        {
                            JsonDocument doc = exportOperations.GetDocument("Raven/Hilo/" + collectionName);
                            if (doc != null)
                            {
                                doc.Metadata["@id"] = doc.Key;
                                var jsonDoc = doc.ToJson();
                                AsyncHelpers.RunSync(() => importOperations.PutDocument(jsonDoc, (int)DocumentHelpers.GetRoughSize(jsonDoc)));
                                totalCount++;
                            }
                        });
                    }
                });

                await importOperations.PutDocument(null, -1).ConfigureAwait(false); // force flush 

                ShowProgress("Done with reading documents, total: {0}, lastEtag: {1}", totalCount, lastEtag);
                return lastEtag;
            }
        }

        [Obsolete("Use RavenFS instead.")]
        private async Task<Etag> ExportAttachments(ISmugglerDatabaseOperations exportOperations, ISmugglerDatabaseOperations importOperations, SmugglerDatabaseOptions databaseOptions)
        {
            Etag lastEtag = databaseOptions.StartAttachmentsEtag;
            int totalCount = 0;

            while (true)
            {
                try
                {
                    if (databaseOptions.Limit - totalCount <= 0)
                    {
                        await importOperations.PutAttachment(null).ConfigureAwait(false); // force flush

                        ShowProgress("Done with reading attachments, total: {0}", totalCount);
                        return lastEtag;
                    }
                    var maxRecords = Math.Min(databaseOptions.Limit - totalCount, databaseOptions.BatchSize);
                    var attachments = await exportOperations.GetAttachments(totalCount, lastEtag, maxRecords).ConfigureAwait(false);
                    if (attachments.Count == 0)
                    {
                        var databaseStatistics = await exportOperations.GetStats().ConfigureAwait(false);
                        if (lastEtag == null) lastEtag = Etag.Empty;
                        if (lastEtag.CompareTo(databaseStatistics.LastAttachmentEtag) < 0)
                        {
                            lastEtag = EtagUtil.Increment(lastEtag, maxRecords);
                            ShowProgress("Got no results but didn't get to the last attachment etag, trying from: {0}",
                                         lastEtag);
                            continue;
                        }
                        ShowProgress("Done with reading attachments, total: {0}", totalCount);
                        return lastEtag;
                    }

                    totalCount += attachments.Count;
                    ShowProgress("Reading batch of {0,3} attachments, read so far: {1,10:#,#;;0}", attachments.Count, totalCount);

                    foreach (var attachment in attachments)
                    {
                        var attachmentData = await exportOperations.GetAttachmentData(attachment).ConfigureAwait(false);
                        if (attachmentData == null)
                            continue;

                        var attachmentToExport = new AttachmentExportInfo
                        {
                            Key = attachment.Key,
                            Metadata = attachment.Metadata,
                            Data = new MemoryStream(attachmentData)
                        };

                        if (databaseOptions.StripReplicationInformation)
                            attachmentToExport.Metadata = StripReplicationInformationFromMetadata(attachmentToExport.Metadata);

                        await importOperations.PutAttachment(attachmentToExport).ConfigureAwait(false);

                        lastEtag = attachment.Etag;
                    }
                }
                catch (Exception e)
                {
                    ShowProgress("Got Exception during smuggler export. Exception: {0}. ", e.Message);
                    ShowProgress("Done with reading attachments, total: {0}", totalCount, lastEtag);
                    throw new SmugglerExportException(e.Message, e)
                    {
                        LastEtag = lastEtag,
                    };
                }
            }
        }

        private async Task ExportTransformers(ISmugglerDatabaseOperations exportOperations, ISmugglerDatabaseOperations importOperations)
        {
            var totalCount = 0;
            while (true)
            {
                var transformers = await exportOperations.GetTransformers(totalCount).ConfigureAwait(false);
                if (transformers.Length == 0)
                {
                    ShowProgress("Done with reading transformers, total: {0}", totalCount);
                    break;
                }
                totalCount += transformers.Length;
                ShowProgress("Reading batch of {0,3} transformers, read so far: {1,10:#,#;;0}", transformers.Length, totalCount);
                foreach (var transformer in transformers)
                {
                    var transformerName = transformer.Value<string>("name");

                    await importOperations.PutTransformer(transformerName, transformer).ConfigureAwait(false);
                    ShowProgress("Successfully PUT transformer '{0}'", transformerName);
                }
            }
        }

        

        // [StringFormatMethod("format")]
        private void ShowProgress(string format, params object[] args)
        {
            var message = string.Format(format, args);

            Console.WriteLine(message);

            var action = OnShowProgress;
            if (action != null)
                action(message);
        }

        public RavenJObject StripReplicationInformationFromMetadata(RavenJObject metadata)
        {
            if (metadata != null)
            {
                metadata.Remove(Constants.RavenReplicationHistory);
                metadata.Remove(Constants.RavenReplicationSource);
                metadata.Remove(Constants.RavenReplicationVersion);
            }

            return metadata;
        }

        public RavenJToken DisableVersioning(RavenJObject metadata)
        {
            if (metadata != null)
            {
                metadata[Constants.RavenIgnoreVersioning] = true;
            }

            return metadata;
        }
    }
}
