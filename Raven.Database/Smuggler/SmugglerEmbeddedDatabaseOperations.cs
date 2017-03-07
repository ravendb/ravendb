// -----------------------------------------------------------------------
//  <copyright file="SmugglerEmbeddedDatabaseOperations.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Smuggler;
using Raven.Abstractions.Smuggler.Data;
using Raven.Abstractions.Util;
using Raven.Database.Data;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Database.Smuggler
{
    public class SmugglerEmbeddedDatabaseOperations : ISmugglerDatabaseOperations
    {
        private readonly DocumentDatabase database;
        private readonly TimeSpan timeout;
        private readonly long maxSize;

        private List<JsonDocument> bulkInsertBatch = new List<JsonDocument>();

        private readonly SmugglerJintHelper scriptedJsonPatcher = new SmugglerJintHelper();
        private readonly Etag etagEmpty = Etag.Empty;

        public SmugglerEmbeddedDatabaseOperations(DocumentDatabase database)
        {
            this.database = database;
            timeout = TimeSpan.FromSeconds(database.Configuration.Prefetcher.FetchingDocumentsFromDiskTimeoutInSeconds);
            maxSize = 10*1024*1024; //10MB
        }

        public Action<string> Progress { get; set; }

        public Task<RavenJArray> GetIndexes(int totalCount)
        {
            return new CompletedTask<RavenJArray>(database.Indexes.GetIndexes(totalCount, 128));
        }

        public JsonDocument GetDocument(string key)
        {
            return database.Documents.Get(key, null);
        }

        public Task<IAsyncEnumerator<RavenJObject>> GetDocuments(Etag lastEtag, int take)
        {
            const int dummy = 0;
            
            var enumerator = database.Documents.GetDocumentsAsJson(dummy, Math.Min(Options.BatchSize, take), lastEtag,
                    CancellationToken.None, maxSize, timeout)
                .ToList()
                .Cast<RavenJObject>()
                .GetEnumerator();

            return new CompletedTask<IAsyncEnumerator<RavenJObject>>(new AsyncEnumeratorBridge<RavenJObject>(enumerator));
        }

        [Obsolete("Use RavenFS instead.")]
        public Task<Etag> ExportAttachmentsDeletion(SmugglerJsonTextWriter jsonWriter, Etag startAttachmentsDeletionEtag, Etag maxAttachmentEtag)
        {
            var lastEtag = startAttachmentsDeletionEtag;
            database.TransactionalStorage.Batch(accessor =>
            {
                foreach (var listItem in accessor.Lists.Read(Constants.RavenPeriodicExportsAttachmentsTombstones, startAttachmentsDeletionEtag, maxAttachmentEtag, int.MaxValue))
                {
                    var o = new RavenJObject
                    {
                        {"Key", listItem.Key}
                    };
                    jsonWriter.Write(o);

                    lastEtag = listItem.Etag;
                }
            });
            return new CompletedTask<Etag>(lastEtag);
        }

        public Task<RavenJArray> GetTransformers(int start)
        {
            return new CompletedTask<RavenJArray>(database.Transformers.GetTransformers(start, Options.BatchSize));
        }

        public Task<Etag> ExportDocumentsDeletion(SmugglerJsonTextWriter jsonWriter, Etag startDocsEtag, Etag maxEtag)
        {
            var lastEtag = startDocsEtag;
            database.TransactionalStorage.Batch(accessor =>
            {
                foreach (var listItem in accessor.Lists.Read(Constants.RavenPeriodicExportsDocsTombstones, startDocsEtag, maxEtag, int.MaxValue))
                {
                    var ravenJObj = new RavenJObject
                    {
                        {"Key", listItem.Key}
                    };
                    jsonWriter.Write(ravenJObj);

                    lastEtag = listItem.Etag;
                }
            });
            return new CompletedTask<Etag>(lastEtag);
        }

        public LastEtagsInfo FetchCurrentMaxEtags()
        {
            LastEtagsInfo result = null;

            database.TransactionalStorage.Batch(accessor =>
            {
                result = new LastEtagsInfo
                {
                    LastDocsEtag = accessor.Staleness.GetMostRecentDocumentEtag(),
                    LastAttachmentsEtag = accessor.Staleness.GetMostRecentAttachmentEtag()
                };

                var lastDocumentTombstone = accessor.Lists.ReadLast(Constants.RavenPeriodicExportsDocsTombstones);
                if (lastDocumentTombstone != null)
                    result.LastDocDeleteEtag = lastDocumentTombstone.Etag;

                var attachmentTombstones =
                    accessor.Lists.Read(Constants.RavenPeriodicExportsAttachmentsTombstones, etagEmpty, null, int.MaxValue)
                            .OrderBy(x => x.Etag).ToArray();
                if (attachmentTombstones.Any())
                {
                    result.LastAttachmentsDeleteEtag = attachmentTombstones.Last().Etag;
                }
            });

            return result;
        }

        public Task PutIndex(string indexName, RavenJToken index)
        {
            if (index != null)
            {
                database.Indexes.PutIndex(indexName, index.Value<RavenJObject>("definition").JsonDeserialization<IndexDefinition>());
            }

            return new CompletedTask();
        }

        [Obsolete("Use RavenFS instead.")]
        public Task PutAttachment(AttachmentExportInfo attachmentExportInfo)
        {
            if (attachmentExportInfo != null)
            {
                // we filter out content length, because getting it wrong will cause errors 
                // in the server side when serving the wrong value for this header.
                // worse, if we are using http compression, this value is known to be wrong
                // instead, we rely on the actual size of the data provided for us
                attachmentExportInfo.Metadata.Remove("Content-Length");
                database.Attachments.PutStatic(attachmentExportInfo.Key, null, attachmentExportInfo.Data,
                                    attachmentExportInfo.Metadata);
            }

            return new CompletedTask();
        }

        private long totalSize;
        private Stopwatch sp;
        
        public Task PutDocument(RavenJObject document, int size)
        {
            if (document != null)
            {
                totalSize += size;
                if (sp == null)
                    sp = Stopwatch.StartNew();

                var metadata = document.Value<RavenJObject>("@metadata");
                var key = metadata.Value<string>("@id");
                document.Remove("@metadata");

                bulkInsertBatch.Add(new JsonDocument
                {
                    Key = key,
                    Metadata = metadata,
                    DataAsJson = document,
                });

                if (Options.BatchSize > bulkInsertBatch.Count &&
                    totalSize <= maxSize &&
                    sp.Elapsed <= timeout)
                    return new CompletedTask();
            }

            var stopWatchExists = sp != null;
            if (stopWatchExists)
                sp.Stop();

            var batchToSave = new List<IEnumerable<JsonDocument>> { bulkInsertBatch };
            bulkInsertBatch = new List<JsonDocument>();
            database.Documents.BulkInsert(new BulkInsertOptions { BatchSize = Options.BatchSize, OverwriteExisting = true }, batchToSave, Guid.NewGuid(), CancellationToken.None);
            totalSize = 0;

            if (stopWatchExists)
                sp.Restart();

            return new CompletedTask();
        }

        public Task PutTransformer(string transformerName, RavenJToken transformer)
        {
            if (transformer != null)
            {
                var transformerDefinition =
                    JsonConvert.DeserializeObject<TransformerDefinition>(transformer.Value<RavenJObject>("definition").ToString());
                database.Transformers.PutTransform(transformerName, transformerDefinition);
            }

            return new CompletedTask();
        }

        public Task DeleteDocument(string key)
        {
            if (key != null)
            {
                database.Documents.Delete(key, null, null);
            }
            return new CompletedTask();
        }

        public SmugglerDatabaseOptions Options { get; private set; }

        [Obsolete("Use RavenFS instead.")]
        public Task DeleteAttachment(string key)
        {
            database.Attachments.DeleteStatic(key, null);
            return new CompletedTask();
        }

        public void PurgeTombstones(OperationState result)
        {
            database.TransactionalStorage.Batch(accessor =>
            {
                // since remove all before is inclusive, but we want last etag for function FetchCurrentMaxEtags we modify ranges
                accessor.Lists.RemoveAllBefore(Constants.RavenPeriodicExportsDocsTombstones, result.LastDocDeleteEtag.IncrementBy(-1));
                accessor.Lists.RemoveAllBefore(Constants.RavenPeriodicExportsAttachmentsTombstones, result.LastAttachmentsDeleteEtag.IncrementBy(-1));
            });
        }

        public Task<BuildNumber> GetVersion(RavenConnectionStringOptions server)
        {
            return new CompletedTask<BuildNumber>(new BuildNumber { BuildVersion = DocumentDatabase.BuildVersion.ToString(), ProductVersion = DocumentDatabase.ProductVersion });
        }

        public Task<DatabaseStatistics> GetStats()
        {
            return new CompletedTask<DatabaseStatistics>(database.Statistics);
        }

        public Task<RavenJObject> TransformDocument(RavenJObject document, string transformScript)
        {
            return new CompletedTask<RavenJObject>(scriptedJsonPatcher.Transform(transformScript, document));
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

        public void Initialize(SmugglerDatabaseOptions databaseOptions)
        {
            Options = databaseOptions;
            scriptedJsonPatcher.Initialize(databaseOptions);
        }

        public void Configure(SmugglerDatabaseOptions databaseOptions)
        {
            var current = databaseOptions.BatchSize;
            var maxNumberOfItemsToProcessInSingleBatch = database.Configuration.MaxNumberOfItemsToProcessInSingleBatch;

            databaseOptions.BatchSize = Math.Min(current, maxNumberOfItemsToProcessInSingleBatch);
        }

        public Task<List<KeyValuePair<string, long>>> GetIdentities()
        {
            var start = 0;
            const int pageSize = 1024;

            long totalCount = 0;
            var identities = new List<KeyValuePair<string, long>>();

            do
            {
                database.TransactionalStorage.Batch(accessor => identities.AddRange(accessor.General.GetIdentities(start, pageSize, out totalCount)));
                start += pageSize;
            } while (identities.Count < totalCount);

            return new CompletedTask<List<KeyValuePair<string, long>>>(identities);
        }

        public Task SeedIdentityFor(string identityName, long identityValue)
        {
            if (identityName != null)
            {
                using (database.IdentityLock.Lock())
                {
                    database.TransactionalStorage.Batch(accessor => accessor.General.SetIdentityValue(identityName, identityValue));
                }
            }

            return new CompletedTask();
        }

        public Task SeedIdentities(List<KeyValuePair<string, long>> itemsToInsert)
        {
            using (database.IdentityLock.Lock())
            {
                database.TransactionalStorage.Batch(accessor =>
                {
                    foreach (var identityPair in itemsToInsert)
                    {
                        if (identityPair.Key != null)
                        {
                            accessor.General.SetIdentityValue(identityPair.Key, identityPair.Value);
                        }
                    }
                });
            }

            return new CompletedTask();
        }

        public async Task WaitForLastBulkInsertTaskToFinish()
        {
            //noop
        }

        public Task<IAsyncEnumerator<RavenJObject>> ExportItems(ItemType types, OperationState state)
        {
            var exporter = new SmugglerExporter(database, ExportOptions.Create(state, types, Options.ExportDeletions, Options.Limit));

            var items = new List<RavenJObject>();

            exporter.Export(items.Add, database.WorkContext.CancellationToken);

            return new CompletedTask<IAsyncEnumerator<RavenJObject>>(new AsyncEnumeratorBridge<RavenJObject>(items.GetEnumerator()));
        }

        public RavenJToken DisableVersioning(RavenJObject metadata)
        {
            if (metadata != null)
            {
                metadata[Constants.RavenIgnoreVersioning] = true;
            }

            return metadata;
        }

        public void ShowProgress(string format, params object[] args)
        {
            if (Progress != null)
            {
                Progress(string.Format(format, args));
            }
        }

        [Obsolete("Use RavenFS instead.")]
        public Task<List<AttachmentInformation>> GetAttachments(int start, Etag etag, int maxRecords)
        {
            var attachments = database
                .Attachments
                .GetAttachments(start, maxRecords, etag, null, 1024 * 1024 * 10)
                .ToList();

            return new CompletedTask<List<AttachmentInformation>>(attachments);
        }

        [Obsolete("Use RavenFS instead.")]
        public Task<byte[]> GetAttachmentData(AttachmentInformation attachmentInformation)
        {
            var attachment = database.Attachments.GetStatic(attachmentInformation.Key);
            if (attachment == null)
                return null;

            var data = attachment.Data;
            attachment.Data = () =>
            {
                var memoryStream = new MemoryStream();
                database.TransactionalStorage.Batch(accessor => data().CopyTo(memoryStream));
                memoryStream.Position = 0;
                return memoryStream;
            };

            return new CompletedTask<byte[]>(attachment.Data().ReadData());
        }

        public string GetIdentifier()
        {
            return string.Format("embedded: {0}/{1}", database.Name ?? Constants.SystemDatabase, database.TransactionalStorage.Id);
    }
}
}
