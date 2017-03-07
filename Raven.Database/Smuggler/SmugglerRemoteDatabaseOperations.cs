// -----------------------------------------------------------------------
//  <copyright file="SmugglerRemoteDatabaseOperations.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Smuggler;
using Raven.Abstractions.Smuggler.Data;
using Raven.Abstractions.Util;
using Raven.Client.Connection;
using Raven.Client.Connection.Async;
using Raven.Client.Document;
using Raven.Database.Data;
using Raven.Database.Smuggler;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Smuggler
{
    public class SmugglerRemoteDatabaseOperations : ISmugglerDatabaseOperations
    {
        private readonly Func<DocumentStore> store;

        private readonly Func<BulkInsertOperation> operation;

        private readonly Func<bool> isDocsStreamingSupported;

        private readonly Func<bool> isTransformersSupported;

        private readonly Func<bool> isIdentitiesSmugglingSupported;

        const int RetriesCount = 5;

        private DocumentStore Store
        {
            get { return store(); }
        }

        private BulkInsertOperation Operation
        {
            get { return operation(); }
        }

        private readonly SmugglerJintHelper jintHelper = new SmugglerJintHelper();

        public SmugglerDatabaseOptions Options { get; private set; }

        public bool LastRequestErrored { get; set; }

        public SmugglerRemoteDatabaseOperations(Func<DocumentStore> store, Func<BulkInsertOperation> operation, Func<bool> isDocsStreamingSupported, Func<bool> isTransformersSupported, Func<bool> isIdentitiesSmugglingSupported)
        {
            this.store = store;

            this.operation = operation;
            this.isDocsStreamingSupported = isDocsStreamingSupported;
            this.isTransformersSupported = isTransformersSupported;
            this.isIdentitiesSmugglingSupported = isIdentitiesSmugglingSupported;
        }

        [Obsolete("Use RavenFS instead.")]
        public Task DeleteAttachment(string key)
        {
            return Store.AsyncDatabaseCommands.DeleteAttachmentAsync(key, null);
        }

        public Task DeleteDocument(string key)
        {
            return Store.AsyncDatabaseCommands.DeleteAsync(key, null);
        }

        [Obsolete("Use RavenFS instead.")]
        public Task<Etag> ExportAttachmentsDeletion(SmugglerJsonTextWriter jsonWriter, Etag startAttachmentsDeletionEtag, Etag maxAttachmentEtag)
        {
            throw new NotSupportedException("Exporting deletions is not supported for Command Line Smuggler");
        }

        public Task<Etag> ExportDocumentsDeletion(SmugglerJsonTextWriter jsonWriter, Etag startDocsEtag, Etag maxEtag)
        {
            throw new NotSupportedException("Exporting deletions is not supported for Command Line Smuggler");
        }

        public LastEtagsInfo FetchCurrentMaxEtags()
        {
            return new LastEtagsInfo
            {
                LastAttachmentsDeleteEtag = null,
                LastDocDeleteEtag = null,
                LastAttachmentsEtag = null,
                LastDocsEtag = null
            };
        }

        [Obsolete("Use RavenFS instead.")]
        public async Task<List<AttachmentInformation>> GetAttachments(int start, Etag etag, int maxRecords)
        {
            var attachments = await Store.AsyncDatabaseCommands.GetAttachmentsAsync(start, etag, maxRecords).ConfigureAwait(false);

            return attachments.ToList();
        }

        [Obsolete("Use RavenFS instead.")]
        public async Task<byte[]> GetAttachmentData(AttachmentInformation attachmentInformation)
        {
            var attachment = await Store.AsyncDatabaseCommands.GetAttachmentAsync(attachmentInformation.Key).ConfigureAwait(false);
            if (attachment == null)
                return null;

            return attachment.Data().ReadData();
        }

        public JsonDocument GetDocument(string key)
        {
            return Store.DatabaseCommands.Get(key);
        }

        public async Task<IAsyncEnumerator<RavenJObject>> GetDocuments(Etag lastEtag, int take)
        {
            if (isDocsStreamingSupported())
            {
                ShowProgress("Streaming documents from {0}, batch size {1}", lastEtag, take);
                return await Store.AsyncDatabaseCommands.StreamDocsAsync(lastEtag, pageSize: take).ConfigureAwait(false);
            }

            int retries = RetriesCount;
            var originalRequestTimeout = Store.JsonRequestFactory.RequestTimeout;
            var timeout = Options.Timeout.Seconds;
            if (timeout < 30)
                timeout = 30;

            try
            {
                while (true)
                {
                    try
                    {
                        await ((AsyncServerClient)Store.AsyncDatabaseCommands).GetDocumentsAsync(lastEtag, Math.Min(Options.BatchSize, take)).ConfigureAwait(false);

                    }
                    catch (Exception e)
                    {
                        if (retries-- == 0)
                            throw;

                        Store.JsonRequestFactory.RequestTimeout = TimeSpan.FromSeconds(timeout *= 2);
                        LastRequestErrored = true;
                        ShowProgress("Error reading from database, remaining attempts {0}, will retry. Error: {1}", retries, e);
                    }
                }
            }
            finally
            {
                Store.JsonRequestFactory.RequestTimeout = originalRequestTimeout;
            }
        }

        public async Task<RavenJArray> GetIndexes(int totalCount)
        {
            var indexes = await Store.AsyncDatabaseCommands.GetIndexesAsync(totalCount, Options.BatchSize).ConfigureAwait(false);
            var result = new RavenJArray();

            foreach (var index in indexes)
            {
                result.Add(new RavenJObject
                           {
                               { "name", index.Name },
                               { "definition", RavenJObject.FromObject(index) }
                           });
            }

            return (RavenJArray)RavenJToken.FromObject(result);
        }

        public Task<DatabaseStatistics> GetStats()
        {
            return Store.AsyncDatabaseCommands.GetStatisticsAsync();
        }

        public async Task<RavenJArray> GetTransformers(int start)
        {
            if (isTransformersSupported() == false)
                return new RavenJArray();

            var transformers = await Store.AsyncDatabaseCommands.GetTransformersAsync(start, Options.BatchSize).ConfigureAwait(false);
            var result = new RavenJArray();

            foreach (var transformer in transformers)
            {
                result.Add(new RavenJObject
                           {
                               { "name", transformer.Name },
                               { "definition", RavenJObject.FromObject(transformer) }
                           });
            }

            return result;
        }

        public Task<BuildNumber> GetVersion(RavenConnectionStringOptions server)
        {
            return Store.AsyncDatabaseCommands.GlobalAdmin.GetBuildNumberAsync();
        }

        public void PurgeTombstones(OperationState result)
        {
            throw new NotImplementedException("Purge tombstones is not supported for Command Line Smuggler");
        }

        [Obsolete("Use RavenFS instead.")]
        public async Task PutAttachment(AttachmentExportInfo attachmentExportInfo)
        {
            if (attachmentExportInfo != null)
            {
                await Store.AsyncDatabaseCommands.PutAttachmentAsync(attachmentExportInfo.Key, null, attachmentExportInfo.Data, attachmentExportInfo.Metadata).ConfigureAwait(false);
            }
        }

        public Task PutDocument(RavenJObject document, int size)
        {
            if (document == null)
                return new CompletedTask();

            var metadata = document.Value<RavenJObject>("@metadata");
            var id = metadata.Value<string>("@id");
            if (String.IsNullOrWhiteSpace(id))
                throw new InvalidDataException("Error while importing document from the dump: \n\r Missing id in the document metadata. This shouldn't be happening, most likely the dump you are importing from is corrupt");

            document.Remove("@metadata");

            Operation.Store(document, metadata, id, size);

            return new CompletedTask();
        }

        public Task PutIndex(string indexName, RavenJToken index)
        {
            if (index != null)
            {
                var indexDefinition = JsonConvert.DeserializeObject<IndexDefinition>(index.Value<RavenJObject>("definition").ToString());

                return Store.AsyncDatabaseCommands.PutIndexAsync(indexName, indexDefinition, overwrite: true);
            }

            return new CompletedTask();
        }

        public async Task PutTransformer(string transformerName, RavenJToken transformer)
        {
            if (isTransformersSupported() == false)
                return;

            if (transformer != null)
            {
                var transformerDefinition = JsonConvert.DeserializeObject<TransformerDefinition>(transformer.Value<RavenJObject>("definition").ToString());
                await Store.AsyncDatabaseCommands.PutTransformerAsync(transformerName, transformerDefinition).ConfigureAwait(false);
            }
        }

        public virtual void ShowProgress(string format, params object[] args)
        {
            try
            {
                Console.WriteLine(format, args);
            }
            catch (FormatException e)
            {
                throw new FormatException("Input string is invalid: " + format + Environment.NewLine + string.Join(", ", args), e);
            }
        }

        public Task<RavenJObject> TransformDocument(RavenJObject document, string transformScript)
        {
            return new CompletedTask<RavenJObject>(jintHelper.Transform(transformScript, document));
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
            jintHelper.Initialize(databaseOptions);
        }

        public void Configure(SmugglerDatabaseOptions databaseOptions)
        {
            if (Store.HasJsonRequestFactory == false)
                return;

            var url = Store.Url.ForDatabase(Store.DefaultDatabase) + "/debug/config";
            try
            {
                using (var request = Store.JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null, url, HttpMethods.Get, Store.DatabaseCommands.PrimaryCredentials, Store.Conventions)))
                {
                    var configuration = (RavenJObject)request.ReadResponseJson();

                    var maxNumberOfItemsToProcessInSingleBatch = configuration.Value<int>("MaxNumberOfItemsToProcessInSingleBatch");
                    if (maxNumberOfItemsToProcessInSingleBatch <= 0)
                        return;

                    var current = databaseOptions.BatchSize;
                    databaseOptions.BatchSize = Math.Min(current, maxNumberOfItemsToProcessInSingleBatch);
                }
            }
            catch (ErrorResponseException e)
            {
                if (e.StatusCode == HttpStatusCode.Forbidden) // let it continue with the user defined batch size
                    return;

                throw;
            }
        }

        public async Task<List<KeyValuePair<string, long>>> GetIdentities()
        {
            if (isIdentitiesSmugglingSupported() == false)
                return new List<KeyValuePair<string, long>>();

            int start = 0;
            const int pageSize = 1024;
            long totalIdentitiesCount;
            var identities = new List<KeyValuePair<string, long>>();

            do
            {
                var url = Store.Url.ForDatabase(Store.DefaultDatabase) + "/debug/identities?start=" + start + "&pageSize=" + pageSize;
                using (var request = Store.JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null, url, HttpMethods.Get, Store.DatabaseCommands.PrimaryCredentials, Store.Conventions)))
                {
                    var identitiesInfo = (RavenJObject)await request.ReadResponseJsonAsync().ConfigureAwait(false);
                    totalIdentitiesCount = identitiesInfo.Value<long>("TotalCount");

                    foreach (var identity in identitiesInfo.Value<RavenJArray>("Identities"))
                    {
                        identities.Add(new KeyValuePair<string, long>(identity.Value<string>("Key"), identity.Value<long>("Value")));
                    }

                    start += pageSize;
                }
            } while (identities.Count < totalIdentitiesCount);

            return identities;
        }

        public Task SeedIdentityFor(string identityName, long identityValue)
        {
            if (isIdentitiesSmugglingSupported() == false)
                return new CompletedTask();

            if (identityName != null)
                return Store.AsyncDatabaseCommands.SeedIdentityForAsync(identityName, identityValue);

            return new CompletedTask();
        }

        public Task SeedIdentities(List<KeyValuePair<string, long>> itemsToInsert)
        {
            var client = (AsyncServerClient)Store.AsyncDatabaseCommands;
            return client.SeedIdentitiesAsync(itemsToInsert);
        }

        public async Task WaitForLastBulkInsertTaskToFinish()
        {
            await Operation.WaitForLastTaskToFinish().ConfigureAwait(false);
        }

        public Task<IAsyncEnumerator<RavenJObject>> ExportItems(ItemType types, OperationState state)
        {
            var options = ExportOptions.Create(state, types, Options.ExportDeletions, Options.Limit);

            var client = (AsyncServerClient)Store.AsyncDatabaseCommands;

            return client.StreamExportAsync(options);
        }

        public string GetIdentifier()
        {
            return ((AsyncServerClient)Store.AsyncDatabaseCommands).Url;
        }
    }
}