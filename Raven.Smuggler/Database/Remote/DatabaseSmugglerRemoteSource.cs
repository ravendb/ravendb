// -----------------------------------------------------------------------
//  <copyright file="DatabaseSmugglerRemoteSource.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Data;
using Raven.Abstractions.Database.Smuggler.Database;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Util;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Json.Linq;
using Raven.Smuggler.Helpers;

namespace Raven.Smuggler.Database.Remote
{
    public class DatabaseSmugglerRemoteSource : DatabaseSmugglerRemoteBase, IDatabaseSmugglerSource
    {
        private DocumentStore _store;

        private DatabaseSmugglerOptions _options;

        private readonly List<SmuggleType> _types = new List<SmuggleType>
        {
            SmuggleType.Index,
            SmuggleType.Document,
            SmuggleType.Transformer,
            SmuggleType.Identity,
            SmuggleType.None
        };

        private int _typeIndex;

        private readonly bool _ownsStore;

        private readonly Func<DocumentStore> _storeFactory;

        public DatabaseSmugglerRemoteSource(DatabaseSmugglerRemoteConnectionOptions connectionOptions)
        {
            _storeFactory = () =>
            {
                var store = new DocumentStore
                {
                    ApiKey = connectionOptions.ApiKey,
                    DefaultDatabase = connectionOptions.Database,
                    Url = connectionOptions.Url,
                    Credentials = connectionOptions.Credentials
                };

                if (string.IsNullOrWhiteSpace(connectionOptions.ConnectionStringName) == false)
                    store.ConnectionStringName = connectionOptions.ConnectionStringName;

                return store;
            };
            _ownsStore = true;
        }

        public DatabaseSmugglerRemoteSource(DocumentStore store)
        {
            _storeFactory = () => store;
            _ownsStore = false;
        }

        public void Dispose()
        {
            if (_ownsStore)
                _store?.Dispose();
        }

        public string DisplayName => _store.Url;

        public bool SupportsMultipleSources => false;

        public IReadOnlyList<IDatabaseSmugglerSource> Sources => null;

        public async Task InitializeAsync(DatabaseSmugglerOptions options, CancellationToken cancellationToken)
        {
            _typeIndex = 0;
            _options = options;
            _store = _storeFactory();

            if (_ownsStore)
                _store.Initialize(ensureDatabaseExists: false);

            await ServerValidation.ValidateThatServerIsUpAndDatabaseExistsAsync(_store, cancellationToken).ConfigureAwait(false);

            await InitializeBatchSizeAsync(_store, _options).ConfigureAwait(false);
        }

        public async Task<List<IndexDefinition>> ReadIndexesAsync(int start, int pageSize, CancellationToken cancellationToken)
        {
            var indexes = await _store
                .AsyncDatabaseCommands
                .GetIndexesAsync(start, pageSize, cancellationToken)
                .ConfigureAwait(false);

            return indexes.ToList();
        }

        public Task<DatabaseLastEtagsInfo> FetchCurrentMaxEtagsAsync(CancellationToken cancellationToken)
        {
            return new CompletedTask<DatabaseLastEtagsInfo>(new DatabaseLastEtagsInfo
            {
                LastDocDeleteEtag = null,
                LastDocsEtag = null
            });
        }

        public async Task<IAsyncEnumerator<RavenJObject>> ReadDocumentsAfterAsync(Etag afterEtag, int pageSize, CancellationToken cancellationToken)
        {
            return await _store
                .AsyncDatabaseCommands
                .StreamDocsAsync(afterEtag, pageSize: pageSize, token: cancellationToken)
                .ConfigureAwait(false);
        }

        public async Task<RavenJObject> ReadDocumentAsync(string key, CancellationToken cancellationToken)
        {
            var document = await _store
                .AsyncDatabaseCommands
                .GetAsync(key, cancellationToken)
                .ConfigureAwait(false);

            if (document == null)
                return null;

            JsonDocument.EnsureIdInMetadata(document);
            return document.ToJson();
        }

        public bool SupportsReadingHiLoDocuments => true;

        public bool SupportsDocumentDeletions => false;

        public bool SupportsPaging => true;

        public bool SupportsRetries => true;

        public async Task<List<TransformerDefinition>> ReadTransformersAsync(int start, int batchSize, CancellationToken cancellationToken)
        {
            var transformers = await _store
                .AsyncDatabaseCommands
                .GetTransformersAsync(start, batchSize, cancellationToken)
                .ConfigureAwait(false);

            return transformers.ToList();
        }

        public Task<List<KeyValuePair<string, Etag>>> ReadDocumentDeletionsAsync(Etag fromEtag, Etag maxEtag, CancellationToken cancellationToken)
        {
            throw new NotSupportedException();
        }

        public async Task<List<KeyValuePair<string, long>>> ReadIdentitiesAsync(CancellationToken cancellationToken)
        {
            var start = 0;
            const int PageSize = 1024;
            long totalIdentitiesCount;
            var identities = new List<KeyValuePair<string, long>>();

            do
            {
                var url = _store.Url.ForDatabase(_store.DefaultDatabase) + "/debug/identities?start=" + start + "&pageSize=" + PageSize;
                using (var request = _store.JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null, url, HttpMethods.Get, _store.DatabaseCommands.PrimaryCredentials, _store.Conventions)))
                {
                    var identitiesInfo = (RavenJObject)await request.ReadResponseJsonAsync().ConfigureAwait(false);
                    totalIdentitiesCount = identitiesInfo.Value<long>("TotalCount");

                    foreach (var identity in identitiesInfo.Value<RavenJArray>("Identities"))
                        identities.Add(new KeyValuePair<string, long>(identity.Value<string>("Key"), identity.Value<long>("Value")));

                    start += PageSize;
                }
            } while (identities.Count < totalIdentitiesCount);

            return identities;
        }

        public Task<SmuggleType> GetNextSmuggleTypeAsync(CancellationToken cancellationToken)
        {
            return new CompletedTask<SmuggleType>(_types[_typeIndex++]);
        }

        public Task SkipDocumentsAsync(CancellationToken cancellationToken)
        {
            return new CompletedTask();
        }

        public Task SkipIndexesAsync(CancellationToken cancellationToken)
        {
            return new CompletedTask();
        }

        public Task SkipTransformersAsync(CancellationToken cancellationToken)
        {
            return new CompletedTask();
        }

        public Task SkipDocumentDeletionsAsync(CancellationToken cancellationToken)
        {
            return new CompletedTask();
        }

        public Task SkipIdentitiesAsync(CancellationToken cancellationToken)
        {
            return new CompletedTask();
        }

        public Task SkipAttachmentsAsync(CancellationToken cancellationToken)
        {
            return new CompletedTask();
        }

        public Task SkipAttachmentDeletionsAsync(CancellationToken cancellationToken)
        {
            return new CompletedTask();
        }

        public Task AfterExecuteAsync(DatabaseSmugglerOperationState state)
        {
            return new CompletedTask();
        }

        public void OnException(SmugglerException exception)
        {
        }
    }
}
