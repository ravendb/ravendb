//-----------------------------------------------------------------------
// <copyright file="ServerClient.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

using Raven.NewClient.Abstractions.Cluster;
using Raven.NewClient.Abstractions.Connection;
using Raven.NewClient.Abstractions.Replication;
using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Abstractions.Extensions;
using Raven.NewClient.Abstractions.Indexing;
using Raven.NewClient.Abstractions.Util;
using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Connection.Async;
using Raven.NewClient.Client.Connection.Implementation;
using Raven.NewClient.Client.Connection.Profiling;
using Raven.NewClient.Client.Connection.Request;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Data.Indexes;
using Raven.NewClient.Client.Data.Queries;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Document.Commands;
using Raven.NewClient.Client.Exceptions;
using Raven.NewClient.Client.Indexes;
using Raven.NewClient.Client.Indexing;
using Raven.NewClient.Json.Linq;

namespace Raven.NewClient.Client.Connection
{
    public class ServerClient : IDatabaseCommands, IInfoDatabaseCommands
    {
        private readonly AsyncServerClient asyncServerClient;

        public event EventHandler<FailoverStatusChangedEventArgs> FailoverStatusChanged
        {
            add { asyncServerClient.RequestExecuter.FailoverStatusChanged += value; }
            remove { asyncServerClient.RequestExecuter.FailoverStatusChanged -= value; }
        }

        public ServerClient(AsyncServerClient asyncServerClient)
        {
            this.asyncServerClient = asyncServerClient;
        }

        public IInfoDatabaseCommands Info => this;

        public OperationCredentials PrimaryCredentials => asyncServerClient.PrimaryCredentials;

        public DocumentConvention Convention => asyncServerClient.convention;

        public IRequestExecuter RequestExecuter => asyncServerClient.RequestExecuter;

        #region IDatabaseCommands Members

        public NameValueCollection OperationsHeaders
        {
            get { return asyncServerClient.OperationsHeaders; }
            set { asyncServerClient.OperationsHeaders = value; }
        }

        public JsonDocument Get(string key, bool metadataOnly = false)
        {
            return AsyncHelpers.RunSync(() => asyncServerClient.GetAsync(key, metadataOnly));
        }

        public IGlobalAdminDatabaseCommands GlobalAdmin =>
            new AdminServerClient(asyncServerClient, new AsyncAdminServerClient(asyncServerClient));

        public JsonDocument[] StartsWith(string keyPrefix, string matches, int start, int pageSize,
                                         RavenPagingInformation pagingInformation = null, bool metadataOnly = false,
                                         string exclude = null, string transformer = null,
                                         Dictionary<string, RavenJToken> transformerParameters = null,
                                         string skipAfter = null)
        {
            return
                AsyncHelpers.RunSync(() => asyncServerClient.StartsWithAsync(keyPrefix, matches, start, pageSize, pagingInformation, metadataOnly, exclude,
                                                  transformer, transformerParameters, skipAfter)
                                 );
        }

        public JsonDocument[] GetRevisionsFor(string key, int start, int pageSize)
        {
            return AsyncHelpers.RunSync(() => asyncServerClient.GetRevisionsForAsync(key, start, pageSize));
        }

        public RavenJToken ExecuteGetRequest(string requestUrl)
        {
            return AsyncHelpers.RunSync(() => asyncServerClient.ExecuteGetRequest(requestUrl));
        }

        internal T ExecuteWithReplication<T>(HttpMethod method, Func<OperationMetadata, T> operation)
        {
            return
                AsyncHelpers.RunSync(() => asyncServerClient.ExecuteWithReplication(method,
                    operationMetadata => Task.FromResult(operation(operationMetadata))));
        }

        public JsonDocument[] GetDocuments(int start, int pageSize, bool metadataOnly = false)
        {
            return AsyncHelpers.RunSync(() => asyncServerClient.GetDocumentsAsync(start, pageSize, metadataOnly));
        }

        public JsonDocument[] GetDocuments(long? fromEtag, int pageSize, bool metadataOnly = false)
        {
            return AsyncHelpers.RunSync(() => asyncServerClient.GetDocumentsAsync(fromEtag, pageSize, metadataOnly));
        }

        public PutResult Put(string key, long? etag, RavenJObject document, RavenJObject metadata)
        {
            return AsyncHelpers.RunSync(() => asyncServerClient.PutAsync(key, etag, document, metadata));
        }

        public void Delete(string key, long? etag)
        {
            AsyncHelpers.RunSync(() => asyncServerClient.DeleteAsync(key, etag));
        }

        public string[] GetDatabaseNames(int pageSize, int start = 0)
        {
            return AsyncHelpers.RunSync(() => asyncServerClient.GlobalAdmin.GetDatabaseNamesAsync(pageSize, start));
        }

        public string[] GetIndexNames(int start, int pageSize)
        {
            return AsyncHelpers.RunSync(() => asyncServerClient.GetIndexNamesAsync(start, pageSize));
        }

        public IndexDefinition[] GetIndexes(int start, int pageSize)
        {
            return AsyncHelpers.RunSync(() => asyncServerClient.GetIndexesAsync(start, pageSize));
        }

        public TransformerDefinition[] GetTransformers(int start, int pageSize)
        {
            return AsyncHelpers.RunSync(() => asyncServerClient.GetTransformersAsync(start, pageSize));
        }

        public TransformerDefinition GetTransformer(string name)
        {
            return AsyncHelpers.RunSync(() => asyncServerClient.GetTransformerAsync(name));
        }

        public void DeleteTransformer(string name)
        {
            AsyncHelpers.RunSync(() => asyncServerClient.DeleteTransformerAsync(name));
        }

        public void SetTransformerLock(string name, TransformerLockMode lockMode)
        {
            AsyncHelpers.RunSync(() => asyncServerClient.SetTransformerLockAsync(name, lockMode));
        }

        public void ResetIndex(string name)
        {
            AsyncHelpers.RunSync(() => asyncServerClient.ResetIndexAsync(name));
        }
        public void SetIndexLock(string name, IndexLockMode mode)
        {
            AsyncHelpers.RunSync(() => asyncServerClient.SetIndexLockAsync(name, mode));
        }
        public void SetIndexPriority(string name, IndexingPriority priority)
        {
            AsyncHelpers.RunSync(() => asyncServerClient.SetIndexPriorityAsync(name, priority));
        }

        public IndexDefinition GetIndex(string name)
        {
            return AsyncHelpers.RunSync(() => asyncServerClient.GetIndexAsync(name));
        }

        public IndexPerformanceStats[] GetIndexPerformanceStatistics(string[] indexNames = null)
        {
            return AsyncHelpers.RunSync(() => asyncServerClient.GetIndexPerformanceStatisticsAsync(indexNames));
        }

        internal void ReplicateIndex(string name)
        {
            AsyncHelpers.RunSync(() => asyncServerClient.ReplicateIndexAsync(name));
        }

        public string PutIndex(string name, IndexDefinition definition)
        {
            return AsyncHelpers.RunSync(() => asyncServerClient.PutIndexAsync(name, definition, false));
        }

        public string[] PutIndexes(IndexToAdd[] indexesToAdd)
        {
            return AsyncHelpers.RunSync(() => asyncServerClient.PutIndexesAsync(indexesToAdd));
        }

        public string[] PutSideBySideIndexes(IndexToAdd[] indexesToAdd, long? minimumEtagBeforeReplace = null, DateTime? replaceTimeUtc = null)
        {
            return AsyncHelpers.RunSync(() => asyncServerClient.PutSideBySideIndexesAsync(indexesToAdd, minimumEtagBeforeReplace, replaceTimeUtc));
        }

        public bool IndexHasChanged(string name, IndexDefinition indexDef)
        {
            return AsyncHelpers.RunSync(() => asyncServerClient.IndexHasChangedAsync(name, indexDef));
        }

        public string PutTransformer(string name, TransformerDefinition transformerDef)
        {
            return AsyncHelpers.RunSync(() => asyncServerClient.PutTransformerAsync(name, transformerDef));
        }

        public string PutIndex(string name, IndexDefinition definition, bool overwrite)
        {
            return AsyncHelpers.RunSync(() => asyncServerClient.PutIndexAsync(name, definition, overwrite));
        }

        public string PutIndex<TDocument, TReduceResult>(string name,
            IndexDefinitionBuilder<TDocument, TReduceResult> indexDef)
        {
            return AsyncHelpers.RunSync(() => asyncServerClient.PutIndexAsync(name, indexDef, default(CancellationToken)));
        }

        public string PutIndex<TDocument, TReduceResult>(string name,
            IndexDefinitionBuilder<TDocument, TReduceResult> indexDef, bool overwrite)
        {
            return AsyncHelpers.RunSync(() => asyncServerClient.PutIndexAsync(name, indexDef, overwrite));
        }

        public string DirectPutIndex(string name, OperationMetadata operationMetadata, bool overwrite,
            IndexDefinition definition)
        {
            return asyncServerClient.DirectPutIndexAsync(name, definition, overwrite, operationMetadata).Result;
        }

        public QueryResult Query(string index, IndexQuery query, bool metadataOnly = false,
            bool indexEntriesOnly = false)
        {
            try
            {
                return AsyncHelpers.RunSync(() => asyncServerClient.QueryAsync(index, query, metadataOnly, indexEntriesOnly));
            }
            catch (Exception e)
            {
                if (e is ConflictException)
                    throw;

                throw new InvalidOperationException("Query failed. See inner exception for details.", e);
            }
        }

        public IEnumerator<RavenJObject> StreamQuery(string index, IndexQuery query,
            out QueryHeaderInformation queryHeaderInfo)
        {
            var reference = new Reference<QueryHeaderInformation>();
            var streamQueryAsync = AsyncHelpers.RunSync(() => asyncServerClient.StreamQueryAsync(index, query, reference));
            queryHeaderInfo = reference.Value;
            return new AsyncEnumerableWrapper<RavenJObject>(streamQueryAsync);
        }

        public IEnumerator<RavenJObject> StreamDocs(long? fromEtag = null, string startsWith = null, string matches = null, int start = 0, int pageSize = int.MaxValue, string exclude = null, RavenPagingInformation pagingInformation = null, string skipAfter = null, string transformer = null, Dictionary<string, RavenJToken> transformerParameters = null)
        {
            var streamDocsAsync = AsyncHelpers.RunSync(() => asyncServerClient.StreamDocsAsync(fromEtag, startsWith, matches, start, pageSize, exclude, pagingInformation, skipAfter, transformer, transformerParameters));
            return new AsyncEnumerableWrapper<RavenJObject>(streamDocsAsync);
        }

        public void DeleteIndex(string name)
        {
            AsyncHelpers.RunSync(() => asyncServerClient.DeleteIndexAsync(name));
        }

        public LoadResult Get(string[] ids, string[] includes, string transformer = null,
            Dictionary<string, RavenJToken> transformerParameters = null, bool metadataOnly = false)
        {
            return AsyncHelpers.RunSync(() => asyncServerClient.GetAsync(ids, includes, transformer, transformerParameters, metadataOnly));
        }

        public BatchResult[] Batch(IEnumerable<ICommandData> commandDatas)
        {
            return AsyncHelpers.RunSync(() => asyncServerClient.BatchAsync(commandDatas.ToArray()));
        }

        public BuildNumber GetBuildNumber()
        {
            return AsyncHelpers.RunSync(() => asyncServerClient.GetBuildNumberAsync());
        }

        public IndexMergeResults GetIndexMergeSuggestions()
        {
            return AsyncHelpers.RunSync(() => asyncServerClient.GetIndexMergeSuggestionsAsync());
        }

        public LogItem[] GetLogs(bool errorsOnly)
        {
            return AsyncHelpers.RunSync(() => asyncServerClient.GetLogsAsync(errorsOnly));
        }

        public LicensingStatus GetLicenseStatus()
        {
            return AsyncHelpers.RunSync(() => asyncServerClient.GetLicenseStatusAsync());
        }

        public TcpBulkInsertOperation GetBulkInsertOperation()
        {

            return asyncServerClient.GetBulkInsertOperation();
        }

        public HttpJsonRequest CreateReplicationAwareRequest(string currentServerUrl, string requestUrl, HttpMethod method, bool disableRequestCompression = false, bool disableAuthentication = false, TimeSpan? timeout = null)
        {
            return asyncServerClient.CreateReplicationAwareRequest(currentServerUrl, requestUrl, method, disableRequestCompression, disableAuthentication, timeout);
        }

        public IDatabaseCommands With(ICredentials credentialsForSession)
        {
            return new ServerClient(asyncServerClient.WithInternal(credentialsForSession));
        }

        public IDisposable ForceReadFromMaster()
        {
            return asyncServerClient.ForceReadFromMaster();
        }

        public IDatabaseCommands ForDatabase(string database, ClusterBehavior? clusterBehavior = null)
        {
            var newAsyncServerClient = asyncServerClient.ForDatabaseInternal(database, clusterBehavior);
            if (asyncServerClient == newAsyncServerClient)
                return this;

            return new ServerClient(newAsyncServerClient);
        }

        public IDatabaseCommands ForSystemDatabase()
        {
            return new ServerClient(asyncServerClient.ForSystemDatabaseInternal());
        }

        public string Url
        {
            get { return asyncServerClient.Url; }
        }

        public Operation DeleteByIndex(string indexName, IndexQuery queryToDelete, QueryOperationOptions options = null)
        {
            return AsyncHelpers.RunSync(() => asyncServerClient.DeleteByIndexAsync(indexName, queryToDelete, options));
        }

        public Operation UpdateByIndex(string indexName, IndexQuery queryToUpdate, PatchRequest patch,
            QueryOperationOptions options = null)
        {
            return AsyncHelpers.RunSync(() => asyncServerClient.UpdateByIndexAsync(indexName, queryToUpdate, patch, options));
        }

        public SuggestionQueryResult Suggest(string index, SuggestionQuery suggestionQuery)
        {
            return AsyncHelpers.RunSync(() => asyncServerClient.SuggestAsync(index, suggestionQuery));
        }

        public QueryResult MoreLikeThis(MoreLikeThisQuery query)
        {
            return AsyncHelpers.RunSync(() => asyncServerClient.MoreLikeThisAsync(query));
        }

        public DatabaseStatistics GetStatistics()
        {
            return AsyncHelpers.RunSync(() => asyncServerClient.GetStatisticsAsync());
        }

        public IndexErrors GetIndexErrors(string name)
        {
            return AsyncHelpers.RunSync(() => asyncServerClient.GetIndexErrorsAsync(name));
        }

        public IndexErrors[] GetIndexErrors(IEnumerable<string> indexNames)
        {
            return AsyncHelpers.RunSync(() => asyncServerClient.GetIndexErrorsAsync(indexNames));
        }

        public IndexErrors[] GetIndexErrors()
        {
            return AsyncHelpers.RunSync(() => asyncServerClient.GetIndexErrorsAsync());
        }

        public IndexStats GetIndexStatistics(string name)
        {
            return AsyncHelpers.RunSync(() => asyncServerClient.GetIndexStatisticsAsync(name));
        }

        public UserInfo GetUserInfo()
        {
            return AsyncHelpers.RunSync(() => asyncServerClient.GetUserInfoAsync());
        }

        public UserPermission GetUserPermission(string database, bool readOnly)
        {
            return AsyncHelpers.RunSync(() => asyncServerClient.GetUserPermissionAsync(database, readOnly));
        }

        public long NextIdentityFor(string name)
        {
            return AsyncHelpers.RunSync(() => asyncServerClient.NextIdentityForAsync(name));
        }

        public long SeedIdentityFor(string name, long value)
        {
            return AsyncHelpers.RunSync(() => asyncServerClient.SeedIdentityForAsync(name, value));
        }

        public string UrlFor(string documentKey)
        {
            return asyncServerClient.UrlFor(documentKey);
        }

        public long? Head(string key)
        {
            return AsyncHelpers.RunSync(() => asyncServerClient.HeadAsync(key));
        }

        public GetResponse[] MultiGet(GetRequest[] requests)
        {
            return AsyncHelpers.RunSync(() => asyncServerClient.MultiGetAsync(requests));
        }

        public IEnumerable<string> GetTerms(string index, string field, string fromValue, int pageSize)
        {
            return AsyncHelpers.RunSync(() => asyncServerClient.GetTermsAsync(index, field, fromValue, pageSize));
        }

        public FacetedQueryResult GetFacets(FacetQuery query)
        {
            return AsyncHelpers.RunSync(() => asyncServerClient.GetFacetsAsync(query));
        }

        public FacetedQueryResult[] GetMultiFacets(FacetQuery[] facetedQueries)
        {
            return AsyncHelpers.RunSync(() => asyncServerClient.GetMultiFacetsAsync(facetedQueries));
        }

        public RavenJObject Patch(string key, PatchRequest patch)
        {
            return AsyncHelpers.RunSync(() => asyncServerClient.PatchAsync(key, patch));
        }

        public RavenJObject Patch(string key, PatchRequest patch, bool ignoreMissing)
        {
            return AsyncHelpers.RunSync(() => asyncServerClient.PatchAsync(key, patch, ignoreMissing));
        }

        public RavenJObject Patch(string key, PatchRequest patch, long? etag)
        {
            return AsyncHelpers.RunSync(() => asyncServerClient.PatchAsync(key, patch, etag));
        }

        public RavenJObject Patch(string key, PatchRequest patchExisting, PatchRequest patchDefault)
        {
            return AsyncHelpers.RunSync(() => asyncServerClient.PatchAsync(key, patchExisting, patchDefault));
        }

        public HttpJsonRequest CreateRequest(string relativeUrl, HttpMethod method, bool disableRequestCompression = false, bool disableAuthentication = false, TimeSpan? timeout = null)
        {
            return asyncServerClient.CreateRequest(relativeUrl, method, disableRequestCompression, disableAuthentication, timeout);
        }

        public IDisposable DisableAllCaching()
        {
            return asyncServerClient.DisableAllCaching();
        }

        internal ReplicationDocumentWithClusterInformation DirectGetReplicationDestinations(OperationMetadata operationMetadata)
        {
            return AsyncHelpers.RunSync(() => asyncServerClient.DirectGetReplicationDestinationsAsync(operationMetadata));
        }

        #endregion

        public ProfilingInformation ProfilingInformation
        {
            get { return asyncServerClient.ProfilingInformation; }
        }

        public void Dispose()
        {
            GC.SuppressFinalize(this);
            asyncServerClient.Dispose();
        }

        ~ServerClient()
        {
            Dispose();
        }

        public ReplicationStatistics GetReplicationInfo()
        {
            return AsyncHelpers.RunSync(() => asyncServerClient.Info.GetReplicationInfoAsync());
        }

        public IAdminDatabaseCommands Admin
        {
            get { return new AdminServerClient(asyncServerClient, new AsyncAdminServerClient(asyncServerClient)); }
        }

        private class AsyncEnumerableWrapper<T> : IEnumerator<T>, IEnumerable<T>
        {
            private readonly IAsyncEnumerator<T> asyncEnumerator;

            public AsyncEnumerableWrapper(IAsyncEnumerator<T> asyncEnumerator)
            {
                this.asyncEnumerator = asyncEnumerator;
            }

            public void Dispose()
            {
                asyncEnumerator.Dispose();
            }

            public bool MoveNext()
            {
                return AsyncHelpers.RunSync(() => asyncEnumerator.MoveNextAsync());
            }

            public void Reset()
            {
                throw new NotSupportedException();
            }

            public T Current
            {
                get { return asyncEnumerator.Current; }
            }

            object IEnumerator.Current
            {
                get { return Current; }
            }

            public IEnumerator<T> GetEnumerator()
            {
                return this;
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }
        }

    }
}
