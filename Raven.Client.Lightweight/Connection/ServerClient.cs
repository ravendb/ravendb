//-----------------------------------------------------------------------
// <copyright file="ServerClient.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Connection;
using Raven.Abstractions.Replication;
using Raven.Client.Changes;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Util;
using Raven.Client.Connection.Async;
using Raven.Client.Connection.Profiling;
using Raven.Client.Document;
using Raven.Client.Exceptions;
using Raven.Client.Extensions;
using Raven.Client.Indexes;
using Raven.Database.Data;
using Raven.Json.Linq;

namespace Raven.Client.Connection
{
	public class ServerClient : IDatabaseCommands, IInfoDatabaseCommands
	{
		private readonly AsyncServerClient asyncServerClient;

		public event EventHandler<FailoverStatusChangedEventArgs> FailoverStatusChanged
		{
			add { asyncServerClient.ReplicationInformer.FailoverStatusChanged += value; }
			remove { asyncServerClient.ReplicationInformer.FailoverStatusChanged -= value; }
		}

		public ServerClient(AsyncServerClient asyncServerClient)
		{
			this.asyncServerClient = asyncServerClient;
		}

		public IInfoDatabaseCommands Info
		{
			get
			{
				return this;
			}
		}

		public OperationCredentials PrimaryCredentials
		{
			get { return asyncServerClient.PrimaryCredentials; }
		}

		public DocumentConvention Convention
		{
			get { return asyncServerClient.convention; }
		}

		public IDocumentStoreReplicationInformer ReplicationInformer
		{
			get { return asyncServerClient.ReplicationInformer; }
		}

		#region IDatabaseCommands Members

		public NameValueCollection OperationsHeaders
		{
			get { return asyncServerClient.OperationsHeaders; }
			set { asyncServerClient.OperationsHeaders = value; }
		}

		public JsonDocument Get(string key)
		{
			return asyncServerClient.GetAsync(key).ResultUnwrap();
		}

		public IGlobalAdminDatabaseCommands GlobalAdmin
		{
			get { return new AdminServerClient(asyncServerClient, new AsyncAdminServerClient(asyncServerClient)); }
		}

		public JsonDocument[] StartsWith(string keyPrefix, string matches, int start, int pageSize,
										 RavenPagingInformation pagingInformation = null, bool metadataOnly = false,
										 string exclude = null, string transformer = null,
										 Dictionary<string, RavenJToken> transformerParameters = null,
										 string skipAfter = null)
		{
			return
				asyncServerClient.StartsWithAsync(keyPrefix, matches, start, pageSize, pagingInformation, metadataOnly, exclude,
												  transformer, transformerParameters, skipAfter)
								 .ResultUnwrap();
		}

		public RavenJToken ExecuteGetRequest(string requestUrl)
		{
			return asyncServerClient.ExecuteGetRequest(requestUrl).ResultUnwrap();
		}

		internal T ExecuteWithReplication<T>(string method, Func<OperationMetadata, T> operation)
		{
			return
				asyncServerClient.ExecuteWithReplication(method,
					operationMetadata => Task.FromResult(operation(operationMetadata))).ResultUnwrap();
		}

		public JsonDocument[] GetDocuments(int start, int pageSize, bool metadataOnly = false)
		{
			return asyncServerClient.GetDocumentsAsync(start, pageSize, metadataOnly).ResultUnwrap();
		}

		public JsonDocument[] GetDocuments(Etag fromEtag, int pageSize, bool metadataOnly = false)
		{
			return asyncServerClient.GetDocumentsAsync(fromEtag, pageSize, metadataOnly).ResultUnwrap();
		}

		public PutResult Put(string key, Etag etag, RavenJObject document, RavenJObject metadata)
		{
			return asyncServerClient.PutAsync(key, etag, document, metadata).ResultUnwrap();
		}

		public void Delete(string key, Etag etag)
		{
			asyncServerClient.DeleteAsync(key, etag).WaitUnwrap();
		}

		[Obsolete("Use RavenFS instead.")]
		public void PutAttachment(string key, Etag etag, Stream data, RavenJObject metadata)
		{
			asyncServerClient.PutAttachmentAsync(key, etag, data, metadata).WaitUnwrap();
		}

		[Obsolete("Use RavenFS instead.")]
		public void UpdateAttachmentMetadata(string key, Etag etag, RavenJObject metadata)
		{
			asyncServerClient.UpdateAttachmentMetadataAsync(key, etag, metadata).WaitUnwrap();
		}

		[Obsolete("Use RavenFS instead.")]
		public IEnumerable<Attachment> GetAttachmentHeadersStartingWith(string idPrefix, int start, int pageSize)
		{
			return new AsyncEnumerableWrapper<Attachment>(asyncServerClient.GetAttachmentHeadersStartingWithAsync(idPrefix,
				start, pageSize).Result);

		}

		[Obsolete("Use RavenFS instead.")]
		public Attachment GetAttachment(string key)
		{
			return asyncServerClient.GetAttachmentAsync(key).ResultUnwrap();
		}

		[Obsolete("Use RavenFS instead.")]
		public Attachment HeadAttachment(string key)
		{
			return asyncServerClient.HeadAttachmentAsync(key).ResultUnwrap();
		}

		[Obsolete("Use RavenFS instead.")]
		public void DeleteAttachment(string key, Etag etag)
		{
			asyncServerClient.DeleteAttachmentAsync(key, etag).WaitUnwrap();
		}

		public string[] GetDatabaseNames(int pageSize, int start = 0)
		{
			return asyncServerClient.GlobalAdmin.GetDatabaseNamesAsync(pageSize, start).ResultUnwrap();
		}

		public string[] GetIndexNames(int start, int pageSize)
		{
			return asyncServerClient.GetIndexNamesAsync(start, pageSize).ResultUnwrap();
		}

		public IndexDefinition[] GetIndexes(int start, int pageSize)
		{
			return asyncServerClient.GetIndexesAsync(start, pageSize).ResultUnwrap();
		}

		public TransformerDefinition[] GetTransformers(int start, int pageSize)
		{
			return asyncServerClient.GetTransformersAsync(start, pageSize).ResultUnwrap();
		}

		public TransformerDefinition GetTransformer(string name)
		{
			return asyncServerClient.GetTransformerAsync(name).ResultUnwrap();
		}

		public void DeleteTransformer(string name)
		{
			asyncServerClient.DeleteTransformerAsync(name).WaitUnwrap();
		}

		public void ResetIndex(string name)
		{
			asyncServerClient.ResetIndexAsync(name).WaitUnwrap();
		}
        public void SetIndexLock(string name, IndexLockMode unLockMode) 
        {
            asyncServerClient.SetIndexLockAsync(name, unLockMode).WaitUnwrap();
        }
        public void SetIndexPriority(string name, IndexingPriority priority )
        {
            asyncServerClient.SetIndexPriorityAsync(name, priority).WaitUnwrap();
        }

		public IndexDefinition GetIndex(string name)
		{
			return asyncServerClient.GetIndexAsync(name).ResultUnwrap();
		}

		public string PutIndex(string name, IndexDefinition definition)
		{
			return asyncServerClient.PutIndexAsync(name, definition, false).ResultUnwrap();
		}

		public bool IndexHasChanged(string name, IndexDefinition indexDef)
		{
			return asyncServerClient.IndexHasChangedAsync(name, indexDef).ResultUnwrap();
		}

		public string PutTransformer(string name, TransformerDefinition transformerDef)
		{
			return asyncServerClient.PutTransformerAsync(name, transformerDef).ResultUnwrap();
		}

		public string PutIndex(string name, IndexDefinition definition, bool overwrite)
		{
			return asyncServerClient.PutIndexAsync(name, definition, overwrite).ResultUnwrap();
		}

		public string PutIndex<TDocument, TReduceResult>(string name,
			IndexDefinitionBuilder<TDocument, TReduceResult> indexDef)
		{
			return asyncServerClient.PutIndexAsync(name, indexDef,default(CancellationToken)).ResultUnwrap();
		}

		public string PutIndex<TDocument, TReduceResult>(string name,
			IndexDefinitionBuilder<TDocument, TReduceResult> indexDef, bool overwrite)
		{
			return asyncServerClient.PutIndexAsync(name, indexDef, overwrite).ResultUnwrap();
		}

		public string DirectPutIndex(string name, OperationMetadata operationMetadata, bool overwrite,
			IndexDefinition definition)
		{
			return asyncServerClient.DirectPutIndexAsync(name, definition, overwrite, operationMetadata).Result;
		}

		public QueryResult Query(string index, IndexQuery query, string[] includes = null, bool metadataOnly = false,
			bool indexEntriesOnly = false)
		{
			try
			{
				return asyncServerClient.QueryAsync(index, query, includes, metadataOnly, indexEntriesOnly).ResultUnwrap();
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
			var streamQueryAsync = asyncServerClient.StreamQueryAsync(index, query, reference).ResultUnwrap();
			queryHeaderInfo = reference.Value;
			return new AsyncEnumerableWrapper<RavenJObject>(streamQueryAsync);
		}

		public IEnumerator<RavenJObject> StreamDocs(Etag fromEtag = null, string startsWith = null, string matches = null, int start = 0, int pageSize = int.MaxValue, string exclude = null, RavenPagingInformation pagingInformation = null, string skipAfter = null)
		{
			return new AsyncEnumerableWrapper<RavenJObject>(
				asyncServerClient.StreamDocsAsync(fromEtag, startsWith, matches, start, pageSize, exclude, pagingInformation, skipAfter).Result);
		}

		public void DeleteIndex(string name)
		{
			asyncServerClient.DeleteIndexAsync(name).WaitUnwrap();
		}

		public MultiLoadResult Get(string[] ids, string[] includes, string transformer = null,
			Dictionary<string, RavenJToken> transformerParameters = null, bool metadataOnly = false)
		{
			return asyncServerClient.GetAsync(ids, includes, transformer, transformerParameters, metadataOnly).ResultUnwrap();
		}

		public BatchResult[] Batch(IEnumerable<ICommandData> commandDatas)
		{
			return asyncServerClient.BatchAsync(commandDatas.ToArray()).ResultUnwrap();
		}

		public void Commit(string txId)
		{
			asyncServerClient.CommitAsync(txId).WaitUnwrap();
		}

		public void Rollback(string txId)
		{
			asyncServerClient.RollbackAsync(txId).WaitUnwrap();
		}

		public void PrepareTransaction(string txId, Guid? resourceManagerId, byte[] recoveryInformation)
		{
			asyncServerClient.PrepareTransactionAsync(txId).WaitUnwrap();
		}

		public BuildNumber GetBuildNumber()
		{
			return asyncServerClient.GetBuildNumberAsync().ResultUnwrap();
		}

		public IndexMergeResults GetIndexMergeSuggestions()
		{
			return asyncServerClient.GetIndexMergeSuggestionsAsync().ResultUnwrap();
		}

		public LogItem[] GetLogs(bool errorsOnly)
		{
			return asyncServerClient.GetLogsAsync(errorsOnly).ResultUnwrap();
		}

		public LicensingStatus GetLicenseStatus()
		{
			return asyncServerClient.GetLicenseStatusAsync().ResultUnwrap();
		}

		public ILowLevelBulkInsertOperation GetBulkInsertOperation(BulkInsertOptions options, IDatabaseChanges changes)
		{
			return asyncServerClient.GetBulkInsertOperation(options, changes);
		}

		public HttpJsonRequest CreateReplicationAwareRequest(string currentServerUrl, string requestUrl, string method, bool disableRequestCompression = false, bool disableAuthentication = false, TimeSpan? timeout = null)
		{
			return asyncServerClient.CreateReplicationAwareRequest(currentServerUrl, requestUrl, method, disableRequestCompression, disableAuthentication, timeout);
		}

		[Obsolete("Use RavenFS instead.")]
		public AttachmentInformation[] GetAttachments(int start, Etag startEtag, int pageSize)
		{
			return asyncServerClient.GetAttachmentsAsync(start, startEtag, pageSize).ResultUnwrap();
		}

		public IDatabaseCommands With(ICredentials credentialsForSession)
		{
			return new ServerClient(asyncServerClient.WithInternal(credentialsForSession));
		}

		public IDisposable ForceReadFromMaster()
		{
			return asyncServerClient.ForceReadFromMaster();
		}

		public IDatabaseCommands ForDatabase(string database)
		{
			return new ServerClient(asyncServerClient.ForDatabaseInternal(database));
		}

		public IDatabaseCommands ForSystemDatabase()
		{
			return new ServerClient(asyncServerClient.ForSystemDatabaseInternal());
		}

		public string Url
		{
			get { return asyncServerClient.Url; }
		}

		public Operation DeleteByIndex(string indexName, IndexQuery queryToDelete, BulkOperationOptions options = null)
		{
			return asyncServerClient.DeleteByIndexAsync(indexName, queryToDelete, options).ResultUnwrap();
		}

		public Operation UpdateByIndex(string indexName, IndexQuery queryToUpdate, PatchRequest[] patchRequests,
			BulkOperationOptions options = null)
		{
			return asyncServerClient.UpdateByIndexAsync(indexName, queryToUpdate, patchRequests, options).ResultUnwrap();
		}

		public Operation UpdateByIndex(string indexName, IndexQuery queryToUpdate, ScriptedPatchRequest patch,
			BulkOperationOptions options = null)
		{
			return asyncServerClient.UpdateByIndexAsync(indexName, queryToUpdate, patch, options).ResultUnwrap();
		}

		public SuggestionQueryResult Suggest(string index, SuggestionQuery suggestionQuery)
		{
			return asyncServerClient.SuggestAsync(index, suggestionQuery).ResultUnwrap();
		}

		public MultiLoadResult MoreLikeThis(MoreLikeThisQuery query)
		{
			return asyncServerClient.MoreLikeThisAsync(query).ResultUnwrap();
		}

		public DatabaseStatistics GetStatistics()
		{
			return asyncServerClient.GetStatisticsAsync().ResultUnwrap();
		}

		public long NextIdentityFor(string name)
		{
			return asyncServerClient.NextIdentityForAsync(name).ResultUnwrap();
		}

		public long SeedIdentityFor(string name, long value)
		{
			return asyncServerClient.SeedIdentityForAsync(name, value).ResultUnwrap();
		}

		public string UrlFor(string documentKey)
		{
			return asyncServerClient.UrlFor(documentKey);
		}

		public JsonDocumentMetadata Head(string key)
		{
			return asyncServerClient.HeadAsync(key).ResultUnwrap();
		}

		public GetResponse[] MultiGet(GetRequest[] requests)
		{
			return asyncServerClient.MultiGetAsync(requests).ResultUnwrap();
		}

		public IEnumerable<string> GetTerms(string index, string field, string fromValue, int pageSize)
		{
			return asyncServerClient.GetTermsAsync(index, field, fromValue, pageSize).ResultUnwrap();
		}

		public FacetResults GetFacets(string index, IndexQuery query, string facetSetupDoc, int start, int? pageSize)
		{
			return asyncServerClient.GetFacetsAsync(index, query, facetSetupDoc, start, pageSize).ResultUnwrap();
		}

		public FacetResults[] GetMultiFacets(FacetQuery[] facetedQueries)
		{
			return asyncServerClient.GetMultiFacetsAsync(facetedQueries).ResultUnwrap();
		}

		public FacetResults GetFacets(string index, IndexQuery query, List<Facet> facets, int start, int? pageSize)
		{
			return asyncServerClient.GetFacetsAsync(index, query, facets, start, pageSize).ResultUnwrap();
		}

		public RavenJObject Patch(string key, PatchRequest[] patches)
		{
			return asyncServerClient.PatchAsync(key, patches, null).ResultUnwrap();
		}

		public RavenJObject Patch(string key, PatchRequest[] patches, bool ignoreMissing)
		{
			return asyncServerClient.PatchAsync(key, patches, ignoreMissing).ResultUnwrap();
		}

		public RavenJObject Patch(string key, ScriptedPatchRequest patch)
		{
			return asyncServerClient.PatchAsync(key, patch, null).ResultUnwrap();
		}

		public RavenJObject Patch(string key, ScriptedPatchRequest patch, bool ignoreMissing)
		{
			return asyncServerClient.PatchAsync(key, patch, ignoreMissing).ResultUnwrap();
		}

		public RavenJObject Patch(string key, PatchRequest[] patches, Etag etag)
		{
			return asyncServerClient.PatchAsync(key, patches, etag).ResultUnwrap();
		}

		public RavenJObject Patch(string key, PatchRequest[] patchesToExisting, PatchRequest[] patchesToDefault,
			RavenJObject defaultMetadata)
		{
			return asyncServerClient.PatchAsync(key, patchesToExisting, patchesToDefault, defaultMetadata).ResultUnwrap();
		}

		public RavenJObject Patch(string key, ScriptedPatchRequest patch, Etag etag)
		{
			return asyncServerClient.PatchAsync(key, patch, etag).ResultUnwrap();
		}

		public RavenJObject Patch(string key, ScriptedPatchRequest patchExisting, ScriptedPatchRequest patchDefault,
			RavenJObject defaultMetadata)
		{
			return asyncServerClient.PatchAsync(key, patchExisting, patchDefault, defaultMetadata).ResultUnwrap();
		}

		public HttpJsonRequest CreateRequest(string relativeUrl, string method, bool disableRequestCompression = false, bool disableAuthentication = false, TimeSpan? timeout = null)
		{
			return asyncServerClient.CreateRequest(relativeUrl, method, disableRequestCompression, disableAuthentication, timeout);
		}

		public IDisposable DisableAllCaching()
		{
			return asyncServerClient.DisableAllCaching();
		}

		internal ReplicationDocument DirectGetReplicationDestinations(OperationMetadata operationMetadata)
		{
			return asyncServerClient.DirectGetReplicationDestinationsAsync(operationMetadata).ResultUnwrap();
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
			return asyncServerClient.Info.GetReplicationInfoAsync().ResultUnwrap();
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
				return asyncEnumerator.MoveNextAsync().ResultUnwrap();
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
