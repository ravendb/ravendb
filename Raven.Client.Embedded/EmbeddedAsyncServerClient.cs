using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Util;
using Raven.Client.Connection;
using Raven.Client.Connection.Async;
using Raven.Client.Connection.Profiling;
using Raven.Json.Linq;

namespace Raven.Client.Embedded
{
	internal class EmbeddedAsyncServerClient : IAsyncDatabaseCommands
	{
		private readonly IDatabaseCommands databaseCommands;

		public EmbeddedAsyncServerClient(IDatabaseCommands databaseCommands)
		{
			this.databaseCommands = databaseCommands;
		}

		public void Dispose()
		{
		}

		public ProfilingInformation ProfilingInformation
		{
			get { return databaseCommands.ProfilingInformation; }
		}

		public IDictionary<string, string> OperationsHeaders
		{
			get
			{
				// IDatabaseCommands.OperationsHeaders is of type NameValueCollection. Should it, or this property,
				// be changed to the types match?
				throw new NotSupportedException();
			}
		}

		public Task<JsonDocument> GetAsync(string key)
		{
			return new CompletedTask<JsonDocument>(databaseCommands.Get(key));
		}

		public Task<MultiLoadResult> GetAsync(string[] keys, string[] includes, bool metadataOnly = false)
		{
			return new CompletedTask<MultiLoadResult>(databaseCommands.Get(keys, includes, metadataOnly));
		}

		public Task<JsonDocument[]> GetDocumentsAsync(int start, int pageSize, bool metadataOnly = false)
		{
			return new CompletedTask<JsonDocument[]>(databaseCommands.GetDocuments(start, pageSize, metadataOnly));
		}

		public Task<QueryResult> QueryAsync(string index, IndexQuery query, string[] includes, bool metadataOnly = false)
		{
			return new CompletedTask<QueryResult>(databaseCommands.Query(index, query, includes, metadataOnly));
		}

		public Task<BatchResult[]> BatchAsync(ICommandData[] commandDatas)
		{
			return new CompletedTask<BatchResult[]>(databaseCommands.Batch(commandDatas));
		}

		public Task<SuggestionQueryResult> SuggestAsync(string index, SuggestionQuery suggestionQuery)
		{
			return new CompletedTask<SuggestionQueryResult>(databaseCommands.Suggest(index, suggestionQuery));
		}

		public Task<string[]> GetIndexNamesAsync(int start, int pageSize)
		{
			return new CompletedTask<string[]>(databaseCommands.GetIndexNames(start, pageSize));
		}

		public Task<IndexDefinition[]> GetIndexesAsync(int start, int pageSize)
		{
			return new CompletedTask<IndexDefinition[]>(databaseCommands.GetIndexes(start, pageSize));
		}

		public Task ResetIndexAsync(string name)
		{
			databaseCommands.ResetIndex(name);
			return new CompletedTask();
		}

		public Task<IndexDefinition> GetIndexAsync(string name)
		{
			return new CompletedTask<IndexDefinition>(databaseCommands.GetIndex(name));
		}

		public Task<string> PutIndexAsync(string name, IndexDefinition indexDef, bool overwrite)
		{
			return new CompletedTask<string>(databaseCommands.PutIndex(name, indexDef, overwrite));
		}

		public Task DeleteIndexAsync(string name)
		{
			databaseCommands.DeleteIndex(name);
			return new CompletedTask();
		}

		public Task DeleteByIndexAsync(string indexName, IndexQuery queryToDelete, bool allowStale)
		{
			databaseCommands.DeleteByIndex(indexName, queryToDelete, allowStale);
			return new CompletedTask();
		}

		public Task DeleteDocumentAsync(string id)
		{
			databaseCommands.Delete(id, null);
			return new CompletedTask();
		}

		public Task<PutResult> PutAsync(string key, Guid? etag, RavenJObject document, RavenJObject metadata)
		{
			return new CompletedTask<PutResult>(databaseCommands.Put(key, etag, document, metadata));
		}

		public IAsyncDatabaseCommands ForDatabase(string database)
		{
			return new EmbeddedAsyncServerClient(databaseCommands.ForDatabase(database));
		}

		public IAsyncDatabaseCommands ForDefaultDatabase()
		{
			return new EmbeddedAsyncServerClient(databaseCommands.ForDefaultDatabase());
		}

		public IAsyncDatabaseCommands With(ICredentials credentialsForSession)
		{
			return new EmbeddedAsyncServerClient(databaseCommands.With(credentialsForSession));
		}

		public Task<DatabaseStatistics> GetStatisticsAsync()
		{
			return new CompletedTask<DatabaseStatistics>(databaseCommands.GetStatistics());
		}

		public Task<string[]> GetDatabaseNamesAsync(int pageSize, int start = 0)
		{
			return new CompletedTask<string[]>(databaseCommands.GetDatabaseNames(pageSize, start));
		}

		public Task PutAttachmentAsync(string key, Guid? etag, byte[] data, RavenJObject metadata)
		{
			// Should the data paramater be changed to a Stream type so it matches IDatabaseCommands.PutAttachment?
			var stream = new MemoryStream();
			stream.Write(data, 0, data.Length);
			databaseCommands.PutAttachment(key, etag, stream, metadata);
			return new CompletedTask();
		}

		public Task<Attachment> GetAttachmentAsync(string key)
		{
			return new CompletedTask<Attachment>(databaseCommands.GetAttachment(key));
		}

		public Task DeleteAttachmentAsync(string key, Guid? etag)
		{
			databaseCommands.DeleteAttachment(key, etag);
			return new CompletedTask();
		}

		public Task<string[]> GetTermsAsync(string index, string field, string fromValue, int pageSize)
		{
			return new CompletedTask<string[]>(databaseCommands.GetTerms(index, field, fromValue, pageSize).ToArray());
		}

		public Task EnsureSilverlightStartUpAsync()
		{
			throw new NotSupportedException("Method to be removed from IAsyncDatabaseCommands (RavenDB-761)");
		}

		public IDisposable DisableAllCaching()
		{
			return databaseCommands.DisableAllCaching();
		}

		public Task<GetResponse[]> MultiGetAsync(GetRequest[] requests)
		{
			return new CompletedTask<GetResponse[]>(databaseCommands.MultiGet(requests));
		}

		public Task UpdateByIndex(string indexName, IndexQuery queryToUpdate, ScriptedPatchRequest patch)
		{
			databaseCommands.UpdateByIndex(indexName, queryToUpdate, patch);
			return new CompletedTask();
		}

		public Task UpdateByIndex(string indexName, IndexQuery queryToUpdate, ScriptedPatchRequest patch, bool allowStale)
		{
			databaseCommands.UpdateByIndex(indexName, queryToUpdate, patch, allowStale);
			return new CompletedTask();
		}

		public Task<FacetResults> GetFacetsAsync(string index, IndexQuery query, string facetSetupDoc)
		{
			return new CompletedTask<FacetResults>(databaseCommands.GetFacets(index, query, facetSetupDoc));
		}

		public Task<LogItem[]> GetLogsAsync(bool errorsOnly)
		{
			// No sync equivalent on IDatabaseCommands.
			throw new NotSupportedException();
		}

		public Task<LicensingStatus> GetLicenseStatusAsync()
		{
			// No sync equivalent on IDatabaseCommands.
			throw new NotSupportedException();
		}

		public Task<BuildNumber> GetBuildNumberAsync()
		{
			// No sync equivalent on IDatabaseCommands.
			throw new NotSupportedException();
		}

		public Task StartBackupAsync(string backupLocation, DatabaseDocument databaseDocument)
		{
			// No sync equivalent on IDatabaseCommands.
			throw new NotSupportedException();
		}

		public Task StartRestoreAsync(string restoreLocation, string databaseLocation, string databaseName = null)
		{
			// No sync equivalent on IDatabaseCommands.
			throw new NotSupportedException();
		}

		public Task StartIndexingAsync()
		{
			// No sync equivalent on IDatabaseCommands.
			throw new NotSupportedException();
		}

		public Task StopIndexingAsync()
		{
			// No sync equivalent on IDatabaseCommands.
			throw new NotSupportedException();
		}

		public Task<string> GetIndexingStatusAsync()
		{
			// No sync equivalent on IDatabaseCommands.
			throw new NotSupportedException();
		}

		public Task<JsonDocument[]> StartsWithAsync(string keyPrefix, int start, int pageSize, bool metadataOnly = false)
		{
			// Should add a 'matches' paramater? Setting to null for now.
			return new CompletedTask<JsonDocument[]>(databaseCommands.StartsWith(keyPrefix, null, start, pageSize, metadataOnly));
		}

		public void ForceReadFromMaster()
		{
			databaseCommands.ForceReadFromMaster();
		}
	}
}