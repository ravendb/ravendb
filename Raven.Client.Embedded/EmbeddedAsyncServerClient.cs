using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Json;
using Raven.Abstractions.Util;
using Raven.Client.Connection;
using Raven.Client.Connection.Async;
using Raven.Client.Connection.Profiling;
using Raven.Database;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Client.Embedded
{
	internal class EmbeddedAsyncServerClient : IAsyncDatabaseCommands
	{
		private readonly IDatabaseCommands databaseCommands;
		private readonly DocumentDatabase documentDatabase;

		public EmbeddedAsyncServerClient(IDatabaseCommands databaseCommands, DocumentDatabase documentDatabase)
		{
			this.databaseCommands = databaseCommands;
			this.documentDatabase = documentDatabase;
		}

		public void Dispose()
		{}

		public ProfilingInformation ProfilingInformation { get { return databaseCommands.ProfilingInformation; } }

		public IDictionary<string, string> OperationsHeaders { get { throw new NotSupportedException(); } }

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
			throw new NotSupportedException();
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
			RavenJArray json = documentDatabase.GetIndexes(start, pageSize);
			//NOTE: To review, I'm not confidence this is the correct way to deserialize the index definition
			IndexDefinition[] indexDefinitions = json
				.Select(x => JsonConvert.DeserializeObject<IndexDefinition>(((RavenJObject) x)["definition"].ToString(), new JsonToJsonConverter()))
				.ToArray();
			return new CompletedTask<IndexDefinition[]>(indexDefinitions);
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
			return new EmbeddedAsyncServerClient(databaseCommands.ForDatabase(database), documentDatabase);
		}

		public IAsyncDatabaseCommands ForDefaultDatabase()
		{
			return new EmbeddedAsyncServerClient(databaseCommands.ForDefaultDatabase(), documentDatabase);
		}

		public IAsyncDatabaseCommands With(ICredentials credentialsForSession)
		{
			return new EmbeddedAsyncServerClient(databaseCommands.With(credentialsForSession), documentDatabase);
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
			throw new NotSupportedException();
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
			// No equivalent on IDatabaseCommands or DocumentDatabase.
			throw new NotSupportedException();
		}

		public Task<LicensingStatus> GetLicenseStatusAsync()
		{
			// No equivalent on IDatabaseCommands or DocumentDatabase.
			throw new NotSupportedException();
		}

		public Task<BuildNumber> GetBuildNumberAsync()
		{
			// No equivalent on IDatabaseCommands or DocumentDatabase.
			throw new NotSupportedException();
		}

		public Task StartBackupAsync(string backupLocation, DatabaseDocument databaseDocument)
		{
			// don't know what the incremental backup valud should be. Should it be added as a paramater to this method?
			documentDatabase.StartBackup(backupLocation, false, databaseDocument); 
			return new CompletedTask();
		}

		public Task StartRestoreAsync(string restoreLocation, string databaseLocation, string databaseName = null)
		{
			// No equivalent on IDatabaseCommands or DocumentDatabase.
			throw new NotSupportedException();
		}

		public Task StartIndexingAsync()
		{
			// No equivalent on IDatabaseCommands or DocumentDatabase.
			throw new NotSupportedException();
		}

		public Task StopIndexingAsync()
		{
			// No equivalent on IDatabaseCommands or DocumentDatabase.
			throw new NotSupportedException();
		}

		public Task<string> GetIndexingStatusAsync()
		{
			// No equivalent on IDatabaseCommands or DocumentDatabase.
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