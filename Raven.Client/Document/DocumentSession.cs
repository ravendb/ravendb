using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Net;
using Newtonsoft.Json.Linq;
using System;
using Raven.Client.Client;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Raven.Database;
using Raven.Database.Data;

namespace Raven.Client.Document
{
	public class DocumentSession : InMemoryDocumentSessionOperations, IDocumentSession, ITransactionalDocumentSession
	{
		public IDatabaseCommands DatabaseCommands { get; private set; }

		public DocumentSession(DocumentStore documentStore, IDocumentStoreListener[] storeListeners, IDocumentDeleteListener[] deleteListeners)
			: base(documentStore, storeListeners, deleteListeners)
		{
			DatabaseCommands = documentStore.DatabaseCommands;
		}

		public T Load<T>(string id)
		{
		    object existingEntity;
		    if(entitiesByKey.TryGetValue(id, out existingEntity))
		    {
		        return (T)existingEntity;
		    }

	        IncrementRequestCount();

			JsonDocument documentFound;
            try
            {
				Trace.WriteLine(string.Format("Loading document [{0}] from {1}", id, StoreIdentifier));
				documentFound = DatabaseCommands.Get(id);
            }
            catch (WebException ex)
            {
            	var httpWebResponse = ex.Response as HttpWebResponse;
            	if (httpWebResponse != null && httpWebResponse.StatusCode == HttpStatusCode.NotFound)
                    return default(T);
            	throw;
            }
			if (documentFound == null)
				return default(T);

			return TrackEntity<T>(documentFound);
		}

		public T[] Load<T>(params string[] ids)
		{
			return LoadInternal<T>(ids, null);
		}


		internal T[] LoadInternal<T>(string[] ids, string[] includes)
		{
			IncrementRequestCount();
			Trace.WriteLine(string.Format("Bulk loading ids [{0}] from {1}", string.Join(", ", ids), StoreIdentifier));
			var multiLoadResult = documentStore.DatabaseCommands.Get(ids, includes);

			foreach (var include in SerializationHelper.JObjectsToJsonDocuments(multiLoadResult.Includes))
			{
				TrackEntity<object>(include);
			}

			return SerializationHelper.JObjectsToJsonDocuments(multiLoadResult.Results)
				.Select(TrackEntity<T>)
				.ToArray();
		}

		public IRavenQueryable<T> Query<T>(string indexName)
	    {
	        return new RavenQueryable<T>(new RavenQueryProvider<T>(this, indexName));
	    }

		public IRavenQueryable<T> Query<T, TIndexCreator>() where TIndexCreator : AbstractIndexCreationTask, new()
		{
			var indexCreator = new TIndexCreator();
			return Query<T>(indexCreator.IndexName);
		}

		public void Refresh<T>(T entity)
	    {
	        DocumentMetadata value;
	        if(entitiesAndMetadata.TryGetValue(entity, out value) == false)
	            throw new InvalidOperationException("Cannot refresh a trasient instance");
			var jsonDocument = documentStore.DatabaseCommands.Get(value.Key);
            if(jsonDocument == null)
                throw new InvalidOperationException("Document '" + value.Key + "' no longer exists and was probably deleted");

	        value.Metadata = jsonDocument.Metadata;
			value.OriginalMetadata = new JObject(jsonDocument.Metadata);
	        value.ETag = jsonDocument.Etag;
	        value.OriginalValue = jsonDocument.DataAsJson;
	        var newEntity = ConvertToEntity<T>(value.Key, jsonDocument.DataAsJson, jsonDocument.Metadata);
	        foreach (PropertyDescriptor property in TypeDescriptor.GetProperties(entity))
	        {
                property.SetValue(entity, property.GetValue(newEntity));
	        }
	    }

		public ILoaderWithInclude Include(string path)
		{
			return new MultiLoaderWithInclude(this).Include(path);
		}

		public void SaveChanges()
		{
			var data = PrepareForSaveChanges();
			if (data.Commands.Count == 0)
				return; // nothing to do here
			IncrementRequestCount();
            Trace.WriteLine(string.Format("Saving {0} changes to {1}", data.Commands.Count, StoreIdentifier));
			UpdateBatchResults(DatabaseCommands.Batch(data.Commands.ToArray()), data.Entities);
		}

		public IDocumentQuery<T> LuceneQuery<T>(string indexName)
		{
			return new DocumentQuery<T>(this, DatabaseCommands, indexName, null);
		}

	    public override void Commit(Guid txId)
	    {
            IncrementRequestCount();
			documentStore.DatabaseCommands.Commit(txId);
	        ClearEnlistment();
	    }

		public override void Rollback(Guid txId)
	    {
            IncrementRequestCount();
			documentStore.DatabaseCommands.Rollback(txId);
			ClearEnlistment();
	    }

		public override byte[] PromoteTransaction(Guid fromTxId)
		{
			return documentStore.DatabaseCommands.PromoteTransaction(fromTxId);
		}

		public void StoreRecoveryInformation(Guid txId, byte[] recoveryInformation)
		{
			documentStore.DatabaseCommands.StoreRecoveryInformation(txId, recoveryInformation);
		}

		public class DocumentMetadata
        {
			public JObject OriginalValue { get; set; }
            public JObject Metadata { get; set; }
            public Guid? ETag { get; set; }
            public string Key { get; set; }
			public JObject OriginalMetadata { get; set; }
        }

		public class SaveChangesData
		{
			public IList<ICommandData> Commands { get; set; }
			public IList<object> Entities { get; set; }
		}
    }

	public interface ILoaderWithInclude
	{
		MultiLoaderWithInclude Include(string path);
		T[] Load<T>(params string[] ids);

		T Load<T>(string id);
	}

	public class MultiLoaderWithInclude : ILoaderWithInclude
	{
		private readonly DocumentSession session;
		private readonly List<string> includes = new List<string>();

		public MultiLoaderWithInclude Include(string path)
		{
			includes.Add(path);
			return this;
		}

		public MultiLoaderWithInclude(DocumentSession session)
		{
			this.session = session;
		}

		public T[] Load<T>(params string[] ids)
		{
			return session.LoadInternal<T>(ids, includes.ToArray());
		}

		public T Load<T>(string id)
		{
			return Load<T>(new[] {id}).FirstOrDefault();
		}
	}
}