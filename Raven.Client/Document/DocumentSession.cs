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
            IncrementRequestCount();
            Trace.WriteLine(string.Format("Bulk loading ids [{0}] from {1}", string.Join(", ", ids), StoreIdentifier));
			return documentStore.DatabaseCommands.Get(ids)
                .Select(TrackEntity<T>).ToArray();
	    }

		public IRavenQueryable<T> Query<T>(string indexName)
	    {
	        return new RavenQueryable<T>(new RavenQueryProvider<T>(this, indexName));
	    }

		public IRavenQueryable<T> Query<T, TIndexCreator>(string indexName) where TIndexCreator : AbstractIndexCreationTask, new()
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
}