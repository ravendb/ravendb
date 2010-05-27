using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Transactions;
using Newtonsoft.Json.Linq;
using System;
using Raven.Client.Client;
using Raven.Client.Linq;
using Raven.Database;
using Raven.Database.Data;

namespace Raven.Client.Document
{
	public class DocumentSession : InMemoryDocumentSessionOperations, IDocumentSession
	{
		private RavenClientEnlistment enlistment;

		public override event EntityStored Stored;

		public override event EntityToDocument OnEntityConverted;

		private IDatabaseCommands DatabaseCommands
		{
			get { return documentStore.DatabaseCommands; }
		}


		public DocumentSession(DocumentStore documentStore) : base(documentStore)
		{
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

		public void Refresh<T>(T entity)
	    {
	        DocumentMetadata value;
	        if(entitiesAndMetadata.TryGetValue(entity, out value) == false)
	            throw new InvalidOperationException("Cannot refresh a trasient instance");
	        var jsonDocument = documentStore.DatabaseCommands.Get(value.Key);
            if(jsonDocument == null)
                throw new InvalidOperationException("Document '" + value.Key + "' no longer exists and was probably deleted");

	        value.Metadata = jsonDocument.Metadata;
	        value.ETag = jsonDocument.Etag;
	        value.OriginalValue = jsonDocument.DataAsJson;
	        var newEntity = ConvertToEntity<T>(value.Key, jsonDocument.DataAsJson, jsonDocument.Metadata);
	        foreach (PropertyDescriptor property in TypeDescriptor.GetProperties(entity))
	        {
                property.SetValue(entity, property.GetValue(newEntity));
	        }
	    }

	    public void Evict<T>(T entity)
		{
		    DocumentMetadata value;
		    if(entitiesAndMetadata.TryGetValue(entity, out value))
		    {
		        entitiesAndMetadata.Remove(entity);
		        entitiesByKey.Remove(value.Key);
		    }
		    deletedEntities.Remove(entity);
		}

		public void SaveChanges()
		{
            if(enlistment == null && Transaction.Current != null)
            {
                enlistment = new RavenClientEnlistment(this, Transaction.Current.TransactionInformation.DistributedIdentifier);
                Transaction.Current.EnlistVolatile(enlistment,EnlistmentOptions.None);
            }
			var entities = new List<object>();
			var cmds = new List<ICommandData>();
		    DocumentMetadata value = null;
		    foreach (var key in (from deletedEntity in deletedEntities
                                 where entitiesAndMetadata.TryGetValue(deletedEntity, out value)
                                 select value.Key))
            {
                Guid? etag = null;
                object existingEntity;
                if (entitiesByKey.TryGetValue(key, out existingEntity))
                {
                    DocumentMetadata metadata;
                    if (entitiesAndMetadata.TryGetValue(existingEntity, out metadata))
                        etag = metadata.ETag;
                    entitiesAndMetadata.Remove(existingEntity);
                }

                etag = UseOptimisticConcurrency ? etag : null;
            	entities.Add(existingEntity);
                cmds.Add(new DeleteCommandData
                {
                	Etag = etag,
					Key = key,
                });
            }
            deletedEntities.Clear();
		    foreach (var entity in entitiesAndMetadata.Where(EntityChanged))
			{
				entities.Add(entity.Key);
				if (entity.Value.Key != null)
					entitiesByKey.Remove(entity.Value.Key);
				cmds.Add(CreatePutEntityCommand(entity.Key, entity.Value));
			}
			
			if (cmds.Count == 0)
				return;

            IncrementRequestCount();
            Trace.WriteLine(string.Format("Saving {0} changes to {1}", cmds.Count, StoreIdentifier));
			UpdateBatchResults(DatabaseCommands.Batch(cmds.ToArray()), entities);
		}

		public IDocumentQuery<T> LuceneQuery<T>(string indexName)
		{
			return new DocumentQuery<T>(this, DatabaseCommands, indexName, null);
		}

        #region IDisposable Members

        public void Dispose()
        {
            //dereference all event listeners
            Stored = null;
        }

        #endregion

	    public void Commit(Guid txId)
	    {
            IncrementRequestCount();
            documentStore.DatabaseCommands.Commit(txId);
	        enlistment = null;
	    }

	    public void Rollback(Guid txId)
	    {
            IncrementRequestCount();
            documentStore.DatabaseCommands.Rollback(txId);
            enlistment = null;
	    }

        public class DocumentMetadata
        {
			public JObject OriginalValue { get; set; }
            public JObject Metadata { get; set; }
            public Guid? ETag { get; set; }
            public string Key { get; set; }
        }
	}
}