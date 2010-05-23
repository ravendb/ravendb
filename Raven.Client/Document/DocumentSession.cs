using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Transactions;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Client.Client;
using System;
using Raven.Client.Exceptions;
using Raven.Client.Linq;
using Raven.Database;
using Raven.Database.Data;
using Raven.Database.Json;

namespace Raven.Client.Document
{
	public class DocumentSession : IDocumentSession
	{
		private const string RavenEntityName = "Raven-Entity-Name";
		private readonly IDatabaseCommands database;
		private readonly DocumentStore documentStore;
        private readonly Dictionary<object, DocumentMetadata> entitiesAndMetadata = new Dictionary<object, DocumentMetadata>();
        private readonly Dictionary<string, object> entitiesByKey = new Dictionary<string, object>();

		private readonly HashSet<object> deletedEntities = new HashSet<object>();
	    private RavenClientEnlistment enlistment;
	    private int numberOfRequests;

	    public event EntityStored Stored;
	    public event EntityToDocument OnEntityConverted;

	    public JObject GetMetadataFor<T>(T instance)
	    {
	        DocumentMetadata value;
            if (entitiesAndMetadata.TryGetValue(instance, out value) == false)
                return null;
	        return value.Metadata;
	    }

	    public string StoreIdentifier { get { return documentStore.Identifier; } }

		public DocumentConvention Conventions
		{
			get { return documentStore.Conventions; }
		}

		public DocumentSession(DocumentStore documentStore, IDatabaseCommands database)
		{
			this.documentStore = documentStore;
			this.database = database;
		    UseOptimisticConcurrency = false;
		    MaxNumberOfRequestsPerSession = documentStore.Conventions.MaxNumberOfRequestsPerSession;
		}

        public int MaxNumberOfRequestsPerSession
        {
            get; set;
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
				documentFound = database.Get(id);
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

	    internal void IncrementRequestCount()
	    {
	        if(++numberOfRequests > MaxNumberOfRequestsPerSession)
                throw new InvalidOperationException(string.Format(@"The maximum number of requests ({0}) allowed for this session has been reached.
Raven limits the number of remote calls that a session is allowed to make as an early warning system. Sessions are expected to be short lived, and 
Raven provide facilities like Load(string[] keys) to load multiple documents at once and batch saves.
You can increase the limit by setting DocumentConvention.MaxNumberOfRequestsPerSession or DocumentSession.MaxNumberOfRequestsPerSession, but it is
advisable that you'll look into reducing the number of remote calls first, since that will speed up your application signficantly and result in a 
more responsible application.
", MaxNumberOfRequestsPerSession));

	    }

	    private T TrackEntity<T>(JsonDocument documentFound)
		{
			if(documentFound.Metadata.Property("@etag") == null)
			{
				documentFound.Metadata.Add("@etag", new JValue(documentFound.Etag.ToString()));
			}
			return TrackEntity<T>(documentFound.Key, documentFound.DataAsJson, documentFound.Metadata);
		}

		public T TrackEntity<T>(string key, JObject document, JObject metadata)
	    {
			object entity;
			if (entitiesByKey.TryGetValue(key, out entity) == false)
			{
				entity = ConvertToEntity<T>(key, document, metadata);
			}
			else
			{
				// the local instnace may have been changed, we adhere to the current Unit of Work
				// instance, and return that, ignoring anything new.
				return (T) entity;
			}
			var etag = metadata.Value<string>("@etag");
			entitiesAndMetadata[entity] =  new DocumentMetadata
	        {
				OriginalValue = document,
	            Metadata = metadata,
				ETag = new Guid(etag),
	            Key = key
	        };
	        entitiesByKey[key] = entity;
	        return (T) entity;
	    }

	    public T[] Load<T>(params string[] ids)
	    {
            IncrementRequestCount();
            Trace.WriteLine(string.Format("Bulk loading ids [{0}] from {1}", string.Join(", ", ids), StoreIdentifier));
            return documentStore.DatabaseCommands.Get(ids)
                .Select(TrackEntity<T>).ToArray();
	    }

	    public void Delete<T>(T entity)
	    {
	        deletedEntities.Add(entity);
	    }

	    public IRavenQueryable<T> Query<T>(string indexName)
	    {
	        return new RavenQueryable<T>(new RavenQueryProvider<T>(this, indexName));
	    }

	    private object ConvertToEntity<T>(string id, JObject documentFound, JObject metadata)
	    {
	    	T entity = default(T);

	    	var documentType = metadata.Value<string>("Raven-Clr-Type");
	    	if (documentType != null)
	    	{
	    		Type type = Type.GetType(documentType);
	    		if (type != null)
	    			entity = (T) documentFound.Deserialize(type, Conventions.JsonContractResolver);
	    	}
	    	if (Equals(entity, default(T)))
	    	{
	    		entity = documentFound.Deserialize<T>(Conventions.JsonContractResolver);
	    	}
	    	var identityProperty = documentStore.Conventions.GetIdentityProperty(entity.GetType());
	    	if (identityProperty != null)
	    		identityProperty.SetValue(entity, id, null);
	    	return entity;
	    }

		public void Store<T>(T entity)
		{
            if (ReferenceEquals(null, entity))
                throw new ArgumentNullException("entity");
            var identityProperty = GetIdentityProperty(entity.GetType());
		    string id = null;
            if(identityProperty != null)
		        id = identityProperty.GetValue(entity, null) as string;

		    if (id == null)
            {
                // Generate the key up front
                id = Conventions.GenerateDocumentKey(entity);

                if (id != null && identityProperty != null)
                {
                    // And store it so the client has access to to it
                    identityProperty.SetValue(entity, id, null);
                }                
            }            
            // we make the check here even if we just generated the key
            // users can override the key generation behavior, and we need
            // to detect if they generate duplicates.
			if (id != null && 
                id.EndsWith("/") == false // not a prefix id
                && entitiesByKey.ContainsKey(id))
			{
				if (ReferenceEquals(entitiesByKey[id], entity))
					return;// calling Store twice on the same reference is a no-op
				throw new NonUniqueObjectException("Attempted to associated a different object with id '" + id + "'.");
			}

            var tag = documentStore.Conventions.GetTypeTagName(entity.GetType());
			entitiesAndMetadata.Add(entity, new DocumentMetadata
			{
                Key = id,
                Metadata = new JObject(new JProperty(RavenEntityName, new JValue(tag))),
                ETag = null,
				OriginalValue = new JObject()
			});
			if (id != null)
				entitiesByKey[id] = entity;
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

		private ICommandData CreatePutEntityCommand(object entity, DocumentMetadata documentMetadata)
		{
			var json = ConvertEntityToJson(entity, documentMetadata.Metadata);
			var entityType = entity.GetType();
			var identityProperty = GetIdentityProperty(entityType);

		    string key = null;
            if (identityProperty != null)
                key = (string) identityProperty.GetValue(entity, null);
		    var etag = UseOptimisticConcurrency ? documentMetadata.ETag : null;

			return new PutCommandData
			{
				Document = json,
				Etag = etag,
				Key = key,
				Metadata = documentMetadata.Metadata,
			};
		}

		private PropertyInfo GetIdentityProperty(Type entityType)
		{
		    return documentStore.Conventions.GetIdentityProperty(entityType);
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
			UpdateBatchResults(database.Batch(cmds.ToArray()), entities);
		}

		private void UpdateBatchResults(IList<BatchResult> batchResults, IList<object> entities)
		{
			var stored = Stored;
			for (int i = 0; i < batchResults.Count; i++)
			{
				var batchResult = batchResults[i];
				if (batchResult.Method != "PUT")
					continue;

				var entity = entities[i];
				DocumentMetadata documentMetadata;
				if (entitiesAndMetadata.TryGetValue(entity, out documentMetadata) == false)
					continue;
                                
				entitiesByKey[batchResult.Key] = entity;
				documentMetadata.ETag = batchResult.Etag;
				documentMetadata.Key = batchResult.Key;
				documentMetadata.OriginalValue = ConvertEntityToJson(entity, documentMetadata.Metadata);

                // Set/Update the id of the entity
			    var identityProperty = GetIdentityProperty(entity.GetType());
                if (identityProperty != null && identityProperty.CanWrite)// this is allowed because we need to support store anonymous types
                    identityProperty.SetValue(entity, batchResult.Key, null);

				if (stored != null)
					stored(entity);
			}
		}

		private bool EntityChanged(KeyValuePair<object, DocumentMetadata> kvp)
		{
			var newObj = ConvertEntityToJson(kvp.Key, kvp.Value.Metadata);
			if (kvp.Value == null)
				return true;
			return new JTokenEqualityComparer().Equals(newObj, kvp.Value.OriginalValue) == false;
		}

		private JObject ConvertEntityToJson(object entity, JObject metadata)
		{
			var entityType = entity.GetType();
			var identityProperty = documentStore.Conventions.GetIdentityProperty(entityType);

			var objectAsJson = JObject.FromObject(entity,new JsonSerializer
			{
				Converters = { new JsonEnumConverter() }
			});
			if (identityProperty != null)
			{
				objectAsJson.Remove(identityProperty.Name);
			}

			metadata["Raven-Clr-Type"] = JToken.FromObject(entityType.FullName + ", " + entityType.Assembly.GetName().Name);

		    var entityConverted = OnEntityConverted;
            if(entityConverted!=null)
                entityConverted(entity,objectAsJson, metadata);

		    return objectAsJson;
		}

		public void Clear()
		{
            this.entitiesAndMetadata.Clear();
            this.deletedEntities.Clear();
            this.entitiesByKey.Clear();
		}

	    public bool UseOptimisticConcurrency
	    {
	        get; set;
	    }

	    public IDocumentQuery<T> LuceneQuery<T>(string indexName)
		{
	    	return new DocumentQuery<T>(this, database, indexName, null);
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