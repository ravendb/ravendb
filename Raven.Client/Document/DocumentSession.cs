using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Text;
using System.Transactions;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Client.Client;
using System;
using Raven.Database;

namespace Raven.Client.Document
{
	public class DocumentSession : IDocumentSessionImpl
	{
	    private const string TemporaryIdPrefix = "Temporary Id: ";
	    private readonly IDatabaseCommands database;
		private readonly DocumentStore documentStore;
		private readonly Dictionary<object, JObject> entitiesAndMetadata = new Dictionary<object, JObject>();
        private readonly Dictionary<string, object> entitiesByKey = new Dictionary<string, object>();

	    private readonly ISet<object> deletedEntities = new HashSet<object>();
	    private RavenClientEnlistment enlistment;

		public event Action<object> Stored;
        public string StoreIdentifier { get { return documentStore.Identifier; } }

		public DocumentSession(DocumentStore documentStore, IDatabaseCommands database)
		{
			this.documentStore = documentStore;
			this.database = database;
		}

		public T Load<T>(string id)
		{
		    object existingEntity;
		    if(entitiesByKey.TryGetValue(id, out existingEntity))
		    {
		        return (T)existingEntity;
		    }

            JsonDocument documentFound;

            try
            {
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

			var jsonString = Encoding.UTF8.GetString(documentFound.Data);
			var entity = ConvertToEntity<T>(id, jsonString);
			entitiesAndMetadata.Add(entity, documentFound.Metadata);
		    entitiesByKey[id] = entity;
			return (T) entity;
		}

	    public void Delete<T>(T entity)
	    {
	        deletedEntities.Add(entity);
	    }

	    private object ConvertToEntity<T>(string id, string documentFound)
		{
			var entity = JsonConvert.DeserializeObject(documentFound, typeof (T));

			foreach (var property in entity.GetType().GetProperties())
			{
				var isIdentityProperty = documentStore.Conventions.FindIdentityProperty.Invoke(property);
				if (isIdentityProperty)
					property.SetValue(entity, id, null);
			}
			return entity;
		}

		public void Store<T>(T entity)
		{
			var tag = documentStore.Conventions.FindTypeTagName(typeof(T));
			entitiesAndMetadata.Add(entity, new JObject(new JProperty("Raven-Entity-Name", new JValue(tag))));
		    var identityProperty = GetIdentityProperty(typeof(T));
		    var id = identityProperty.GetValue(entity, null) as string ?? TemporaryIdPrefix + Guid.NewGuid();
		    entitiesByKey[id] = entity;
		}

		public void Evict<T>(T entity)
		{
			entitiesAndMetadata.Remove(entity);
		}

		private void StoreEntity(KeyValuePair<object, JObject> entityAndMetadata)
		{
			var json = ConvertEntityToJson(entityAndMetadata.Key);
			var entityType = entityAndMetadata.Key.GetType();
			PropertyInfo identityProperty = GetIdentityProperty(entityType);

            var key = (string)identityProperty.GetValue(entityAndMetadata.Key, null);
            if (key != null && key.StartsWith(TemporaryIdPrefix))
            {
                entitiesByKey.Remove(key);
                key = null;
            }
            key = database.Put(key, null, json, entityAndMetadata.Value);
		    entitiesByKey[key] = entityAndMetadata.Key;
			identityProperty.SetValue(entityAndMetadata.Key, key, null);

			var stored = Stored;
			if (stored != null)
				stored(entityAndMetadata.Key);
		}

		private PropertyInfo GetIdentityProperty(Type entityType)
		{
			var identityProperty = entityType.GetProperties()
				.FirstOrDefault(q => documentStore.Conventions.FindIdentityProperty(q));

			if(identityProperty == null)
				throw new InvalidOperationException("Could not find id proeprty for " + entityType.Name);
			return identityProperty;
		}

		public void SaveChanges()
		{
            if(enlistment == null && Transaction.Current != null)
            {
                enlistment = new RavenClientEnlistment(this, Transaction.Current.TransactionInformation.DistributedIdentifier);
                Transaction.Current.EnlistPromotableSinglePhase(enlistment);
            }
            foreach (var key in (from deletedEntity in deletedEntities
                                 let identityProperty = GetIdentityProperty(deletedEntity.GetType())
                                 select identityProperty.GetValue(deletedEntity, null))
                                 .OfType<string>())
            {
                object existingEntity;
                if (entitiesByKey.TryGetValue(key, out existingEntity))
                    entitiesAndMetadata.Remove(existingEntity);

                documentStore.DatabaseCommands.Delete(key, null);
            }
            deletedEntities.Clear();
		    foreach (var entity in entitiesAndMetadata)
			{
				//TODO: Switch to more the batch version when it becomes available
				StoreEntity(entity);
			}
		    
		}

		private JObject ConvertEntityToJson(object entity)
		{
			var identityProperty = entity.GetType().GetProperties()
				.FirstOrDefault(q => documentStore.Conventions.FindIdentityProperty.Invoke(q));

			var objectAsJson = JObject.FromObject(entity);
			if (identityProperty != null)
			{
				objectAsJson.Remove(identityProperty.Name);
			}

			objectAsJson.Add("type", JToken.FromObject(entity.GetType().FullName));
			return objectAsJson;
		}

		public void Clear()
		{
			entitiesAndMetadata.Clear();
		}

		public IDocumentQuery<T> Query<T>(string indexName)
		{
			return new DocumentQuery<T>(database, indexName);
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
	        documentStore.DatabaseCommands.Commit(txId);
	        enlistment = null;
	    }

	    public void Rollback(Guid txId)
	    {
	        documentStore.DatabaseCommands.Rollback(txId);
            enlistment = null;
	    }
	}
}