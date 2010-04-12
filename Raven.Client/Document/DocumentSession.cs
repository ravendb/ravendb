using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Client.Client;
using Raven.Database.Data;
using System;
using Raven.Database;

namespace Raven.Client.Document
{
	public class DocumentSession : IDocumentSession
	{
		private readonly IDatabaseCommands database;
		private readonly DocumentStore documentStore;
		private readonly HashSet<object> entities = new HashSet<object>();

        public event Action<object> Stored;
        public string StoreIdentifier { get { return documentStore.Identifier; } }

		public DocumentSession(DocumentStore documentStore, IDatabaseCommands database)
		{
			this.documentStore = documentStore;
			this.database = database;
		}

		public T Load<T>(string id)
		{
            JsonDocument documentFound;

            try
            {
                documentFound = database.Get(id);
            }
            catch (System.Net.WebException ex)
            {
                //Status is ProtocolError, couldn't find a better way to trap 404 which shouldn't be an exception
                if (ex.Message == "The remote server returned an error: (404) Not Found.")
                    return default(T);
                else
                    throw;
            }

			var jsonString = Encoding.UTF8.GetString(documentFound.Data);
			var entity = ConvertToEntity<T>(id, jsonString);
			entities.Add(entity);
			return (T) entity;
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

        public void StoreAll<T>(IEnumerable<T> entities)
        {
            foreach (var entity in entities)
            {
                Store(entity);
            }
        }

		public void Store<T>(T entity)
		{
			StoreEntity(entity);
			entities.Add(entity);

			var stored = Stored;
			if (stored != null)
                stored(entity);
		}

		private void StoreEntity<T>(T entity)
		{
			var json = ConvertEntityToJson(entity);
			var identityProperty = entity.GetType().GetProperties()
				.FirstOrDefault(q => documentStore.Conventions.FindIdentityProperty.Invoke(q));

			if(identityProperty == null)
				throw new InvalidOperationException("Could not find id proeprty for " + typeof (T).Name);

			var key = (string) identityProperty.GetValue(entity, null);
			key = database.Put(key, null, json, new JObject());

			identityProperty.SetValue(entity, key, null);
		}

		public void SaveChanges()
		{
            //I don't really understand what the point of this is, given that store sends
            //info to the server and this resends it.. wouldn't that duplicate it?
            foreach (var entity in entities)
			{
				//TODO: Switch to more the batch version when it becomes available#
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

		public IQueryable<T> Query<T>()
		{
			// Todo implement Linq to Lucene here instead of the horrible list all below.
			//return GetAll<T>().AsQueryable();
			throw new InvalidOperationException();
		}

        #region IDisposable Members

        public void Dispose()
        {
            //DocumentStore owns IDatabaseCommands, allow it to dispose in case multiple sessions in play

            //dereference all event listeners
            Stored = null;
        }

        #endregion

    }
}