using System.Collections.Generic;
using System.Linq;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Database;

namespace Raven.Client
{
    public class DocumentSession
    {
        private readonly DocumentStore documentStore;
        private readonly IDatabaseCommands database;
        private readonly HashSet<object> trackedEntities = new HashSet<object>();

        public DocumentSession(DocumentStore documentStore, IDatabaseCommands database)
        {
            this.documentStore = documentStore;
            this.database = database;
        }

        public T Load<T>(string id)
        {
            var documentFound = database.Get(id);
            var jsonString = Encoding.UTF8.GetString(documentFound.Data);
            var entity = ConvertToEntity<T>(id, jsonString);
            trackedEntities.Add(entity);
            return (T)entity;
        }

        private object ConvertToEntity<T>(string id, string documentFound)
        {
            var entity = JsonConvert.DeserializeObject(documentFound, typeof(T));

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
            string id;
            var json = ConvertEntityToJson(entity, out id);

            var key = database.Put(id, json, new JObject());
            trackedEntities.Add(entity);

            var identityProperty = entity.GetType().GetProperties()
                .FirstOrDefault(q => documentStore.Conventions.FindIdentityProperty.Invoke(q));
            
            if (identityProperty != null)
                identityProperty.SetValue(entity, key, null);
        }

        public void SaveChanges()
        {
            foreach (var entity in trackedEntities)
            {
                //TODO: Switch to more the batch version when it becomes available
                string id;
                var entityAsJson = ConvertEntityToJson(entity,out id);
                database.Put(id, entityAsJson, new JObject());
            }
        }

        private JObject ConvertEntityToJson(object entity, out string id)
        {
            var identityProperty = entity.GetType().GetProperties()
                .FirstOrDefault(q => documentStore.Conventions.FindIdentityProperty.Invoke(q));
            id = null;
            var objectAsJson = JObject.FromObject(entity);
            if (identityProperty != null)
            {
                objectAsJson.Remove(identityProperty.Name);
                id = (string) identityProperty.GetValue(entity, null);
            }

            objectAsJson.Add("type", JToken.FromObject(entity.GetType().FullName));
            return objectAsJson;
        }

        public IQueryable<T> Query<T>()
        {
            // Todo implement Linq to Lucene here instead of the horrible list all below.
            return GetAll<T>().AsQueryable();
        }

        public IList<T> GetAll<T>() // NOTE: We probably need to ask the user if they can accept stale results
        {
            QueryResult result;
            do
            {
                result = database.Query("getByType", "type:" + typeof(T), 0, int.MaxValue); // To be replaced with real paging
            } while (result.IsStale);

            return result.Results.Select(q =>
            {
                var entity = JsonConvert.DeserializeObject(q.ToString(), typeof(T));
                var id = q.Value<string>("_id");
                ConvertToEntity<T>(id, q.ToString());
                return (T)entity;
            }).ToList();
        }
    }
}