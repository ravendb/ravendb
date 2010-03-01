using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Newtonsoft.Json;
using JObject=Newtonsoft.Json.Linq.JObject;
using JToken=Newtonsoft.Json.Linq.JToken;

namespace Rhino.DivanDB.Client
{
    public class DocumentSession
    {
        private readonly DocumentStore documentDb;
        private readonly DocumentDatabase database;
        private ArrayList entities = new ArrayList();

        public DocumentSession(DocumentStore documentDb, DocumentDatabase database)
        {
            this.documentDb = documentDb;
            this.database = database;
        }

        public T Load<T>(string id)
        {
            var documentFound = database.Get(id);
            var jsonString = Encoding.UTF8.GetString(documentFound);
            var entity = convertToEntity<T>(id, jsonString);
            entities.Add(entity);
            return (T)entity;
        }

        private object convertToEntity<T>(string id, string documentFound)
        {
            var entity = JsonConvert.DeserializeObject(documentFound, typeof(T));
            var map = documentDb.GetMap<T>();
            map.IdentityProperty.SetValue(entity, id, null);
            return entity;
        }

        public void Store<T>(T entity)
        {
            var json = convertEntityToJson(entity);

            var key = database.Put(json);
            entities.Add(entity);

            var map = documentDb.GetMap<T>();
            map.IdentityProperty.SetValue(entity, key, null);
        }

        public void SaveChanges()
        {
            foreach (var entity in entities)
            {
                JObject objectAsJson = convertEntityToJson(entity);
                database.Put(objectAsJson);
            }
        }

        private JObject convertEntityToJson(object entity)
        {
            var map = documentDb.GetMap(entity.GetType());
            var value = map.IdentityProperty.GetValue(entity, null);
                
            var objectAsJson = JObject.FromObject(entity);
            objectAsJson.Remove(map.IdentityProperty.Name);
            objectAsJson.Add("_id", JToken.FromObject(value));
            objectAsJson.Add("type", JToken.FromObject(entity.GetType()));
            return objectAsJson;
        }

        public IQueryable<T> Query<T>()
        {
            // Todo implement Linq to document here instead of the horrible list all below.
            return GetAll<T>().AsQueryable();
        }

        public IList<T> GetAll<T>()
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
                convertToEntity<T>(id, q.ToString());
                return (T)entity;
            }).ToList();
        }
    }
}