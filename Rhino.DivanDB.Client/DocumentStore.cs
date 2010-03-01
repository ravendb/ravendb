using System;
using System.Collections.Generic;
using System.Reflection;

namespace Rhino.DivanDB.Client
{
    public class DocumentStore : IDisposable
    {
        private List<DocumentMap> maps = new List<DocumentMap>();
        public string Database { get; set; }

        public DocumentMap GetMap<T>()
        {
            return GetMap(typeof (T));
        }

        public DocumentSession OpenSession()
        {
            return new DocumentSession(this, database);
        }

        public void Dispose()
        {
            database.Dispose();
        }

        public void Initialise()
        {
            database = new DocumentDatabase(Database);
            database.SpinBackgroundWorkers();
            database.PutIndex("getByType", "from entity in docs select new { entity.type };");
        }

        private DocumentDatabase database;

        public DocumentMap GetMap(Type type)
        {
            foreach (var map in maps)
            {
                if (map.Type == type)
                    return map;
            }
            return null;
        }

        public void Delete(Guid id)
        {
            database.Delete(id.ToString());
        }

        public void MapAggregate<T>(Func<DocumentMap, object> func)
        {
            var documentMap = new DocumentMap();
            documentMap.Type = typeof (T);
            func(documentMap);
            maps.Add(documentMap);
        }
    }

    public class DocumentConvention
    {
    }

    public class DocumentMap
    {
        public Type Type { get; set; }
        public PropertyInfo IdentityProperty { get; set; }
    }
}