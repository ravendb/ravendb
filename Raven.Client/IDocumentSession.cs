using System;
using System.Linq;
using Newtonsoft.Json.Linq;
using Raven.Client.Client;
using Raven.Client.Document;
using Raven.Client.Linq;

namespace Raven.Client
{
    public interface IDocumentSession : IDisposable
    {

        string StoreIdentifier { get; }
        
		T Load<T>(string id);

        T[] Load<T>(params string[] ids);

        void Delete<T>(T entity);

        IRavenQueryable<T> Query<T>(string indexName);

		IDocumentQuery<T> LuceneQuery<T>(string indexName);
        
		void SaveChanges();
        
		void Store<T>(T entity);

        void Refresh<T>(T entity);

    	void Evict<T>(T entity);

    	void Clear();

        bool UseOptimisticConcurrency { get; set; }

    	DocumentConvention Conventions { get; }

    	void Commit(Guid txId);
 
        void Rollback(Guid txId);

        int MaxNumberOfRequestsPerSession { get; set; }

        event EntityStored Stored;

        event EntityToDocument OnEntityConverted;

        JObject GetMetadataFor<T>(T instance);
    }

    public delegate void EntityStored(object entity);

    public delegate void EntityToDocument(object entity, JObject document, JObject metadata);
}
