using System;

namespace Raven.Client
{
    public interface IDocumentSession : IDisposable
    {
        string StoreIdentifier { get; }
        
		T Load<T>(string id);

        void Delete<T>(T entity);

		IDocumentQuery<T> Query<T>(string indexName);
        
		void SaveChanges();
        
		void Store<T>(T entity);

    	void Evict<T>(T entity);

    	void Clear();

        bool UseOptimisticConcurrency { get; set; }

		event Action<object> Stored;
    }
}
