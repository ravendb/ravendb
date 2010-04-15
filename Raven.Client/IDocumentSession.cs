using System;

namespace Raven.Client
{
    public interface IDocumentSession : IDisposable
    {
        string StoreIdentifier { get; }
        
		T Load<T>(string id);

        T[] Load<T>(params string[] ids);

        void Delete<T>(T entity);

		IDocumentQuery<T> Query<T>(string indexName);
        
		void SaveChanges();
        
		void Store<T>(T entity);

    	void Evict<T>(T entity);

    	void Clear();

        bool UseOptimisticConcurrency { get; set; }

        void Commit(Guid txId);
 
        void Rollback(Guid txId);
 
		event Action<object> Stored;
    }
}
