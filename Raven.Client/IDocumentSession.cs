using System;

namespace Raven.Client
{
    public interface IDocumentSession : IDisposable
    {
        string StoreIdentifier { get; }
        
		T Load<T>(string id);

		IDocumentQuery<T> Query<T>(string indexName);
        
		void SaveChanges();
        
		void Store<T>(T entity);

    	void Evict<T>(T entity);

    	void Clear();

		event Action<object> Stored;
    }
}
