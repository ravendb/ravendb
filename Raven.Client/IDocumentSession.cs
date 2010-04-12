using System;

namespace Raven.Client
{
    public interface IDocumentSession : IDisposable
    {
        string StoreIdentifier { get; }
        
		T Load<T>(string id);
        
		System.Linq.IQueryable<T> Query<T>();
        
		void SaveChanges();
        
		void Store<T>(T entity);

    	void Evict<T>(T entity);

    	void Clear();

		event Action<object> Stored;
    }
}
