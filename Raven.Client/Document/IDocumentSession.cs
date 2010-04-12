using System;

namespace Raven.Client.Document
{
    public interface IDocumentSession : IDisposable
    {
        string StoreIdentifier { get; }
        void StoreAll<T>(System.Collections.Generic.IEnumerable<T> entities);
        T Load<T>(string id);
        System.Linq.IQueryable<T> Query<T>();
        void SaveChanges();
        void Store<T>(T entity);
        event Action<object> Stored;
    }
}
