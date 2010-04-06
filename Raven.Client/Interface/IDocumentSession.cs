using System;
namespace Raven.Client
{
    public interface IDocumentSession
    {
        System.Collections.Generic.IList<T> GetAll<T>();
        T Load<T>(string id);
        System.Linq.IQueryable<T> Query<T>();
        void SaveChanges();
        void Store<T>(T entity);
    }
}
