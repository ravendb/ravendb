using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Raven.Client.UniqueConstraints;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Xunit;

namespace Raven.Tests.Bundles.UniqueConstraints.Bugs
{
    class MyCustomDocumentSession : IDocumentSession
    {
        IDocumentSession _inner;

        public MyCustomDocumentSession(IDocumentSession ravendb)
        {
            _inner = ravendb;
        }

        public ISyncAdvancedSessionOperation Advanced
        {
            get
            {
                return _inner.Advanced;
            }
        }

        public void Delete(string id)
        {
            _inner.Delete(id);
        }

        public void Delete<T>(ValueType id)
        {
            _inner.Delete<T>(id);
        }

        public void Delete<T>(T entity)
        {
            _inner.Delete<T>(entity);
        }

        public void Dispose()
        {
            _inner.Dispose();
        }

        public ILoaderWithInclude<object> Include(string path)
        {
            return _inner.Include(path);
        }

        public ILoaderWithInclude<T> Include<T>(Expression<Func<T, object>> path)
        {
            return _inner.Include<T>(path);
        }

        public ILoaderWithInclude<T> Include<T, TInclude>(Expression<Func<T, object>> path)
        {
            return _inner.Include<T, TInclude>(path);
        }

        public T[] Load<T>(IEnumerable<ValueType> ids)
        {
            return _inner.Load<T>(ids);
        }

        public T[] Load<T>(params ValueType[] ids)
        {
            return _inner.Load<T>(ids);
        }

        public T Load<T>(ValueType id)
        {
            return _inner.Load<T>(id);
        }

        public T[] Load<T>(IEnumerable<string> ids)
        {
            return _inner.Load<T>(ids);
        }

        public T Load<T>(string id)
        {
            return _inner.Load<T>(id);
        }

        public TResult[] Load<TResult>(IEnumerable<string> ids, Type transformerType, Action<ILoadConfiguration> configure = null)
        {
            return _inner.Load<TResult>(ids, transformerType, configure);
        }

        public TResult[] Load<TResult>(IEnumerable<string> ids, string transformer, Action<ILoadConfiguration> configure = null)
        {
            return _inner.Load<TResult>(ids, transformer, configure);
        }

        public TResult Load<TResult>(string id, Type transformerType, Action<ILoadConfiguration> configure = null)
        {
            return _inner.Load<TResult>(id, transformerType, configure);
        }

        public TResult Load<TResult>(string id, string transformer, Action<ILoadConfiguration> configure)
        {
            return _inner.Load<TResult>(id, transformer, configure);
        }

        public TResult[] Load<TTransformer, TResult>(IEnumerable<string> ids, Action<ILoadConfiguration> configure = null) where TTransformer : AbstractTransformerCreationTask, new()
        {
            return _inner.Load<TTransformer, TResult>(ids, configure);
        }

        public TResult Load<TTransformer, TResult>(string id, Action<ILoadConfiguration> configure = null) where TTransformer : AbstractTransformerCreationTask, new()
        {
            return _inner.Load<TTransformer, TResult>(id, configure);
        }

        public IRavenQueryable<T> Query<T>()
        {
            return _inner.Query<T>();
        }

        public IRavenQueryable<T> Query<T>(string indexName, bool isMapReduce = false)
        {
            return _inner.Query<T>(indexName, isMapReduce);
        }

        public IRavenQueryable<T> Query<T, TIndexCreator>() where TIndexCreator : AbstractIndexCreationTask, new()
        {
            return _inner.Query<T, TIndexCreator>();
        }

        public void SaveChanges()
        {
            // Do something

            _inner.SaveChanges();

            // do something else
        }

        public void Store(object entity)
        {
            _inner.Store(entity);
        }

        public void Store(object entity, string id)
        {
            _inner.Store(entity, id);
        }

        public void Store(object entity, Etag etag)
        {
            _inner.Store(entity, etag);
        }

        public void Store(object entity, Etag etag, string id)
        {
            _inner.Store(entity, etag, id);
        }
    }
}
