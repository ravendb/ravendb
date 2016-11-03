using System;
using Raven.Client.Indexes;

namespace Raven.Client.Documents.Async
{
    public partial class AsyncDocumentSession
    {
        /// <summary>
        /// Dynamically queries RavenDB using LINQ
        /// </summary>
        /// <typeparam name="T">The result of the query</typeparam>
        public IRavenQueryable<T> Query<T>()
        {
            string indexName = CreateDynamicIndexName<T>();

            return Query<T>(indexName);
        }

        public IRavenQueryable<T> Query<T, TIndexCreator>() where TIndexCreator : AbstractIndexCreationTask, new()
        {
            var indexCreator = new TIndexCreator();
            return Query<T>(indexCreator.IndexName, indexCreator.IsMapReduce);
        }

        public IRavenQueryable<T> Query<T>(string indexName, bool isMapReduce = false)
        {
            var ravenQueryStatistics = new RavenQueryStatistics();
            var highlightings = new RavenQueryHighlightings();
            var ravenQueryInspector = new RavenQueryInspector<T>();
            var ravenQueryProvider = new RavenQueryProvider<T>(this, indexName, ravenQueryStatistics, highlightings,
                null, AsyncDatabaseCommands, isMapReduce);
            ravenQueryInspector.Init(ravenQueryProvider,
                ravenQueryStatistics,
                highlightings,
                indexName,
                null,
                this, null, AsyncDatabaseCommands, isMapReduce);
            return ravenQueryInspector;
        }

        public IAsyncDocumentQuery<T> AsyncQuery<T>(string indexName, bool isMapReduce)
        {
            return AsyncDocumentQuery<T>(indexName, isMapReduce);
        }

        public IAsyncDocumentQuery<T> AsyncDocumentQuery<T, TIndexCreator>()
            where TIndexCreator : AbstractIndexCreationTask, new()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Query the specified index using Lucene syntax
        /// </summary>
        public IAsyncDocumentQuery<T> AsyncDocumentQuery<T>(string index, bool isMapReduce)
        {
            return new AsyncDocumentQuery<T>(this, null, AsyncDatabaseCommands, index, new string[0], new string[0],
                isMapReduce);
        }

        public IAsyncDocumentQuery<T> AsyncDocumentQuery<T>()
        {
            throw new NotImplementedException();
        }

        public IAsyncDocumentQuery<T> AsyncLuceneQuery<T, TIndexCreator>()
            where TIndexCreator : AbstractIndexCreationTask, new()
        {
            throw new NotImplementedException();
        }

        public IAsyncDocumentQuery<T> AsyncLuceneQuery<T>(string index, bool isMapReduce = false)
        {
            throw new NotImplementedException();
        }

        public IAsyncDocumentQuery<T> AsyncLuceneQuery<T>()
        {
            throw new NotImplementedException();
        }

        public RavenQueryInspector<S> CreateRavenQueryInspector<S>()
        {
            return new RavenQueryInspector<S>();
        }

        IDocumentQuery<T> IDocumentQueryGenerator.Query<T>(string indexName, bool isMapReduce)
        {
            throw new NotSupportedException("You can't query sync from an async session");
        }
    }
}