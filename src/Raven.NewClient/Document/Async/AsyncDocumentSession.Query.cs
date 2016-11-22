using System;
using Raven.NewClient.Client.Indexes;
using Raven.NewClient.Client.Linq;

namespace Raven.NewClient.Client.Document.Async
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

        /// <summary>
        /// Create a new query for <typeparam name="T"/>
        /// </summary>
        public IAsyncDocumentQuery<T> AsyncQuery<T>(string indexName, bool isMapReduce = false)
        {
            return AsyncDocumentQuery<T>(indexName, isMapReduce);
        }

        /// <summary>
        /// Queries the index specified by <typeparamref name="TIndexCreator"/> using lucene syntax.
        /// </summary>
        /// <typeparam name="T">The result of the query</typeparam>
        /// <typeparam name="TIndexCreator">The type of the index creator.</typeparam>
        /// <returns></returns>
        public IAsyncDocumentQuery<T> AsyncDocumentQuery<T, TIndexCreator>() where TIndexCreator : AbstractIndexCreationTask, new()
        {
            var index = new TIndexCreator();

            return AsyncDocumentQuery<T>(index.IndexName, index.IsMapReduce);
        }

        /// <summary>
        /// Query the specified index using Lucene syntax
        /// </summary>
        public IAsyncDocumentQuery<T> AsyncDocumentQuery<T>(string index, bool isMapReduce)
        {
            return new AsyncDocumentQuery<T>(this, null, AsyncDatabaseCommands, index, new string[0], new string[0], isMapReduce);
        }

        /// <summary>
        /// Dynamically query RavenDB using Lucene syntax
        /// </summary>
        public IAsyncDocumentQuery<T> AsyncDocumentQuery<T>()
        {
            var indexName = CreateDynamicIndexName<T>();

            return new AsyncDocumentQuery<T>(this, null, AsyncDatabaseCommands, indexName, new string[0], new string[0], false);
        }

        /// <summary>
        /// Queries the index specified by <typeparamref name="TIndexCreator"/> using lucene syntax.
        /// </summary>
        /// <typeparam name="T">The result of the query</typeparam>
        /// <typeparam name="TIndexCreator">The type of the index creator.</typeparam>
        /// <returns></returns>
        [Obsolete("Use AsyncDocumentQuery instead")]
        public IAsyncDocumentQuery<T> AsyncLuceneQuery<T, TIndexCreator>() where TIndexCreator : AbstractIndexCreationTask, new()
        {
            return AsyncDocumentQuery<T, TIndexCreator>();
        }

        /// <summary>
        /// Query the specified index using Lucene syntax
        /// </summary>
        [Obsolete("Use AsyncDocumentQuery instead.")]
        public IAsyncDocumentQuery<T> AsyncLuceneQuery<T>(string index, bool isMapReduce)
        {
            return AsyncDocumentQuery<T>(index, isMapReduce);
        }

        /// <summary>
        /// Dynamically query RavenDB using Lucene syntax
        /// </summary>
        [Obsolete("Use AsyncDocumentQuery instead.")]
        public IAsyncDocumentQuery<T> AsyncLuceneQuery<T>()
        {
            return AsyncDocumentQuery<T>();
        }

        public RavenQueryInspector<S> CreateRavenQueryInspector<S>()
        {
            return new RavenQueryInspector<S>();
        }

        /// <summary>
        /// Create a new query for <typeparam name="T"/>
        /// </summary>
        IDocumentQuery<T> IDocumentQueryGenerator.Query<T>(string indexName, bool isMapReduce)
        {
            throw new NotSupportedException("You can't query sync from an async session");
        }
    }
}