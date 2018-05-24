using System;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;

namespace Raven.Client.Documents.Session
{
    public partial class AsyncDocumentSession
    {
        public IRavenQueryable<T> Query<T, TIndexCreator>() where TIndexCreator : AbstractIndexCreationTask, new()
        {
            var indexCreator = new TIndexCreator();
            return Query<T>(indexCreator.IndexName, null, indexCreator.IsMapReduce);
        }

        public IRavenQueryable<T> Query<T>(string indexName = null, string collectionName = null, bool isMapReduce = false)
        {
            var type = typeof(T);
            (indexName, collectionName) = ProcessQueryParameters(type, indexName, collectionName, Conventions);

            var queryStatistics = new QueryStatistics();
#if FEATURE_HIGHLIGHTING
            var highlightings = new QueryHighlightings();
#endif
            var ravenQueryInspector = new RavenQueryInspector<T>();
            var ravenQueryProvider = new RavenQueryProvider<T>(
                this,
                indexName,
                collectionName,
                type,
                queryStatistics,
#if FEATURE_HIGHLIGHTING
                highlightings,
#endif
                isMapReduce,
                Conventions);

            ravenQueryInspector.Init(ravenQueryProvider,
                queryStatistics,
#if FEATURE_HIGHLIGHTING
                highlightings,
#endif
                indexName,
                collectionName,
                null,
                this,
                isMapReduce);

            return ravenQueryInspector;
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

            return AsyncDocumentQuery<T>(index.IndexName, null, index.IsMapReduce);
        }

        /// <summary>
        ///     Query the specified index using Lucene syntax
        /// </summary>
        public IAsyncDocumentQuery<T> AsyncDocumentQuery<T>(string indexName = null, string collectionName = null, bool isMapReduce = false)
        {
            (indexName, collectionName) = ProcessQueryParameters(typeof(T), indexName, collectionName, Conventions);

            return new AsyncDocumentQuery<T>(this, indexName, collectionName, isGroupBy: isMapReduce);
        }

        public RavenQueryInspector<S> CreateRavenQueryInspector<S>()
        {
            return new RavenQueryInspector<S>();
        }

        /// <summary>
        /// Create a new query for <typeparam name="T"/>
        /// </summary>
        IDocumentQuery<T> IDocumentQueryGenerator.Query<T>(string indexName, string collectionName, bool isMapReduce)
        {
            throw new NotSupportedException("You can't query sync from an async session");
        }

        /// <summary>
        /// Create a new query for <typeparam name="T"/>
        /// </summary>
        IAsyncDocumentQuery<T> IDocumentQueryGenerator.AsyncQuery<T>(string indexName, string collectionName, bool isMapReduce)
        {
            return AsyncDocumentQuery<T>(indexName, collectionName, isMapReduce);
        }
    }
}
