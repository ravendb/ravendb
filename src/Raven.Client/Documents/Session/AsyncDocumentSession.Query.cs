using System;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Queries.Highlighting;

namespace Raven.Client.Documents.Session
{
    public partial class AsyncDocumentSession
    {
        /// <summary>
        /// Queries the index specified by <typeparamref name="TIndexCreator" /> using Linq.
        /// </summary>
        /// <typeparam name="T">The result of the query</typeparam>
        /// <typeparam name="TIndexCreator">The type of the index creator.</typeparam>
        /// <returns>A queryable instance for the specified index</returns>
        public IRavenQueryable<T> Query<T, TIndexCreator>() where TIndexCreator : AbstractCommonApiForIndexes, new()
        {
            var index = IndexMetadataCache.GetIndexMetadataCacheItem<TIndexCreator>();
            
            return Query<T>(index.IndexName, null, index.IsMapReduce);
        }

        /// <summary>
        /// Queries the specified index using Linq.
        /// </summary>
        /// <typeparam name="T">The result of the query</typeparam>
        /// <param name="indexName">Name of the index (mutually exclusive with collectionName)</param>
        /// <param name="collectionName">Name of the collection (mutually exclusive with indexName)</param>
        /// <param name="isMapReduce">Whether we are querying a map/reduce index (modify how we treat identifier properties)</param>
        /// <returns>A queryable instance for the specified index or collection</returns>
        public IRavenQueryable<T> Query<T>(string indexName = null, string collectionName = null, bool isMapReduce = false)
        {
            var type = typeof(T);
            (indexName, collectionName) = ProcessQueryParameters(type, indexName, collectionName, Conventions);

            var queryStatistics = new QueryStatistics();
            var highlightings = new LinqQueryHighlightings();

            var ravenQueryInspector = new RavenQueryInspector<T>();
            var ravenQueryProvider = new RavenQueryProvider<T>(
                this,
                indexName,
                collectionName,
                type,
                queryStatistics,
                highlightings,
                isMapReduce,
                Conventions);

            ravenQueryInspector.Init(ravenQueryProvider,
                queryStatistics,
                highlightings,
                indexName,
                collectionName,
                null,
                this,
                isMapReduce);

            return ravenQueryInspector;
        }

        /// <inheritdoc />
        public IAsyncDocumentQuery<T> AsyncDocumentQuery<T, TIndexCreator>() where TIndexCreator : AbstractCommonApiForIndexes, new()
        {
            var index = IndexMetadataCache.GetIndexMetadataCacheItem<TIndexCreator>();
            
            return AsyncDocumentQuery<T>(index.IndexName, null, index.IsMapReduce);
        }

        /// <inheritdoc />
        public IAsyncDocumentQuery<T> AsyncDocumentQuery<T>(string indexName = null, string collectionName = null, bool isMapReduce = false)
        {
            (indexName, collectionName) = ProcessQueryParameters(typeof(T), indexName, collectionName, Conventions);

            return new AsyncDocumentQuery<T>(this, indexName, collectionName, isGroupBy: isMapReduce);
        }

        /// <summary>
        /// Creates a new Raven query inspector for the specified type
        /// </summary>
        /// <typeparam name="S">The type to create the query inspector for</typeparam>
        /// <returns>A new RavenQueryInspector instance</returns>
        public RavenQueryInspector<S> CreateRavenQueryInspector<S>()
        {
            return new RavenQueryInspector<S>();
        }

        InMemoryDocumentSessionOperations IDocumentQueryGenerator.Session { get => this; }
  
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
