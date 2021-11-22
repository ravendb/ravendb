//-----------------------------------------------------------------------
// <copyright file="DocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Queries.Highlighting;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    /// Implements Unit of Work for accessing the RavenDB server
    /// </summary>
    public partial class DocumentSession
    {
        
        
        /// <summary>
        /// Queries the index specified by <typeparamref name="TIndexCreator"/> using lucene syntax.
        /// </summary>
        /// <typeparam name="T">The result of the query</typeparam>
        /// <typeparam name="TIndexCreator">The type of the index creator.</typeparam>
        /// <returns></returns>
        public IDocumentQuery<T> DocumentQuery<T, TIndexCreator>() where TIndexCreator : AbstractCommonApiForIndexes, new()
        {
            var index = IndexMetadataCache.GetIndexMetadataCacheItem<TIndexCreator>();
            
            return DocumentQuery<T>(index.IndexName, null, index.IsMapReduce);
        }

        /// <summary>
        /// Query the specified index using Lucene syntax
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="indexName">Name of the index (mutually exclusive with collectionName)</param>
        /// <param name="collectionName">Name of the collection (mutually exclusive with indexName)</param>
        /// <param name="isMapReduce">Whether we are querying a map/reduce index (modify how we treat identifier properties)</param>
        public IDocumentQuery<T> DocumentQuery<T>(string indexName = null, string collectionName = null, bool isMapReduce = false)
        {
            (indexName, collectionName) = ProcessQueryParameters(typeof(T), indexName, collectionName, Conventions);

            return new DocumentQuery<T>(this, indexName, collectionName, isGroupBy: isMapReduce);
        }

        /// <summary>
        ///     Queries the specified index using Linq.
        /// </summary>
        /// <typeparam name="T">The result of the query</typeparam>
        /// <param name="indexName">Name of the index (mutually exclusive with collectionName)</param>
        /// <param name="collectionName">Name of the collection (mutually exclusive with indexName)</param>
        /// <param name="isMapReduce">Whether we are querying a map/reduce index (modify how we treat identifier properties)</param>
        public IRavenQueryable<T> Query<T>(string indexName = null, string collectionName = null, bool isMapReduce = false)
        {
            var type = typeof(T);
            (indexName, collectionName) = ProcessQueryParameters(type, indexName, collectionName, Conventions);

            var queryStatistics = new QueryStatistics();
            var highlightings = new LinqQueryHighlightings();

            var ravenQueryProvider = new RavenQueryProvider<T>(
                this,
                indexName,
                collectionName,
                type,
                queryStatistics,
                highlightings,
                isMapReduce,
                Conventions);

            var inspector = new RavenQueryInspector<T>();
            inspector.Init(
                ravenQueryProvider,
                queryStatistics,
                highlightings,
                indexName,
                collectionName,
                null,
                this,
                isMapReduce);

            return inspector;
        }

        /// <summary>
        /// Create a new query for <typeparam name="T"/>
        /// </summary>
        public IAsyncDocumentQuery<T> AsyncQuery<T>(string indexName, string collectionName, bool isMapReduce)
        {
            throw new NotSupportedException("You can't use an async query from a sync session");
        }

        public RavenQueryInspector<S> CreateRavenQueryInspector<S>()
        {
            return new RavenQueryInspector<S>();
        }

        /// <summary>
        /// Queries the index specified by <typeparamref name="TIndexCreator"/> using Linq.
        /// </summary>
        /// <typeparam name="T">The result of the query</typeparam>
        /// <typeparam name="TIndexCreator">The type of the index creator.</typeparam>
        /// <returns></returns>
        public IRavenQueryable<T> Query<T, TIndexCreator>() where TIndexCreator : AbstractCommonApiForIndexes, new()
        {
            var index = IndexMetadataCache.GetIndexMetadataCacheItem<TIndexCreator>();
            
            return Query<T>(index.IndexName, null, index.IsMapReduce);
        }

        /// <summary>
        /// Create a new query for <typeparam name="T"/>
        /// </summary>
        IDocumentQuery<T> IDocumentQueryGenerator.Query<T>(string indexName, string collectionName, bool isMapReduce)
        {
            return Advanced.DocumentQuery<T>(indexName, collectionName, isMapReduce);
        }

        InMemoryDocumentSessionOperations IDocumentQueryGenerator.Session { get => this; }
    }
}
