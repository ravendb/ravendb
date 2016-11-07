//-----------------------------------------------------------------------
// <copyright file="IAsyncAdvancedSessionOperations.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Util;
using Raven.Client.Connection;
using Raven.Client.Data;
using Raven.Client.Data.Queries;
using Raven.Client.Indexes;
using Raven.Json.Linq;

namespace Raven.Client.Documents
{
    /// <summary>
    ///     Advanced async session operations
    /// </summary>
    public interface IAsyncAdvancedSessionOperations : IAdvancedDocumentSessionOperations
    {
        /// <summary>
        ///     Access the eager operations
        /// </summary>
        IAsyncEagerSessionOperations Eagerly { get; }

        /// <summary>
        ///     Access the lazy operations
        /// </summary>
        IAsyncLazySessionOperations Lazily { get; }

        /// <summary>
        ///     Queries the index specified by <typeparamref name="TIndexCreator" /> using lucene syntax.
        /// </summary>
        /// <typeparam name="T">The result of the query</typeparam>
        /// <typeparam name="TIndexCreator">The type of the index creator.</typeparam>
        IAsyncDocumentQuery<T> AsyncDocumentQuery<T, TIndexCreator>() where TIndexCreator : AbstractIndexCreationTask, new();

        /// <summary>
        ///     Query the specified index using Lucene syntax
        /// </summary>
        /// <param name="index">Name of the index.</param>
        /// <param name="isMapReduce">Control how we treat identifier properties in map/reduce indexes</param>
        IAsyncDocumentQuery<T> AsyncDocumentQuery<T>(string index, bool isMapReduce = false);

        /// <summary>
        ///     Dynamically query RavenDB using Lucene syntax
        /// </summary>
        IAsyncDocumentQuery<T> AsyncDocumentQuery<T>();

        /// <summary>
        ///     Queries the index specified by <typeparamref name="TIndexCreator" /> using lucene syntax.
        /// </summary>
        /// <typeparam name="T">The result of the query</typeparam>
        /// <typeparam name="TIndexCreator">The type of the index creator.</typeparam>
        /// <returns></returns>
        [Obsolete("Use AsyncDocumentQuery instead")]
        IAsyncDocumentQuery<T> AsyncLuceneQuery<T, TIndexCreator>() where TIndexCreator : AbstractIndexCreationTask, new();

        /// <summary>
        ///     Query the specified index using Lucene syntax
        /// </summary>
        /// <param name="index">Name of the index.</param>
        /// <param name="isMapReduce">Control how we treat identifier properties in map/reduce indexes</param>
        [Obsolete("Use AsyncDocumentQuery instead")]
        IAsyncDocumentQuery<T> AsyncLuceneQuery<T>(string index, bool isMapReduce = false);

        /// <summary>
        ///     Dynamically query RavenDB using Lucene syntax
        /// </summary>
        [Obsolete("Use AsyncDocumentQuery instead")]
        IAsyncDocumentQuery<T> AsyncLuceneQuery<T>();

        /// <summary>
        ///     Sends a multiple faceted queries in a single request and calculates the facet results for each of them
        /// </summary>
        /// <param name="queries">Array of the faceted queries that will be executed on the server-side</param>
        Task<FacetedQueryResult[]> MultiFacetedSearchAsync(params FacetQuery[] queries);

        /// <summary>
        ///     Returns full document url for a given entity
        /// </summary>
        /// <param name="entity">Instance of an entity for which url will be returned</param>
        string GetDocumentUrl(object entity);

        /// <summary>
        ///     Loads multiple entities that contain common prefix.
        /// </summary>
        /// <param name="keyPrefix">prefix for which documents should be returned e.g. "products/"</param>
        /// <param name="matches">
        ///     pipe ('|') separated values for which document keys (after 'keyPrefix') should be matched ('?'
        ///     any single character, '*' any characters)
        /// </param>
        /// <param name="start">number of documents that should be skipped. By default: 0.</param>
        /// <param name="pageSize">maximum number of documents that will be retrieved. By default: 25.</param>
        /// <param name="pagingInformation">used to perform rapid pagination on a server side</param>
        /// <param name="exclude">
        ///     pipe ('|') separated values for which document keys (after 'keyPrefix') should not be matched
        ///     ('?' any single character, '*' any characters)
        /// </param>
        /// <param name="skipAfter">
        ///     skip document fetching until given key is found and return documents after that key (default:
        ///     null)
        /// </param>
        /// <param name="token">The cancellation token.</param>
        Task<IEnumerable<T>> LoadStartingWithAsync<T>(string keyPrefix, string matches = null, int start = 0, int pageSize = 25, string exclude = null, RavenPagingInformation pagingInformation = null, string skipAfter = null, CancellationToken token = default (CancellationToken));

        /// <summary>
        ///     Loads multiple entities that contain common prefix and applies specified transformer.
        /// </summary>
        /// <param name="keyPrefix">prefix for which documents should be returned e.g. "products/"</param>
        /// <param name="matches">
        ///     pipe ('|') separated values for which document keys (after 'keyPrefix') should be matched ('?'
        ///     any single character, '*' any characters)
        /// </param>
        /// <param name="start">number of documents that should be skipped. By default: 0.</param>
        /// <param name="pageSize">maximum number of documents that will be retrieved. By default: 25.</param>
        /// <param name="pagingInformation">used to perform rapid pagination on a server side</param>
        /// <param name="exclude">
        ///     pipe ('|') separated values for which document keys (after 'keyPrefix') should not be matched
        ///     ('?' any single character, '*' any characters)
        /// </param>
        /// <param name="configure">additional configuration options for operation e.g. AddTransformerParameter</param>
        /// <param name="skipAfter">
        ///     skip document fetching until given key is found and return documents after that key (default:
        ///     null)
        /// </param>
        /// <param name="token">The cancellation token.</param>
        Task<IEnumerable<TResult>> LoadStartingWithAsync<TTransformer, TResult>(string keyPrefix, string matches = null, int start = 0, int pageSize = 25, string exclude = null, RavenPagingInformation pagingInformation = null, Action<ILoadConfiguration> configure = null, string skipAfter = null, CancellationToken token = default (CancellationToken)) where TTransformer : AbstractTransformerCreationTask, new();

        /// <summary>
        ///     Updates entity with latest changes from server
        /// </summary>
        /// <param name="entity">Instance of an entity that will be refreshed</param>
        /// <param name="token">The cancellation token.</param>
        Task RefreshAsync<T>(T entity, CancellationToken token = default (CancellationToken));

        /// <summary>
        ///     Stores entity in session, extracts Id from entity using Conventions or generates new one if it is not available and
        ///     forces concurrency check with given Etag
        /// </summary>
        void Store(object entity, long? etag);

        /// <summary>
        ///     Stores entity in session with given id and forces concurrency check with given Etag.
        /// </summary>
        void Store(object entity, long? etag, string id);

        /// <summary>
        ///     Stores entity in session, extracts Id from entity using Conventions or generates new one if it is not available.
        ///     <para>Forces concurrency check if the Id is not available during extraction.</para>
        /// </summary>
        /// <param name="entity">entity to store.</param>
        void Store(object entity);

        /// <summary>
        ///     Stores the specified dynamic entity, under the specified id.
        /// </summary>
        /// <param name="entity">entity to store.</param>
        /// <param name="id">Id to store this entity under. If other entity exists with the same id it will be overridden.</param>
        void Store(object entity, string id);

        /// <summary>
        ///     Stream the results on the query to the client, converting them to
        ///     CLR types along the way.
        ///     <para>Does NOT track the entities in the session, and will not includes changes there when SaveChanges() is called</para>
        /// </summary>
        /// <param name="query">Query to stream results for</param>
        /// <param name="token">The cancellation token.</param>
        Task<IAsyncEnumerator<StreamResult<T>>> StreamAsync<T>(IAsyncDocumentQuery<T> query, CancellationToken token = default (CancellationToken));

        /// <summary>
        ///     Stream the results on the query to the client, converting them to
        ///     CLR types along the way.
        ///     <para>Does NOT track the entities in the session, and will not includes changes there when SaveChanges() is called</para>
        /// </summary>
        /// <param name="query">Query to stream results for</param>
        /// <param name="token">The cancellation token.</param>
        Task<IAsyncEnumerator<StreamResult<T>>> StreamAsync<T>(IQueryable<T> query, CancellationToken token = default (CancellationToken));

        /// <summary>
        ///     Stream the results on the query to the client, converting them to
        ///     CLR types along the way.
        ///     <para>Does NOT track the entities in the session, and will not includes changes there when SaveChanges() is called</para>
        /// </summary>
        /// <param name="query">Query to stream results for</param>
        /// <param name="queryHeaderInformation">Information about performed query</param>
        /// <param name="token">The cancellation token.</param>
        Task<IAsyncEnumerator<StreamResult<T>>> StreamAsync<T>(IAsyncDocumentQuery<T> query, Reference<QueryHeaderInformation> queryHeaderInformation, CancellationToken token = default (CancellationToken));

        /// <summary>
        ///     Stream the results on the query to the client, converting them to
        ///     CLR types along the way.
        ///     <para>Does NOT track the entities in the session, and will not includes changes there when SaveChanges() is called</para>
        /// </summary>
        /// <param name="query">Query to stream results for</param>
        /// <param name="queryHeaderInformation">Information about performed query</param>
        /// <param name="token">The cancellation token.</param>
        Task<IAsyncEnumerator<StreamResult<T>>> StreamAsync<T>(IQueryable<T> query, Reference<QueryHeaderInformation> queryHeaderInformation, CancellationToken token = default (CancellationToken));

        /// <summary>
        ///     Stream the results of documents search to the client, converting them to CLR types along the way.
        ///     <para>Does NOT track the entities in the session, and will not includes changes there when SaveChanges() is called</para>
        /// </summary>
        /// <param name="fromEtag">ETag of a document from which stream should start</param>
        /// <param name="start">number of documents that should be skipped</param>
        /// <param name="pageSize">maximum number of documents that will be retrieved</param>
        /// <param name="pagingInformation">used to perform rapid pagination on a server side</param>
        /// <param name="token">The cancellation token.</param>
        /// <param name="transformer">name of a transformer that should be used to transform the results</param>
        /// <param name="transformerParameters">parameters that will be passed to transformer</param>
        Task<IAsyncEnumerator<StreamResult<T>>> StreamAsync<T>(long? fromEtag, int start = 0, int pageSize = int.MaxValue, RavenPagingInformation pagingInformation = null, string transformer = null, Dictionary<string, RavenJToken> transformerParameters = null, CancellationToken token = default (CancellationToken));

        /// <summary>
        ///     Stream the results of documents search to the client, converting them to CLR types along the way.
        ///     <para>Does NOT track the entities in the session, and will not includes changes there when SaveChanges() is called</para>
        /// </summary>
        /// <param name="startsWith">prefix for which documents should be returned e.g. "products/"</param>
        /// <param name="matches">
        ///     pipe ('|') separated values for which document keys (after 'keyPrefix') should be matched ('?'
        ///     any single character, '*' any characters)
        /// </param>
        /// <param name="start">number of documents that should be skipped</param>
        /// <param name="pageSize">maximum number of documents that will be retrieved</param>
        /// <param name="pagingInformation">used to perform rapid pagination on a server side</param>
        /// <param name="skipAfter">
        ///     skip document fetching until given key is found and return documents after that key (default:
        ///     null)
        /// </param>
        /// <param name="token">The cancellation token.</param>
        /// <param name="transformer">name of a transformer that should be used to transform the results</param>
        /// <param name="transformerParameters">parameters that will be passed to transformer</param>
        Task<IAsyncEnumerator<StreamResult<T>>> StreamAsync<T>(string startsWith, string matches = null, int start = 0, int pageSize = int.MaxValue, RavenPagingInformation pagingInformation = null, string skipAfter = null, string transformer = null, Dictionary<string, RavenJToken> transformerParameters = null, CancellationToken token = default (CancellationToken));

        /// <summary>
        ///     Gets the metadata for the specified entity.
        /// </summary>
        /// <param name="instance">The instance.</param>
        IDictionary<string, string> GetMetadataForAsync<T>(T instance);

        /// <summary>
        ///     DeleteByIndexAsync using linq expression
        /// </summary>
        /// <param name="expression">The linq expression</param>
        Task<Operation> DeleteByIndexAsync<T, TIndexCreator>(Expression<Func<T, bool>> expression) where TIndexCreator : AbstractIndexCreationTask, new();

        /// <summary>
        ///     DeleteByIndexAsync using linq expression
        /// </summary>
        /// <param name="indexName">Index string name</param>
        /// <param name="expression">The linq expression</param>
        Task<Operation> DeleteByIndexAsync<T>(string indexName, Expression<Func<T, bool>> expression);
    }
}
