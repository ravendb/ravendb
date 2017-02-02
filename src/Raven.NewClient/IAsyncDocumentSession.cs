//-----------------------------------------------------------------------
// <copyright file="IAsyncDocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Indexes;
using Raven.NewClient.Client.Linq;

namespace Raven.NewClient.Client.Document
{
    /// <summary>
    ///     Interface for document session using async approaches
    /// </summary>
    public interface IAsyncDocumentSession : IDisposable
    {
        /// <summary>
        ///     Get the accessor for advanced operations
        /// </summary>
        /// <remarks>
        ///     Those operations are rarely needed, and have been moved to a separate
        ///     property to avoid cluttering the API
        /// </remarks>
        IAsyncAdvancedSessionOperations Advanced { get; }

        /// <summary>
        ///     Marks the specified entity for deletion. The entity will be deleted when
        ///     <see cref="IDocumentSession.SaveChanges" /> is called.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="entity">instance of entity to delete</param>
        void Delete<T>(T entity);
        
        /// <summary>
        ///     Marks the specified entity for deletion. The entity will be deleted when
        ///     <see cref="IDocumentSession.SaveChanges" /> is called.
        ///     <para>WARNING: This method will not call beforeDelete listener!</para>
        /// </summary>
        /// <param name="id">entity Id</param>
        void Delete(string id);

        /// <summary>
        ///     Begin a load while including the specified path
        /// </summary>
        /// <param name="path">Path in documents in which server should look for a 'referenced' documents.</param>
        IAsyncLoaderWithInclude<object> Include(string path);

        /// <summary>
        ///     Begin a load while including the specified path
        /// </summary>
        /// <param name="path">Path in documents in which server should look for a 'referenced' documents.</param>
        IAsyncLoaderWithInclude<T> Include<T>(Expression<Func<T, string>> path);

        /// <summary>
        ///     Begin a load while including the specified path
        /// </summary>
        /// <param name="path">Path in documents in which server should look for a 'referenced' documents.</param>
        IAsyncLoaderWithInclude<T> Include<T, TInclude>(Expression<Func<T, string>> path);

        /// <summary>
        ///     Begin a load while including the specified path
        /// </summary>
        /// <param name="path">Path in documents in which server should look for a 'referenced' documents.</param>
        IAsyncLoaderWithInclude<T> Include<T>(Expression<Func<T, IEnumerable<string>>> path);

        /// <summary>
        ///     Begin a load while including the specified path
        /// </summary>
        /// <param name="path">Path in documents in which server should look for a 'referenced' documents.</param>
        IAsyncLoaderWithInclude<T> Include<T, TInclude>(Expression<Func<T, IEnumerable<string>>> path);

        /// <summary>
        ///     Loads the specified entity with the specified id.
        /// </summary>
        /// <param name="id">Identifier of a entity that will be loaded.</param>
        /// <param name="token">The cancellation token.</param>
        Task<T> LoadAsync<T>(string id, CancellationToken token = default (CancellationToken));

        /// <summary>
        ///     Loads the specified entities with the specified ids.
        /// </summary>
        /// <param name="ids">Enumerable of Ids that should be loaded</param>
        /// <param name="token">The cancellation token.</param>
        Task<Dictionary<string, T>> LoadAsync<T>(IEnumerable<string> ids, CancellationToken token = default (CancellationToken));
        
        /// <summary>
        ///     Performs a load that will use the specified results transformer against the specified id
        /// </summary>
        /// <typeparam name="TTransformer">The transformer to use in this load operation</typeparam>
        /// <typeparam name="TResult">The results shape to return after the load operation</typeparam>
        /// <param name="id">Id of a document to load</param>
        /// <param name="configure">additional configuration options for operation e.g. AddTransformerParameter</param>
        /// <param name="token">The cancellation token.</param>
        Task<TResult> LoadAsync<TTransformer, TResult>(string id, Action<ILoadConfiguration> configure = null, CancellationToken token = default (CancellationToken)) where TTransformer : AbstractTransformerCreationTask, new();

        /// <summary>
        ///     Performs a load that will use the specified results transformer against the specified id
        /// </summary>
        /// <param name="ids">Enumerable of ids of documents to load</param>
        /// <param name="configure">additional configuration options for operation e.g. AddTransformerParameter</param>
        /// <param name="token">The cancellation token.</param>
        Task<TResult[]> LoadAsync<TTransformer, TResult>(IEnumerable<string> ids, Action<ILoadConfiguration> configure = null, CancellationToken token = default (CancellationToken)) where TTransformer : AbstractTransformerCreationTask, new();

        /// <summary>
        ///     Performs a load that will use the specified results transformer against the specified id
        /// </summary>
        /// <typeparam name="TResult">The results shape to return after the load operation</typeparam>
        /// <param name="id">Id of a document to load</param>
        /// <param name="transformer">The transformer to use in this load operation</param>
        /// <param name="configure">additional configuration options for operation e.g. AddTransformerParameter</param>
        /// <param name="token">The cancellation token.</param>
        Task<TResult> LoadAsync<TResult>(string id, string transformer, Action<ILoadConfiguration> configure = null, CancellationToken token = default (CancellationToken));

        /// <summary>
        ///     Performs a load that will use the specified results transformer against the specified ids
        /// </summary>
        /// <typeparam name="TResult">The results shape to return after the load operation</typeparam>
        /// <param name="ids">Enumerable of ids of documents to load</param>
        /// <param name="transformer">The transformer to use in this load operation</param>
        /// <param name="configure">additional configuration options for operation e.g. AddTransformerParameter</param>
        /// <param name="token">The cancellation token.</param>
        Task<TResult[]> LoadAsync<TResult>(IEnumerable<string> ids, string transformer, Action<ILoadConfiguration> configure = null, CancellationToken token = default (CancellationToken));

        /// <summary>
        ///     Performs a load that will use the specified results transformer against the specified id
        /// </summary>
        /// <typeparam name="TResult">The results shape to return after the load operation</typeparam>
        /// <param name="id">Id of a entity to load</param>
        /// <param name="transformerType">The transformer to use in this load operation</param>
        /// <param name="configure">additional configuration options for operation e.g. AddTransformerParameter</param>
        /// <param name="token">The cancellation token.</param>
        Task<TResult> LoadAsync<TResult>(string id, Type transformerType, Action<ILoadConfiguration> configure = null, CancellationToken token = default (CancellationToken));

        /// <summary>
        ///     Performs a load that will use the specified results transformer against the specified ids
        /// </summary>
        /// <typeparam name="TResult">The results shape to return after the load operation</typeparam>
        /// <param name="ids">Enumerable of ids of documents to load</param>
        /// <param name="transformerType">The transformer to use in this load operation</param>
        /// <param name="configure">additional configuration options for operation e.g. AddTransformerParameter</param>
        /// <param name="token">The cancellation token.</param>
        Task<TResult[]> LoadAsync<TResult>(IEnumerable<string> ids, Type transformerType, Action<ILoadConfiguration> configure = null, CancellationToken token = default (CancellationToken));

        /// <summary>
        ///     Queries the specified index using Linq.
        /// </summary>
        /// <typeparam name="T">The result of the query</typeparam>
        /// <param name="indexName">Name of the index.</param>
        /// <param name="isMapReduce">Whatever we are querying a map/reduce index (modify how we treat identifier properties)</param>
        IRavenQueryable<T> Query<T>(string indexName, bool isMapReduce = false);

        /// <summary>
        ///     Dynamically queries RavenDB using LINQ
        /// </summary>
        /// <typeparam name="T">The result of the query</typeparam>
        IRavenQueryable<T> Query<T>();

        /// <summary>
        ///     Queries the index specified by <typeparamref name="TIndexCreator" /> using Linq.
        /// </summary>
        /// <typeparam name="T">The result of the query</typeparam>
        /// <typeparam name="TIndexCreator">The type of the index creator.</typeparam>
        IRavenQueryable<T> Query<T, TIndexCreator>() where TIndexCreator : AbstractIndexCreationTask, new();

        /// <summary>
        ///     Saves all the pending changes to the server.
        /// </summary>
        Task SaveChangesAsync(CancellationToken token = default (CancellationToken));

        /// <summary>
        ///     Stores entity in session, extracts Id from entity using Conventions or generates new one if it is not available and
        ///     forces concurrency check with given Etag
        /// </summary>
        Task StoreAsync(object entity, long? etag, CancellationToken token = default (CancellationToken));

        /// <summary>
        ///     Stores entity in session, extracts Id from entity using Conventions or generates new one if it is not available.
        ///     <para>Forces concurrency check if the Id is not available during extraction.</para>
        /// </summary>
        /// <param name="entity">entity to store.</param>
        /// <param name="token">The cancellation token.</param>
        Task StoreAsync(object entity, CancellationToken token = default (CancellationToken));

        /// <summary>
        ///     Stores entity in session with given id and forces concurrency check with given Etag.
        /// </summary>
        Task StoreAsync(object entity, long? etag, string id, CancellationToken token = default (CancellationToken));

        /// <summary>
        ///     Stores the specified dynamic entity, under the specified id.
        /// </summary>
        /// <param name="entity">entity to store.</param>
        /// <param name="id">Id to store this entity under. If other entity exists with the same id it will be overwritten.</param>
        /// <param name="token">The cancellation token.</param>
        Task StoreAsync(object entity, string id, CancellationToken token = default (CancellationToken));
    }
}
