//-----------------------------------------------------------------------
// <copyright file="IDocumentStore.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.BulkInsert;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Documents.TimeSeries;
using Raven.Client.Http;
using Raven.Client.Util;

namespace Raven.Client.Documents
{
    /// <summary>
    /// Interface for managing access to RavenDB and open sessions.
    /// </summary>
    public interface IDocumentStore : IDisposalNotification
    {
        X509Certificate2 Certificate { get; }

        event EventHandler<BeforeStoreEventArgs> OnBeforeStore;

        event EventHandler<AfterSaveChangesEventArgs> OnAfterSaveChanges;

        event EventHandler<BeforeDeleteEventArgs> OnBeforeDelete;

        event EventHandler<BeforeQueryEventArgs> OnBeforeQuery;

        event EventHandler<SessionCreatedEventArgs> OnSessionCreated;

        event EventHandler<BeforeConversionToDocumentEventArgs> OnBeforeConversionToDocument;

        event EventHandler<AfterConversionToDocumentEventArgs> OnAfterConversionToDocument;

        event EventHandler<BeforeConversionToEntityEventArgs> OnBeforeConversionToEntity;

        event EventHandler<AfterConversionToEntityEventArgs> OnAfterConversionToEntity;

        event EventHandler<FailedRequestEventArgs> OnFailedRequest;

        event EventHandler<TopologyUpdatedEventArgs> OnTopologyUpdated;

        event EventHandler<SessionDisposingEventArgs> OnSessionDisposing;

        /// <summary>
        /// Subscribe to change notifications from the server
        /// </summary>
        IDatabaseChanges Changes(string database = null);

        /// <summary>
        /// Subscribe to change notifications from the selected server
        /// </summary>
        /// <param name="database">The database to subscribe, if null or empty, the default database will be used</param>
        /// <param name="nodeTag">The node tag of selected server</param>
        IDatabaseChanges Changes(string database, string nodeTag);

        /// <summary>
        /// Setup the context for aggressive caching.
        /// </summary>
        /// <param name="cacheDuration">Specify the aggressive cache duration</param>
        /// <param name="database">The database to cache, if not specified, the default database will be used</param>
        /// <remarks>
        /// Aggressive caching means that we will not check the server to see whether the response
        /// we provide is current or not, but will serve the information directly from the local cache
        /// without touching the server.
        /// </remarks>
        IDisposable AggressivelyCacheFor(TimeSpan cacheDuration, string database = null);

        /// <summary>
        /// Setup the context for aggressive caching.
        /// </summary>
        /// <param name="cacheDuration">Specify the aggressive cache duration</param>
        /// <param name="database">The database to cache, if not specified, the default database will be used</param>
        /// <param name="mode">Aggressive caching mode, if not specified, TrackChanges mode will be used</param>
        /// <remarks>
        /// Aggressive caching means that we will not check the server to see whether the response
        /// we provide is current or not, but will serve the information directly from the local cache
        /// without touching the server.
        /// </remarks>
        IDisposable AggressivelyCacheFor(TimeSpan cacheDuration, AggressiveCacheMode mode, string database = null);

        /// <summary>
        /// Setup the context for aggressive caching.
        /// </summary>
        /// <remarks>
        /// Aggressive caching means that we will not check the server to see whether the response
        /// we provide is current or not, but will serve the information directly from the local cache
        /// without touching the server.
        /// </remarks>
        IDisposable AggressivelyCache(string database = null);

        /// <summary>
        /// Setup the context for no aggressive caching
        /// </summary>
        /// <remarks>
        /// This is mainly useful for internal use inside RavenDB, when we are executing
        /// queries that has been marked with WaitForNonStaleResults, we temporarily disable
        /// aggressive caching.
        /// </remarks>
        IDisposable DisableAggressiveCaching(string database = null);

        /// <summary>
        /// Gets or sets the identifier for this store.
        /// </summary>
        /// <value>The identifier.</value>
        string Identifier { get; set; }

        /// <summary>
        /// Initializes this instance.
        /// </summary>
        /// <returns></returns>
        IDocumentStore Initialize();

        /// <summary>
        /// Opens the async session.
        /// </summary>
        /// <returns></returns>
        IAsyncDocumentSession OpenAsyncSession();

        /// <summary>
        /// Opens the async session.
        /// </summary>
        /// <returns></returns>
        IAsyncDocumentSession OpenAsyncSession(string database);

        /// <summary>
        /// Opens the async session with the specified options.
        /// </summary>
        IAsyncDocumentSession OpenAsyncSession(SessionOptions sessionOptions);

        /// <summary>
        /// Opens the session.
        /// </summary>
        /// <returns></returns>
        IDocumentSession OpenSession();

        /// <summary>
        /// Opens the session for a particular database
        /// </summary>
        IDocumentSession OpenSession(string database);

        /// <summary>
        /// Opens the session with the specified options.
        /// </summary>
        IDocumentSession OpenSession(SessionOptions sessionOptions);

        /// <summary>
        /// Executes the index creation.
        /// </summary>
        void ExecuteIndex(IAbstractIndexCreationTask task, string database = null);

        void ExecuteIndexes(IEnumerable<IAbstractIndexCreationTask> tasks, string database = null);

        /// <summary>
        /// Executes the index creation.
        /// </summary>
        Task ExecuteIndexAsync(IAbstractIndexCreationTask task, string database = null, CancellationToken token = default);

        Task ExecuteIndexesAsync(IEnumerable<IAbstractIndexCreationTask> tasks, string database = null, CancellationToken token = default);

        TimeSeriesOperations TimeSeries { get; }

        /// <summary>
        /// Gets the conventions.
        /// </summary>
        /// <value>The conventions.</value>
        DocumentConventions Conventions { get; }

        /// <summary>
        /// Gets the URL's.
        /// </summary>
        string[] Urls { get; }

        BulkInsertOperation BulkInsert(string database = null, CancellationToken token = default);

        /// <summary>
        /// Provides methods to manage data subscriptions.
        /// </summary>
        DocumentSubscriptions Subscriptions { get; }

        string Database { get; set; }

        RequestExecutor GetRequestExecutor(string database = null);

        MaintenanceOperationExecutor Maintenance { get; }

        OperationExecutor Operations { get; }

        DatabaseSmuggler Smuggler { get; }

        IDisposable SetRequestTimeout(TimeSpan timeout, string database = null);
    }
}
