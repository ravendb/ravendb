//-----------------------------------------------------------------------
// <copyright file="IDocumentStore.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Threading.Tasks;
using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Client.Changes;
using Raven.NewClient.Client.Connection;

using Raven.NewClient.Client.Connection.Profiling;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Http;
using Raven.NewClient.Client.Indexes;

namespace Raven.NewClient.Client
{

    /// <summary>
    /// Interface for managing access to RavenDB and open sessions.
    /// </summary>
    public interface IDocumentStore : IDisposalNotification
    {
        /// <summary>
        /// Store events
        /// </summary>
        event EventHandler<BeforeStoreEventArgs> OnBeforeStore;
        event EventHandler<AfterStoreEventArgs> OnAfterStore;

        /// <summary>
        /// Delete event
        /// </summary>
        event EventHandler<BeforeDeleteEventArgs> OnBeforeDelete;

        /// <summary>
        /// Query event
        /// </summary>
        event EventHandler<BeforeQueryExecutedEventArgs> OnBeforeQueryExecuted;

        /// <summary>
        /// Subscribe to change notifications from the server
        /// </summary>
        IDatabaseChanges Changes(string database = null);

        /// <summary>
        /// Setup the context for aggressive caching.
        /// </summary>
        /// <param name="cacheDuration">Specify the aggressive cache duration</param>
        /// <remarks>
        /// Aggressive caching means that we will not check the server to see whatever the response
        /// we provide is current or not, but will serve the information directly from the local cache
        /// without touching the server.
        /// </remarks>
        IDisposable AggressivelyCacheFor(TimeSpan cacheDuration);

        /// <summary>
        /// Setup the context for aggressive caching.
        /// </summary>
        /// <remarks>
        /// Aggressive caching means that we will not check the server to see whatever the response
        /// we provide is current or not, but will serve the information directly from the local cache
        /// without touching the server.
        /// </remarks>
        IDisposable AggressivelyCache();

        /// <summary>
        /// Setup the context for no aggressive caching
        /// </summary>
        /// <remarks>
        /// This is mainly useful for internal use inside RavenDB, when we are executing
        /// queries that has been marked with WaitForNonStaleResults, we temporarily disable
        /// aggressive caching.
        /// </remarks>
        IDisposable DisableAggressiveCaching();

        /// <summary>
        /// Setup the WebRequest timeout for the session
        /// </summary>
        /// <param name="timeout">Specify the timeout duration</param>
        /// <remarks>
        /// Sets the timeout for the JsonRequest.  Scoped to the Current Thread.
        /// </remarks>
        IDisposable SetRequestsTimeoutFor(TimeSpan timeout);

        /// <summary>
        /// Gets the shared operations headers.
        /// </summary>
        /// <value>The shared operations headers.</value>
        NameValueCollection SharedOperationsHeaders { get; }

        /// <summary>
        /// Whatever this instance has json request factory available
        /// </summary>
        bool HasJsonRequestFactory { get; }

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
        IAsyncDocumentSession OpenAsyncSession(OpenSessionOptions sessionOptions);

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
        IDocumentSession OpenSession(OpenSessionOptions sessionOptions);

        /// <summary>
        /// Executes the index creation in side-by-side mode.
        /// </summary>
        void SideBySideExecuteIndex(AbstractIndexCreationTask indexCreationTask, long? minimumEtagBeforeReplace = null, DateTime? replaceTimeUtc = null);

        /// <summary>
        /// Executes the index creation in side-by-side mode.
        /// </summary>
        Task SideBySideExecuteIndexAsync(AbstractIndexCreationTask indexCreationTask, long? minimumEtagBeforeReplace = null, DateTime? replaceTimeUtc = null);

        void SideBySideExecuteIndexes(IList<AbstractIndexCreationTask> indexCreationTasks, long? minimumEtagBeforeReplace = null, DateTime? replaceTimeUtc = null);

        Task SideBySideExecuteIndexesAsync(List<AbstractIndexCreationTask> indexCreationTasks, long? minimumEtagBeforeReplace = null, DateTime? replaceTimeUtc = null);

        /// <summary>
        /// Executes the index creation.
        /// </summary>
        void ExecuteIndex(AbstractIndexCreationTask indexCreationTask);

        void ExecuteIndexes(IList<AbstractIndexCreationTask> indexCreationTasks);

        /// <summary>
        /// Executes the index creation.
        /// </summary>
        /// <param name="indexCreationTask"></param>
        Task ExecuteIndexAsync(AbstractIndexCreationTask indexCreationTask);

        Task ExecuteIndexesAsync(List<AbstractIndexCreationTask> indexCreationTasks);

        /// <summary>
        /// Executes the transformer creation
        /// </summary>
        void ExecuteTransformer(AbstractTransformerCreationTask transformerCreationTask);

        Task ExecuteTransformerAsync(AbstractTransformerCreationTask transformerCreationTask);

        /// <summary>
        /// Gets the conventions.
        /// </summary>
        /// <value>The conventions.</value>
        DocumentConvention Conventions { get; }

        /// <summary>
        /// Gets the URL.
        /// </summary>
        string Url { get; }

        ///<summary>
        /// Gets the etag of the last document written by any session belonging to this 
        /// document store
        ///</summary>
        long? GetLastWrittenEtag();

        BulkInsertOperation BulkInsert(string database = null);

        /// <summary>
        /// Provides methods to manage data subscriptions in async manner.
        /// </summary>
        IAsyncReliableSubscriptions AsyncSubscriptions { get; }

        /// <summary>
        /// Provides methods to manage data subscriptions.
        /// </summary>
        IReliableSubscriptions Subscriptions { get; }

        void InitializeProfiling();

        ProfilingInformation GetProfilingInformationFor(Guid id);

        string DefaultDatabase { get; set; }

        RequestExecuter GetRequestExecuter(string databaseName);

        RequestExecuter GetRequestExecuterForDefaultDatabase();
    }
}
