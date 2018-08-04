//-----------------------------------------------------------------------
// <copyright file="IAdvancedDocumentSessionOperations.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Graph;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    ///     Advanced session operations
    /// </summary>
    public interface IAdvancedDocumentSessionOperations
    {
        /// <summary>
        ///     The document store associated with this session
        /// </summary>
        IDocumentStore DocumentStore { get; }

        /// <summary>
        ///     Allow extensions to provide additional state per session
        /// </summary>
        IDictionary<string, object> ExternalState { get; }

        Task<ServerNode> GetCurrentSessionNode();
        
        RequestExecutor RequestExecutor { get; }
        JsonOperationContext Context { get; }

        event EventHandler<BeforeStoreEventArgs> OnBeforeStore;
        event EventHandler<AfterSaveChangesEventArgs> OnAfterSaveChanges;
        event EventHandler<BeforeDeleteEventArgs> OnBeforeDelete;
        event EventHandler<BeforeQueryEventArgs> OnBeforeQuery;

        /// <summary>
        ///     Gets a value indicating whether any of the entities tracked by the session has changes.
        /// </summary>
        bool HasChanges { get; }

        /// <summary>
        ///     Gets or sets the max number of requests per session.
        ///     If the <see cref="NumberOfRequests" /> rise above <see cref="MaxNumberOfRequestsPerSession" />, an exception will
        ///     be thrown.
        /// </summary>
        /// <value>The max number of requests per session.</value>
        int MaxNumberOfRequestsPerSession { get; set; }

        /// <summary>
        ///     Gets the number of requests for this session
        /// </summary>
        int NumberOfRequests { get; }

        /// <summary>
        ///     Gets the store identifier for this session.
        ///     The store identifier is the identifier for the particular RavenDB instance.
        /// </summary>
        string StoreIdentifier { get; }

        /// <summary>
        ///     Gets or sets a value indicating whether the session should use optimistic concurrency.
        ///     When set to <c>true</c>, a check is made so that a change made behind the session back would fail
        ///     and raise <see cref="ConcurrencyException" />.
        /// </summary>
        bool UseOptimisticConcurrency { get; set; }

        /// <summary>
        ///     Clears this instance.
        ///     Remove all entities from the delete queue and stops tracking changes for all entities.
        /// </summary>
        void Clear();

        /// <summary>
        ///     Defer commands to be executed on SaveChanges()
        /// </summary>
        /// <param name="command">Command to be executed</param>
        /// <param name="commands">Array of commands to be executed.</param>
        void Defer(ICommandData command, params ICommandData[] commands);

        /// <summary>
        ///     Defer commands to be executed on SaveChanges()
        /// </summary>
        /// <param name="commands">Array of commands to be executed.</param>
        void Defer(ICommandData[] commands);

        /// <summary>
        ///     Evicts the specified entity from the session.
        ///     Remove the entity from the delete queue and stops tracking changes for this entity.
        /// </summary>
        /// <param name="entity">Entity to evict.</param>
        void Evict<T>(T entity);

        /// <summary>
        ///     Gets the document id for the specified entity.
        /// </summary>
        /// <remarks>
        ///     This function may return <c>null</c> if the entity isn't tracked by the session, or if the entity is
        ///     a new entity with an ID that should be generated on the server.
        /// </remarks>
        /// <param name="entity">The entity.</param>
        string GetDocumentId(object entity);

        /// <summary>
        ///     Gets the metadata for the specified entity.
        ///     If the entity is transient, it will load the metadata from the store
        ///     and associate the current state of the entity with the metadata from the server.
        /// </summary>
        /// <param name="instance">The instance.</param>
        IMetadataDictionary GetMetadataFor<T>(T instance);

        
        IReadOnlyList<(string EdgeType, EdgeInfo Edge)> GetEdgesOf<T>(T instance);

        /// <summary>
        /// Add edge between two documents
        /// </summary>
        /// <param name="from">entity to add the edge from</param>
        /// <param name="to">entity to add the edge to</param>
        /// <param name="type">type of the edge</param>
        /// <param name="edgeProperties">an entity that contains the properties attached to the edge</param>
        void AddEdgeBetween(object @from, object to, string type, Dictionary<string, object> edgeProperties = null);

        /// <summary>
        ///     Gets change vector for the specified entity.
        ///     If the entity is transient, it will load the metadata from the store
        ///     and associate the current state of the entity with the metadata from the server.
        /// </summary>
        /// <param name="instance">The instance.</param>
        string GetChangeVectorFor<T>(T instance);

        /// <summary>
        ///     Gets all the counter names for the specified entity.
        /// </summary>
        /// <param name="instance">The instance.</param>
        List<string> GetCountersFor<T>(T instance);

        /// <summary>
        ///     Gets last modified date for the specified entity.
        ///     If the entity is transient, it will load the metadata from the store
        ///     and associate the current state of the entity with the metadata from the server.
        /// </summary>
        /// <param name="instance">The instance.</param>
        DateTime? GetLastModifiedFor<T>(T instance);

        /// <summary>
        ///     Determines whether the specified entity has changed.
        /// </summary>
        /// <param name="entity">The entity.</param>
        /// <returns>
        ///     <c>true</c> if the specified entity has changed; otherwise, <c>false</c>.
        /// </returns>
        bool HasChanged(object entity);

        /// <summary>
        ///     Returns whether a document with the specified id is loaded in the
        ///     current session
        /// </summary>
        bool IsLoaded(string id);

        /// <summary>
        /// Mark the entity as one that should be ignore for change tracking purposes,
        /// it still takes part in the session, but is ignored for SaveChanges.
        /// </summary>
        void IgnoreChangesFor(object entity);

        /// <summary>
        /// Returns all changes for each entity stored within session. Including name of the field/property that changed, its old and new value and change type.
        /// </summary>
        IDictionary<string, DocumentsChanges[]> WhatChanged();

        /// <summary>
        /// SaveChanges will wait for the changes made to be replicates to `replicas` nodes
        /// </summary>
        void WaitForReplicationAfterSaveChanges(TimeSpan? timeout = null, bool throwOnTimeout = true, int replicas = 1, bool majority = false);

        /// <summary>
        /// SaveChanges will wait for the indexes to catch up with the saved changes
        /// </summary>
        void WaitForIndexesAfterSaveChanges(TimeSpan? timeout = null, bool throwOnTimeout = true, string[] indexes = null);

        /// <summary>
        /// Overwrite the existing transaction mode for the current session.
        /// </summary>
        /// <param name="mode"></param>
        void SetTransactionMode(TransactionMode mode);

        EntityToBlittable EntityToBlittable { get; }
    }
}
