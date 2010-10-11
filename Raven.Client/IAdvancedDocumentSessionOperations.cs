using System;
using Newtonsoft.Json.Linq;
using Raven.Client.Document;
using Raven.Client.Exceptions;
using Raven.Database.Exceptions;

namespace Raven.Client
{
    /// <summary>
    /// Advanced session operations
    /// </summary>
    public interface IAdvancedDocumentSessionOperations
    {
        /// <summary>
        /// Gets the store identifier for this session.
        /// The store identifier is the identifier for the particular RavenDB instance. 
        /// This is mostly useful when using sharding.
        /// </summary>
        /// <value>The store identifier.</value>
        string StoreIdentifier { get; }

        /// <summary>
        /// Evicts the specified entity from the session.
        /// Remove the entity from the delete queue and stops tracking changes for this entity.
        /// </summary>
        /// <param name="entity">The entity.</param>
        void Evict<T>(T entity);

        /// <summary>
        /// Clears this instance.
        /// Remove all entities from the delete queue and stops tracking changes for all entities.
        /// </summary>
        void Clear();

        /// <summary>
        /// Gets or sets a value indicating whether the session should use optimistic concurrency.
        /// When set to <c>true</c>, a check is made so that a change made behind the session back would fail
        /// and raise <see cref="ConcurrencyException"/>.
        /// </summary>
        bool UseOptimisticConcurrency { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether non authoritive information is allowed.
        /// Non authoritive information is document that has been modified by a transaction that hasn't been committed.
        /// The server provides the latest committed version, but it is known that attempting to write to a non authoritive document
        /// will fail, because it is already modified.
        /// If set to <c>false</c>, the session will wait <see cref="NonAuthoritiveInformationTimeout"/> for the transaction to commit to get an
        /// authoritive information. If the wait is longer than <see cref="NonAuthoritiveInformationTimeout"/>, <see cref="NonAuthoritiveInformationException"/> is thrown.
        /// </summary>
        /// <value>
        /// 	<c>true</c> if non authoritive information is allowed; otherwise, <c>false</c>.
        /// </value>
        bool AllowNonAuthoritiveInformation { get; set; }

        /// <summary>
        /// Gets or sets the timeout to wait for authoritive information if encountered non authoritive document.
        /// </summary>
        TimeSpan NonAuthoritiveInformationTimeout { get; set; }

        /// <summary>
        /// Gets the conventions used by this session
        /// </summary>
        /// <remarks>
        /// This instance is shared among all sessions, changes to the <see cref="DocumentConvention"/> should be done
        /// via the <see cref="IDocumentStore"/> instance, not on a single session.
        /// </remarks>
        /// <value>The conventions.</value>
        DocumentConvention Conventions { get; }

        /// <summary>
        /// Gets or sets the max number of requests per session.
        /// If the <see cref="NumberOfRequests"/> rise above <see cref="MaxNumberOfRequestsPerSession"/>, an exception will be thrown.
        /// </summary>
        /// <value>The max number of requests per session.</value>
        int MaxNumberOfRequestsPerSession { get; set; }

        /// <summary>
        /// Gets the number of requests for this session
        /// </summary>
        int NumberOfRequests { get; }

        /// <summary>
        /// Occurs after an entity is stored in RavenDB.
        /// This event is raised for new and updated entities.
        /// </summary>
        event EntityStored Stored;

        /// <summary>
        /// Occurs when an entity is converted to a document and metadata.
        /// Changes made to the document / metadata instances passed to this event will be persisted.
        /// </summary>
        event EntityToDocument OnEntityConverted;

        /// <summary>
        /// Gets the metadata for the specified entity.
        /// </summary>
        /// <param name="instance">The instance.</param>
        /// <returns></returns>
        JObject GetMetadataFor<T>(T instance);

        /// <summary>
        /// Gets the document id for the specified entity.
        /// </summary>
        /// <remarks>
        /// This function may return <c>null</c> if the entity isn't tracked by the session, or if the entity is 
        /// a new entity with a key that should be generated on the server. 
        /// </remarks>
        /// <param name="entity">The entity.</param>
        string GetDocumentId(object entity);


        /// <summary>
        /// Gets a value indicating whether any of the entities tracked by the session has changes.
        /// </summary>
        bool HasChanges { get; }

        /// <summary>
        /// Determines whether the specified entity has changed.
        /// </summary>
        /// <param name="entity">The entity.</param>
        /// <returns>
        /// 	<c>true</c> if the specified entity has changed; otherwise, <c>false</c>.
        /// </returns>
        bool HasChanged(object entity);       
    }
}