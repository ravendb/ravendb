using Raven.Abstractions.Data;
using Raven.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Client.FileSystem
{
    public interface IAdvancedFilesSessionOperations
    {
        /// <summary>
        /// The filesystem store associated with this session
        /// </summary>
        IFilesStore DocumentStore { get; }

        /// <summary>
        /// Gets the store identifier for this session.
        /// The store identifier is the identifier for the particular RavenDB instance. 
        /// </summary>
        /// <value>The store identifier.</value>
        string StoreIdentifier { get; }

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
        /// Evicts the specified remote object from the session.
        /// Remove the object from the delete queue and stops tracking changes for this object.
        /// </summary>
        /// <param name="entity">The remote object.</param>
        void Evict(IRemoteObject entity);

        /// <summary>
        /// Clears this instance.
        /// Remove all remote objects from the delete queue and stops tracking changes for all objects.
        /// </summary>
        void Clear();

        /// <summary>
        /// Gets the metadata for the specified entity.
        /// If the entity is transient, it will load the metadata from the store
        /// and associate the current state of the entity with the metadata from the server.
        /// </summary>
        /// <param name="instance">The instance.</param>
        /// <returns></returns>
        RavenJObject GetMetadataFor(IRemoteObject instance);

        /// <summary>
        /// Gets the ETag for the specified entity.
        /// If the entity is transient, it will load the etag from the store
        /// and associate the current state of the entity with the etag from the server.
        /// </summary>
        /// <param name="instance">The instance.</param>
        /// <returns></returns>
        Etag GetEtagFor(IRemoteObject instance);

        /// <summary>
        /// Gets a value indicating whether any of the entities tracked by the session has changes.
        /// </summary>
        bool HasChanges { get; }

    }
}
