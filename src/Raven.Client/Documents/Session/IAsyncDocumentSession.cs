//-----------------------------------------------------------------------
// <copyright file="IAsyncDocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    ///     Interface for document session using async approaches
    /// </summary>
    public partial interface IAsyncDocumentSession : IDisposable
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
        ///     Marks the specified entity for deletion. The entity will be deleted when
        ///     <see cref="IDocumentSession.SaveChanges" /> is called.
        ///     <para>WARNING: This method will not call beforeDelete listener!</para>
        /// </summary>
        /// <param name="id">entity Id</param>
        /// <param name="expectedChangeVector">Expected change vector of a document to delete.</param>
        void Delete(string id, string expectedChangeVector);

        /// <summary>
        ///     Saves all the pending changes to the server.
        /// </summary>
        Task SaveChangesAsync(CancellationToken token = default (CancellationToken));

        /// <summary>
        ///     Stores entity in session, extracts Id from entity using Conventions or generates new one if it is not available.
        ///     <para>Forces concurrency check if the Id is not available during extraction.</para>
        /// </summary>
        /// <param name="entity">entity to store.</param>
        /// <param name="token">The cancellation token.</param>
        Task StoreAsync(object entity, CancellationToken token = default (CancellationToken));

        /// <summary>
        ///     Stores entity in session with given id and forces concurrency check with given change vector.
        /// </summary>
        Task StoreAsync(object entity, string changeVector, string id, CancellationToken token = default(CancellationToken));

        /// <summary>
        ///     Stores the specified dynamic entity, under the specified id.
        /// </summary>
        /// <param name="entity">entity to store.</param>
        /// <param name="id">Id to store this entity under. If other entity exists with the same id it will be overwritten.</param>
        /// <param name="token">The cancellation token.</param>
        Task StoreAsync(object entity, string id, CancellationToken token = default (CancellationToken));
    }
}
