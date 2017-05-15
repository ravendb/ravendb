//-----------------------------------------------------------------------
// <copyright file="IDocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    ///     Interface for document session
    /// </summary>
    public partial interface IDocumentSession : IDisposable
    {
        /// <summary>
        ///     Get the accessor for advanced operations
        /// </summary>
        /// <remarks>
        ///     Those operations are rarely needed, and have been moved to a separate
        ///     property to avoid cluttering the API
        /// </remarks>
        IAdvancedSessionOperation Advanced { get; }

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
        /// <param name="expectedEtag">Expected etag of a document to delete.</param>
        void Delete(string id, long? expectedEtag);

        /// <summary>
        ///     Saves all the pending changes to the server.
        /// </summary>
        void SaveChanges();

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
        /// <param name="id">Id to store this entity under. If other entity exists with the same id it will be overwritten.</param>
        void Store(object entity, string id);
    }
}
