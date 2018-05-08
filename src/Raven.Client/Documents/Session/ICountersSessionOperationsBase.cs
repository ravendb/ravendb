//-----------------------------------------------------------------------
// <copyright file="ICountersSessionOperationsBase.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

namespace Raven.Client.Documents.Session
{
    /// <summary>
    ///     Counters advanced synchronous session operations
    /// </summary>
    public interface ICountersSessionOperationsBase
    {
        /// <summary>
        /// Increments by delta the value of a counter, by document id and counter name 
        /// </summary>
        void Increment(string documentId, string counter, long delta = 1);

        /// <summary>
        /// Increments by delta the value of a counter, by entity and counter name 
        /// </summary>
        void Increment(object entity, string counter, long delta = 1);

        /// <summary>
        /// Marks the specified document's counter for deletion. The counter will be deleted when
        /// <see cref="IDocumentSession.SaveChanges" /> is called.
        /// </summary>
        /// <param name="documentId">the document which holds the counter</param>
        /// <param name="counter">the counter name</param>
        void Delete(string documentId, string counter);

        /// <summary>
        /// Marks the specified document's counter for deletion. The counter will be deleted when
        /// <see cref="IDocumentSession.SaveChanges" /> is called.
        /// </summary>
        /// <param name="entity">instance of entity of the document which holds the counter</param>
        /// <param name="counter">the counter name</param>
        void Delete(object entity, string counter);
    }
}
