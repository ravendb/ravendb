//-----------------------------------------------------------------------
// <copyright file="ISessionDocumentCountersBase.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

namespace Raven.Client.Documents.Session
{
    public interface ISessionDocumentCountersBase
    {
        /// <summary>
        /// Increments the counter value by the provided delta, or by 1 if delta is not provided.
        /// </summary>
        /// <param name="counter">The counter to increment</param>
        /// <param name="delta">The value to increment by</param>
        void Increment(string counter, long delta = 1);

        /// <summary>
        /// Marks the specified document's counter for deletion. The counter will be deleted when
        /// <see cref="IDocumentSession.SaveChanges" /> is called.
        /// </summary>
        /// <param name="counter">The counter to delete</param>
        void Delete(string counter);

    }
}
