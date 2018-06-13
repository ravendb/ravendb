//-----------------------------------------------------------------------
// <copyright file="ISessionDocumentCountersBase.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

namespace Raven.Client.Documents.Session
{
    /// <summary>
    ///     Counters advanced synchronous session operations
    /// </summary>
    public interface ISessionDocumentCountersBase
    {
        /// <summary>
        /// Increments by delta the value of a counter 
        /// <param name="counter">the counter name</param>
        /// </summary>
        void Increment(string counter, long delta = 1);

        /// <summary>
        /// Marks the specified document's counter for deletion. The counter will be deleted when
        /// <see cref="IDocumentSession.SaveChanges" /> is called.
        /// </summary>
        /// <param name="counter">the counter name</param>
        void Delete(string counter);

    }
}
