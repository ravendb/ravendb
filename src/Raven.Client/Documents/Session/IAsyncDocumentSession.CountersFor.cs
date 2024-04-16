//-----------------------------------------------------------------------
// <copyright file="IAsyncDocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

namespace Raven.Client.Documents.Session
{
    public partial interface IAsyncDocumentSession
    {
        /// <inheritdoc cref="IDocumentSession.CountersFor(string)"/>
        IAsyncSessionDocumentCounters CountersFor(string documentId);

        /// <inheritdoc cref="IDocumentSession.CountersFor(object)"/>
        IAsyncSessionDocumentCounters CountersFor(object entity);
    }
}
