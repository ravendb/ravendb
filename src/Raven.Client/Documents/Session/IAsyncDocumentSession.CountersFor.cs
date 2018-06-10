//-----------------------------------------------------------------------
// <copyright file="IAsyncDocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

namespace Raven.Client.Documents.Session
{
    /// <inheritdoc />
    public partial interface IAsyncDocumentSession
    {

        AsyncSessionDocumentCounters CountersFor(string documentId);

        AsyncSessionDocumentCounters CountersFor(object entity);
    }
}
