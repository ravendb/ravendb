//-----------------------------------------------------------------------
// <copyright file="IAsyncDocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

namespace Raven.Client.Documents.Session
{
    public partial interface IAsyncDocumentSession
    {
        IAsyncSessionDocumentCounters CountersFor(string documentId);

        IAsyncSessionDocumentCounters CountersFor(object entity);
    }
}
