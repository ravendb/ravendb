//-----------------------------------------------------------------------
// <copyright file="IDocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------


namespace Raven.Client.Documents.Session
{
    /// <summary>
    /// Provides an acces to DocumentSessionCounters API.
    /// </summary>
    public partial interface IDocumentSession
    {

        SessionDocumentCounters CountersFor(string documentId);

        SessionDocumentCounters CountersFor(object entity);

    }
}
