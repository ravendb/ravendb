//-----------------------------------------------------------------------
// <copyright file="IDocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------


namespace Raven.Client.Documents.Session
{
    public partial interface IDocumentSession
    {
        /// <summary>
        /// Advanced session API for counter operations on a specific document
        /// </summary>
        /// <param name="documentId">The id of the document to operate on</param>
        ISessionDocumentCounters CountersFor(string documentId);

        /// <summary>
        /// Advanced session API for counter operations on a specific entity
        /// </summary>
        /// <param name="entity">The entity to operate on</param>
        ISessionDocumentCounters CountersFor(object entity);

    }
}
