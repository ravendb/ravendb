//-----------------------------------------------------------------------
// <copyright file="DocumentSessionRevisions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;
using Raven.Client.Documents.Session.Operations;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    /// Implements Unit of Work for accessing the RavenDB server
    /// </summary>
    public class DocumentSessionRevisions : AdvancedSessionExtentionBase, IRevisionsSessionOperations
    {
        public DocumentSessionRevisions(InMemoryDocumentSessionOperations session) : base(session)
        {
        }

        public List<T> GetFor<T>(string id, int start = 0, int pageSize = 25)
        {
            var operation = new GetRevisionOperation(Session, id, start, pageSize);

            var command = operation.CreateRequest();
            RequestExecutor.Execute(command, Context, sessionInfo: SessionInfo);
            operation.SetResult(command.Result);
            return operation.Complete<T>();
        }
    }
}
