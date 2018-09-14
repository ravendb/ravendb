//-----------------------------------------------------------------------
// <copyright file="DocumentSessionRevisions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Session.Operations;
using Raven.Client.Json;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    /// Implements Unit of Work for accessing the RavenDB server
    /// </summary>
    public class DocumentSessionRevisions : AdvancedSessionExtensionBase, IRevisionsSessionOperations
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
            return operation.GetRevisionsFor<T>();
        }

        public List<MetadataAsDictionary> GetMetadataFor(string id, int start = 0, int pageSize = 25)
        {
            var operation = new GetRevisionOperation(Session, id, start, pageSize, true);
            var command = operation.CreateRequest();
            RequestExecutor.Execute(command, Context, sessionInfo: SessionInfo);
            operation.SetResult(command.Result);
            return operation.GetRevisionsMetadataFor();
        }

        public T Get<T>(string changeVector)
        {
            var operation = new GetRevisionOperation(Session, changeVector);

            var command = operation.CreateRequest();
            RequestExecutor.Execute(command, Context, sessionInfo: SessionInfo);
            operation.SetResult(command.Result);
            return operation.GetRevision<T>();
        }

        public Dictionary<string, T> Get<T>(IEnumerable<string> changeVectors)
        {
            var operation = new GetRevisionOperation(Session, changeVectors);

            var command = operation.CreateRequest();
            RequestExecutor.Execute(command, Context, sessionInfo: SessionInfo);
            operation.SetResult(command.Result);
            return operation.GetRevisions<T>();
        }

        public T Get<T>(string id, DateTime date)
        {
            var operation = new GetRevisionOperation(Session, id, date);
            var command = operation.CreateRequest();
            RequestExecutor.Execute(command, Context, sessionInfo: SessionInfo);
            operation.SetResult(command.Result);
            return operation.GetRevisionsFor<T>().FirstOrDefault();
        }
    }
}
