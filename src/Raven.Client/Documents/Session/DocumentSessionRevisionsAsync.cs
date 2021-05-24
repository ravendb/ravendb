//-----------------------------------------------------------------------
// <copyright file="DocumentSessionRevisionsAsync.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Session.Operations;
using Raven.Client.Json;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    /// Implements Unit of Work for accessing the RavenDB server
    /// </summary>
    public class DocumentSessionRevisionsAsync : DocumentSessionRevisionsBase, IRevisionsSessionOperationsAsync
    {
        public DocumentSessionRevisionsAsync(InMemoryDocumentSessionOperations session) : base(session)
        {
        }

        public async Task<List<T>> GetForAsync<T>(string id, int start = 0, int pageSize = 25, CancellationToken token = default)
        {
            using (Session.AsyncTaskHolder())
            {
                var operation = new GetRevisionOperation(Session, id, start, pageSize);
                var command = operation.CreateRequest();
                await RequestExecutor.ExecuteAsync(command, Context, SessionInfo, token).ConfigureAwait(false);
                operation.SetResult(command.Result);
                return operation.GetRevisionsFor<T>();
            }
        }

        public async Task<List<MetadataAsDictionary>> GetMetadataForAsync(string id, int start = 0, int pageSize = 25, CancellationToken token = default)
        {
            using (Session.AsyncTaskHolder())
            {
                var operation = new GetRevisionOperation(Session, id, start, pageSize, true);
                var command = operation.CreateRequest();
                await RequestExecutor.ExecuteAsync(command, Context, SessionInfo, token).ConfigureAwait(false);
                operation.SetResult(command.Result);
                return operation.GetRevisionsMetadataFor();
            }
        }

        public async Task<T> GetAsync<T>(string changeVector, CancellationToken token = default)
        {
            using (Session.AsyncTaskHolder())
            {
                var operation = new GetRevisionOperation(Session, changeVector);
                var command = operation.CreateRequest();
                await RequestExecutor.ExecuteAsync(command, Context, SessionInfo, token).ConfigureAwait(false);
                operation.SetResult(command.Result);
                return operation.GetRevision<T>();
            }
        }

        public async Task<Dictionary<string, T>> GetAsync<T>(IEnumerable<string> changeVectors, CancellationToken token = default)
        {
            using (Session.AsyncTaskHolder())
            {
                var operation = new GetRevisionOperation(Session, changeVectors);
                var command = operation.CreateRequest();
                await RequestExecutor.ExecuteAsync(command, Context, SessionInfo, token).ConfigureAwait(false);
                operation.SetResult(command.Result);
                return operation.GetRevisions<T>();
            }
        }

        public async Task<T> GetAsync<T>(string id, DateTime date, CancellationToken token = default)
        {
            using (Session.AsyncTaskHolder())
            {
                var operation = new GetRevisionOperation(Session, id, date);
                var command = operation.CreateRequest();
                await RequestExecutor.ExecuteAsync(command, Context, SessionInfo, token).ConfigureAwait(false);
                operation.SetResult(command.Result);
                return operation.GetRevisionsFor<T>().FirstOrDefault();
            }
        }
    }
}
