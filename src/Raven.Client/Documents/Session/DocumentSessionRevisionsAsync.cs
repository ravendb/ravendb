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
using Raven.Client.Documents.Session.Operations.Lazy;
using Raven.Client.Json;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    /// Implements Unit of Work for accessing the RavenDB server
    /// </summary>
    public class DocumentSessionRevisionsAsync : DocumentSessionRevisionsBase, IRevisionsSessionOperationsAsync, ILazyRevisionsOperationsAsync
    {

        public DocumentSessionRevisionsAsync(AsyncDocumentSession session) : base(session)
        {

        }

        public ILazyRevisionsOperationsAsync Lazily => this;

        public async Task<List<T>> GetForAsync<T>(string id, int start = 0, int pageSize = 25, CancellationToken token = default)
        {
            using (Session.AsyncTaskHolder())
            {
                var operation = new GetRevisionOperation(Session, id, start, pageSize);
                var command = operation.CreateRequest();
                await RequestExecutor.ExecuteAsync(command, Context, sessionInfo: SessionInfo, token: token).ConfigureAwait(false);
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
                await RequestExecutor.ExecuteAsync(command, Context, sessionInfo: SessionInfo, token: token).ConfigureAwait(false);
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
                await RequestExecutor.ExecuteAsync(command, Context, sessionInfo: SessionInfo, token).ConfigureAwait(false);
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
                await RequestExecutor.ExecuteAsync(command, Context, sessionInfo: SessionInfo, token).ConfigureAwait(false);
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
                await RequestExecutor.ExecuteAsync(command, Context, sessionInfo: SessionInfo, token: token).ConfigureAwait(false);
                operation.SetResult(command.Result);
                return operation.GetRevisionsFor<T>().FirstOrDefault();
            }
        }

        public async Task<long> GetCountForAsync(string id, CancellationToken token = default)
        {
            using (Session.AsyncTaskHolder())
            {
                var operation = new GetRevisionsCountOperation(id);
                var command = operation.CreateRequest();
                await RequestExecutor.ExecuteAsync(command, Context, sessionInfo: SessionInfo, token: token).ConfigureAwait(false);
                return command.Result;
            }
        }

        Lazy<Task<List<T>>> ILazyRevisionsOperationsAsync.GetForAsync<T>(string id, int start, int pageSize , CancellationToken token)
        {
            var operation = new GetRevisionOperation(Session,id,start, pageSize);
            var lazyRevisionOperation = new LazyRevisionOperation<T>(operation, LazyRevisionOperation<T>.Mode.Multi);
            return ((AsyncDocumentSession )Session).AddLazyOperation<List<T>>(lazyRevisionOperation, null, token);
        }
                                                                                                                                               
        Lazy<Task<T>> ILazyRevisionsOperationsAsync.GetAsync<T>(string changeVector, CancellationToken token)
        {
            var operation = new GetRevisionOperation(Session, changeVector);
            var lazyRevisionOperation = new LazyRevisionOperation<T>(operation, LazyRevisionOperation<T>.Mode.Single);
            return ((AsyncDocumentSession )Session).AddLazyOperation<T>(lazyRevisionOperation, null, token);

        }

        Lazy<Task<T>> ILazyRevisionsOperationsAsync.GetAsync<T>(string id, DateTime dateTime, CancellationToken token)
        {
            var operation = new GetRevisionOperation(Session, id, dateTime);
            var lazyRevisionOperation = new LazyRevisionOperation<T>(operation, LazyRevisionOperation<T>.Mode.Single);
            return ((AsyncDocumentSession )Session).AddLazyOperation<T>(lazyRevisionOperation, null, token);
        }

        Lazy<Task<Dictionary<string, T>>> ILazyRevisionsOperationsAsync.GetAsync<T>(IEnumerable<string> changeVectors, CancellationToken token)
        {
  
            var operation = new GetRevisionOperation(Session,changeVectors);
            var lazyRevisionOperation = new LazyRevisionOperation<T>(operation, LazyRevisionOperation<T>.Mode.Map);
            return ((AsyncDocumentSession )Session).AddLazyOperation<Dictionary<string, T>>(lazyRevisionOperation, null, token);
        }

        Lazy<Task<List<MetadataAsDictionary>>> ILazyRevisionsOperationsAsync.GetMetadataForAsync(string id, int start, int pageSize, CancellationToken token)
        {
            var operation = new GetRevisionOperation(Session, id, start, pageSize);
            var lazyRevisionOperation = new LazyRevisionOperation<MetadataAsDictionary>(operation, LazyRevisionOperation<MetadataAsDictionary>.Mode.ListOfMetadata);
            return ((AsyncDocumentSession )Session).AddLazyOperation<List<MetadataAsDictionary>>(lazyRevisionOperation, null, token);

        }
    }
}
