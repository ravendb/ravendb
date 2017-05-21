//-----------------------------------------------------------------------
// <copyright file="AsyncDocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    /// Implementation for async document session 
    /// </summary>
    public partial class AsyncDocumentSession
    {
        public Task<Operation> PatchByIndexAsync<T, TIndexCreator>(Expression<Func<T, bool>> expression, PatchRequest patch, QueryOperationOptions options = null, CancellationToken token = new CancellationToken()) where TIndexCreator : AbstractIndexCreationTask, new()
        {
            var indexCreator = new TIndexCreator();
            return DeleteByIndexAsync(indexCreator.IndexName, expression, options, token);
        }

        public Task<Operation> PatchByIndexAsync<T>(string indexName, Expression<Func<T, bool>> expression, PatchRequest patch, QueryOperationOptions options = null, CancellationToken token = new CancellationToken())
        {
            var query = Query<T>(indexName).Where(expression);
            var indexQuery = new IndexQuery
            {
                Query = query.ToString()
            };
            if (_operations == null)
                _operations = new OperationExecutor(_documentStore, _requestExecutor, Context);

            return _operations.SendAsync(new PatchByIndexOperation(indexName, indexQuery, patch, options), token);
        }


        public Task<Operation> DeleteByIndexAsync<T, TIndexCreator>(Expression<Func<T, bool>> expression, QueryOperationOptions options = null, CancellationToken token = default(CancellationToken)) where TIndexCreator : AbstractIndexCreationTask, new()
        {
            var indexCreator = new TIndexCreator();
            return DeleteByIndexAsync(indexCreator.IndexName, expression, options, token);
        }

        public Task<Operation> DeleteByIndexAsync<T>(string indexName, Expression<Func<T, bool>> expression, QueryOperationOptions options = null, CancellationToken token = default(CancellationToken))
        {
            var query = Query<T>(indexName).Where(expression);
            var indexQuery = new IndexQuery
            {
                Query = query.ToString()
            };
            if (_operations == null)
                _operations = new OperationExecutor(_documentStore, _requestExecutor, Context);

            return _operations.SendAsync(new DeleteByIndexOperation(indexName, indexQuery, options), token);
        }
    }
}
