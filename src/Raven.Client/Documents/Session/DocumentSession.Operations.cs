//-----------------------------------------------------------------------
// <copyright file="DocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Linq.Expressions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    /// Implements Unit of Work for accessing the RavenDB server
    /// </summary>
    public partial class DocumentSession
    {
        public Operation DeleteByIndex<T, TIndexCreator>(Expression<Func<T, bool>> expression, QueryOperationOptions options = null) where TIndexCreator : AbstractIndexCreationTask, new()
        {
            var indexCreator = new TIndexCreator();
            return DeleteByIndex(indexCreator.IndexName, expression, options);
        }

        public Operation DeleteByIndex<T>(string indexName, Expression<Func<T, bool>> expression, QueryOperationOptions options = null)
        {
            var query = Query<T>(indexName).Where(expression);
            var indexQuery = new IndexQuery
            {
                Query = query.ToString()
            };

            if (_operations == null)
                _operations = new OperationExecutor(_documentStore, _requestExecutor, Context);

            return _operations.Send(new DeleteByIndexOperation(indexName, indexQuery, options));
        }

        public Operation PatchByIndex<T, TIndexCreator>(Expression<Func<T, bool>> expression, PatchRequest patch, QueryOperationOptions options = null) where TIndexCreator : AbstractIndexCreationTask, new()
        {
            var indexCreator = new TIndexCreator();
            return PatchByIndex(indexCreator.IndexName, expression, patch, options);
        }

        public Operation PatchByIndex<T>(string indexName, Expression<Func<T, bool>> expression, PatchRequest patch, QueryOperationOptions options = null)
        {
            var query = Query<T>(indexName).Where(expression);
            var indexQuery = new IndexQuery
            {
                Query = query.ToString()
            };

            if (_operations == null)
                _operations = new OperationExecutor(_documentStore, _requestExecutor, Context);

            return _operations.Send(new PatchByIndexOperation(indexName, indexQuery, patch, options));
        }
    }
}