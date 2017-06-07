using System;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Queries;

namespace Raven.Client.Documents.Operations
{
    public partial class OperationExecutor
    {
        public Operation PatchByIndex<T, TIndexCreator>(Expression<Func<T, bool>> expression, PatchRequest patch, QueryOperationOptions options = null) where TIndexCreator : AbstractIndexCreationTask, new()
        {
            var indexCreator = new TIndexCreator();
            return PatchByIndex(indexCreator.IndexName, expression, patch, options);
        }

        public Operation PatchByIndex<T>(string indexName, Expression<Func<T, bool>> expression, PatchRequest patch, QueryOperationOptions options = null)
        {
            IRavenQueryable<T> query;
            using (var session = _store.OpenSession())
            {
                query = session.Query<T>(indexName).Where(expression);
            }
            var indexQuery = new IndexQuery
            {
                Query = query.ToString()
            };

            return Send(new PatchByIndexOperation(indexName, indexQuery, patch, options));
        }

        public Task<Operation> PatchByIndexAsync<T, TIndexCreator>(Expression<Func<T, bool>> expression, PatchRequest patch, QueryOperationOptions options = null, CancellationToken token = new CancellationToken()) where TIndexCreator : AbstractIndexCreationTask, new()
        {
            var indexCreator = new TIndexCreator();
            return DeleteByIndexAsync(indexCreator.IndexName, expression, options, token);
        }

        public Task<Operation> PatchByIndexAsync<T>(string indexName, Expression<Func<T, bool>> expression, PatchRequest patch, QueryOperationOptions options = null, CancellationToken token = new CancellationToken())
        {
            IRavenQueryable<T> query;
            using (var session = _store.OpenSession())
            {
                query = session.Query<T>(indexName).Where(expression);
            }
            var indexQuery = new IndexQuery
            {
                Query = query.ToString()
            };

            return SendAsync(new PatchByIndexOperation(indexName, indexQuery, patch, options), token);
        }
    }
}