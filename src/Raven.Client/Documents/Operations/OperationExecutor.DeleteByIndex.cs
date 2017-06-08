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
        public Operation DeleteByIndex<T, TIndexCreator>(Expression<Func<T, bool>> expression, QueryOperationOptions options = null) where TIndexCreator : AbstractIndexCreationTask, new()
        {
            var indexCreator = new TIndexCreator();
            return DeleteByIndex(indexCreator.IndexName, expression, options);
        }

        public Operation DeleteByIndex<T>(string indexName, Expression<Func<T, bool>> expression, QueryOperationOptions options = null)
        {
            IndexQuery indexQuery;
            using (var session = _store.OpenSession())
            {
                var query = session.Query<T>(indexName).Where(expression);
                indexQuery = new IndexQuery
                {
                    Query = query.ToString()
                };
            }

            return Send(new DeleteByIndexOperation(indexName, indexQuery, options));
        }

        public Task<Operation> DeleteByIndexAsync<T, TIndexCreator>(Expression<Func<T, bool>> expression, QueryOperationOptions options = null, CancellationToken token = default(CancellationToken)) where TIndexCreator : AbstractIndexCreationTask, new()
        {
            var indexCreator = new TIndexCreator();
            return DeleteByIndexAsync(indexCreator.IndexName, expression, options, token);
        }

        public Task<Operation> DeleteByIndexAsync<T>(string indexName, Expression<Func<T, bool>> expression, QueryOperationOptions options = null, CancellationToken token = default(CancellationToken))
        {
            IndexQuery indexQuery;
            using (var session = _store.OpenSession())
            {
                var query = session.Query<T>(indexName).Where(expression);
                indexQuery = new IndexQuery
                {
                    Query = query.ToString()
                };
            }

            return SendAsync(new DeleteByIndexOperation(indexName, indexQuery, options), token);
        }
    }
}