using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Session.Operations.Lazy;
using Raven.Client.Extensions;
using Sparrow.Json;

namespace Raven.Client.Documents.Queries.Facets
{
    internal class AggregationQuery<T> : AggregationQueryBase, IAggregationQuery<T>
    {
        private IQueryable<T> _source;

        private readonly Func<IQueryable<T>, Expression> _convertExpressionIfNecessary;
        private readonly MethodInfo _aggregateByMethod;

        public AggregationQuery(
            IQueryable<T> source,
            Func<IQueryable<T>, Expression> convertExpressionIfNecessary,
            MethodInfo aggregateByMethod) : base(((IRavenQueryInspector)source).Session)
        {
            _source = source;
            _convertExpressionIfNecessary = convertExpressionIfNecessary;
            _aggregateByMethod = aggregateByMethod;
        }

        public IAggregationQuery<T> AndAggregateBy(Expression<Func<T, object>> path, Action<FacetFactory<T>> factory = null)
        {
            return AndAggregateBy(path.ToPropertyPath('_'), factory);
        }

        public IAggregationQuery<T> AndAggregateBy(string path, Action<FacetFactory<T>> factory = null)
        {
            var f = new FacetFactory<T>(path);
            factory?.Invoke(f);

            return AndAggregateBy(f.Facet);
        }

        public IAggregationQuery<T> AndAggregateBy(Facet facet)
        {
            var expression = _convertExpressionIfNecessary(_source);
            _source = _source.Provider.CreateQuery<T>(Expression.Call(null, _aggregateByMethod.MakeGenericMethod(typeof(T)), expression, Expression.Constant(facet)));
            return this;
        }

        protected override IndexQuery GetIndexQuery(bool isAsync)
        {
            var inspector = (IRavenQueryInspector)_source;
            return inspector.GetIndexQuery(isAsync);
        }

        protected override void InvokeAfterQueryExecuted(QueryResult result)
        {
            var provider = (RavenQueryProvider<T>)_source.Provider;
            provider.InvokeAfterQueryExecuted(result);
        }
    }

    internal abstract class AggregationQueryBase
    {
        private readonly InMemoryDocumentSessionOperations _session;

        protected AggregationQueryBase(InMemoryDocumentSessionOperations session)
        {
            _session = session;
        }

        public Dictionary<string, FacetResult> Execute()
        {
            var command = GetCommand(isAsync: false);

            _session.IncrementRequestCount();
            _session.RequestExecutor.Execute(command, _session.Context);

            return ProcessResults(command.Result, _session.Conventions);
        }

        public async Task<Dictionary<string, FacetResult>> ExecuteAsync()
        {
            var command = GetCommand(isAsync: true);

            _session.IncrementRequestCount();
            await _session.RequestExecutor.ExecuteAsync(command, _session.Context).ConfigureAwait(false);

            return ProcessResults(command.Result, _session.Conventions);
        }

        public Lazy<Dictionary<string, FacetResult>> ExecuteLazy(Action<Dictionary<string, FacetResult>> onEval = null)
        {
            return ((DocumentSession)_session).AddLazyOperation(new LazyAggregationQueryOperation(_session.Conventions, GetIndexQuery(isAsync: false), InvokeAfterQueryExecuted, ProcessResults), onEval);
        }

        public Lazy<Task<Dictionary<string, FacetResult>>> ExecuteLazyAsync(Action<Dictionary<string, FacetResult>> onEval = null)
        {
            return ((AsyncDocumentSession)_session).AddLazyOperation(new LazyAggregationQueryOperation(_session.Conventions, GetIndexQuery(isAsync: true), InvokeAfterQueryExecuted, ProcessResults), onEval);
        }

        protected abstract IndexQuery GetIndexQuery(bool isAsync);

        protected abstract void InvokeAfterQueryExecuted(QueryResult result);

        private static Dictionary<string, FacetResult> ProcessResults(QueryResult queryResult, DocumentConventions conventions)
        {
            var results = new Dictionary<string, FacetResult>();
            foreach (BlittableJsonReaderObject result in queryResult.Results)
            {
                var facetResult = (FacetResult)EntityToBlittable.ConvertToEntity(typeof(FacetResult), "facet/result", result, conventions);
                results[facetResult.Name] = facetResult;
            }

            return results;
        }

        private QueryCommand GetCommand(bool isAsync)
        {
            var iq = GetIndexQuery(isAsync);

            return new QueryCommand(_session.Conventions, iq);
        }
    }
}
