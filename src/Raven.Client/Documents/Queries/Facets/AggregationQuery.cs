using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Session.Operations;
using Raven.Client.Documents.Session.Operations.Lazy;
using Sparrow.Json;

namespace Raven.Client.Documents.Queries.Facets
{
    internal class AggregationQuery<T> : AggregationQueryBase, IAggregationQuery<T>
    {
        private IQueryable<T> _source;

        private readonly Func<IQueryable<T>, Expression> _convertExpressionIfNecessary;
        private readonly Func<MethodInfo, Type, MethodInfo> _convertMethodIfNecessary;
        private readonly MethodInfo _aggregateByMethod;

        public AggregationQuery(
            IQueryable<T> source,
            Func<IQueryable<T>, Expression> convertExpressionIfNecessary,
            Func<MethodInfo, Type, MethodInfo> convertMethodIfNecessary,
            MethodInfo aggregateByMethod) : base(((IRavenQueryInspector)source).Session)
        {
            _source = source;
            _convertExpressionIfNecessary = convertExpressionIfNecessary;
            _convertMethodIfNecessary = convertMethodIfNecessary;
            _aggregateByMethod = aggregateByMethod;
        }

        public IAggregationQuery<T> AndAggregateBy(Action<IFacetBuilder<T>> builder = null)
        {
            var f = new FacetBuilder<T>();
            builder?.Invoke(f);

            return AndAggregateBy(f.Facet);
        }

        public IAggregationQuery<T> AndAggregateBy(FacetBase facet)
        {
            var expression = _convertExpressionIfNecessary(_source);
            var method = _convertMethodIfNecessary(_aggregateByMethod, typeof(T));
            _source = _source.Provider.CreateQuery<T>(Expression.Call(null, method, expression, Expression.Constant(facet)));

            return this;
        }

        protected override IndexQuery GetIndexQuery(bool isAsync, bool updateAfterQueryExecuted = true)
        {
            var inspector = (IRavenQueryInspector)_source;

            if (updateAfterQueryExecuted == false)
            {
                return inspector.GetIndexQuery(isAsync);
            }

            var provider = (RavenQueryProvider<T>)_source.Provider;
            var providersAfterQueryCallback = provider.AfterQueryExecutedCallback;

            if (isAsync == false)
            {
                var documentQuery = ((RavenQueryInspector<T>)inspector).GetDocumentQuery();

                // add provider's AfterQueryExecuted action to documentQuery.AfterQueryExecuted
                documentQuery.AfterQueryExecuted(providersAfterQueryCallback);

                // substitute provider.AfterQueryExecuted with documentQuery.InvokeAfterQueryExecuted
                provider.AfterQueryExecuted(documentQuery.InvokeAfterQueryExecuted);

                return documentQuery.GetIndexQuery();
            }

            var asyncDocumentQuery = ((RavenQueryInspector<T>)inspector).GetAsyncDocumentQuery();

            asyncDocumentQuery.AfterQueryExecuted(providersAfterQueryCallback);

            provider.AfterQueryExecuted(asyncDocumentQuery.InvokeAfterQueryExecuted);

            return asyncDocumentQuery.GetIndexQuery();
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
        private IndexQuery _query;
        private Stopwatch _duration;

        protected AggregationQueryBase(InMemoryDocumentSessionOperations session)
        {
            _session = session;
        }

        public Dictionary<string, FacetResult> Execute()
        {
            var command = GetCommand(isAsync: false);

            _duration = Stopwatch.StartNew();
            _session.IncrementRequestCount();
            _session.RequestExecutor.Execute(command, _session.Context, sessionInfo:_session.SessionInfo);

            return ProcessResults(command.Result, _session.Conventions);
        }

        public async Task<Dictionary<string, FacetResult>> ExecuteAsync(CancellationToken token = default)
        {
            using (_session.AsyncTaskHolder())
            {
                var command = GetCommand(isAsync: true);

                _duration = Stopwatch.StartNew();
                _session.IncrementRequestCount();
                await _session.RequestExecutor.ExecuteAsync(command, _session.Context, _session.SessionInfo, token).ConfigureAwait(false);

                return ProcessResults(command.Result, _session.Conventions);
            }
        }

        public Lazy<Dictionary<string, FacetResult>> ExecuteLazy(Action<Dictionary<string, FacetResult>> onEval = null)
        {
            _query = GetIndexQuery(isAsync: false);
            return ((DocumentSession)_session).AddLazyOperation(new LazyAggregationQueryOperation(_session, _query, InvokeAfterQueryExecuted, ProcessResults), onEval);
        }

        public Lazy<Task<Dictionary<string, FacetResult>>> ExecuteLazyAsync(Action<Dictionary<string, FacetResult>> onEval = null, CancellationToken token = default)
        {
            _query = GetIndexQuery(isAsync: true);
            return ((AsyncDocumentSession)_session).AddLazyOperation(new LazyAggregationQueryOperation(_session, _query, InvokeAfterQueryExecuted, ProcessResults), onEval, token);
        }

        protected abstract IndexQuery GetIndexQuery(bool isAsync, bool updateAfterQueryExecuted = true);

        protected abstract void InvokeAfterQueryExecuted(QueryResult result);

        private Dictionary<string, FacetResult> ProcessResults(QueryResult queryResult, DocumentConventions conventions)
        {
            InvokeAfterQueryExecuted(queryResult);

            var results = new Dictionary<string, FacetResult>();
            foreach (BlittableJsonReaderObject result in queryResult.Results)
            {
                var facetResult = (FacetResult)EntityToBlittable.ConvertToEntity(typeof(FacetResult), "facet/result", result, conventions);
                results[facetResult.Name] = facetResult;
            }

            QueryOperation.EnsureIsAcceptable(queryResult, _query.WaitForNonStaleResults, _duration, _session);

            return results;
        }

        private QueryCommand GetCommand(bool isAsync)
        {
            _query = GetIndexQuery(isAsync);

            return new QueryCommand(_session, _query);
        }

        public override string ToString()
        {
            var iq = GetIndexQuery(_session is AsyncDocumentSession, updateAfterQueryExecuted: false);
            return iq.ToString();
        }
    }
}
