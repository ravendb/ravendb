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

namespace Raven.Client.Documents.Queries.Suggestions
{
    internal class SuggestionQuery<T> : SuggestionQueryBase, ISuggestionQuery<T>
    {
        private IQueryable<T> _source;
        private readonly Func<IQueryable<T>, Expression> _convertExpressionIfNecessary;
        private readonly Func<MethodInfo, Type, MethodInfo> _convertMethodIfNecessary;
        private readonly MethodInfo _suggestUsingMethod;

        public SuggestionQuery(
            IQueryable<T> source,
            Func<IQueryable<T>, Expression> convertExpressionIfNecessary,
            Func<MethodInfo, Type, MethodInfo> convertMethodIfNecessary,
            MethodInfo suggestUsingMethod) : base(((IRavenQueryInspector)source).Session)
        {
            _source = source;
            _convertExpressionIfNecessary = convertExpressionIfNecessary;
            _convertMethodIfNecessary = convertMethodIfNecessary;
            _suggestUsingMethod = suggestUsingMethod;
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

        public ISuggestionQuery<T> AndSuggestUsing(SuggestionBase suggestion)
        {
            var expression = _convertExpressionIfNecessary(_source);
            var method = _convertMethodIfNecessary(_suggestUsingMethod, typeof(T));
            _source = _source.Provider.CreateQuery<T>(Expression.Call(null, method, expression, Expression.Constant(suggestion)));
            return this;
        }

        public ISuggestionQuery<T> AndSuggestUsing(Action<ISuggestionBuilder<T>> builder)
        {
            var f = new SuggestionBuilder<T>();
            builder?.Invoke(f);

            return AndSuggestUsing(f.Suggestion);
        }
    }

    internal abstract class SuggestionQueryBase
    {
        private readonly InMemoryDocumentSessionOperations _session;
        private IndexQuery _query;
        private Stopwatch _duration;

        protected SuggestionQueryBase(InMemoryDocumentSessionOperations session)
        {
            _session = session;
        }

        public Dictionary<string, SuggestionResult> Execute()
        {
            var command = GetCommand(isAsync: false);

            _duration = Stopwatch.StartNew();
            _session.IncrementRequestCount();
            _session.RequestExecutor.Execute(command, _session.Context, sessionInfo: _session._sessionInfo);

            return ProcessResults(command.Result);
        }

        public async Task<Dictionary<string, SuggestionResult>> ExecuteAsync(CancellationToken token = default)
        {
            using (_session.AsyncTaskHolder())
            {
                var command = GetCommand(isAsync: true);

                _duration = Stopwatch.StartNew();
                _session.IncrementRequestCount();
                await _session.RequestExecutor.ExecuteAsync(command, _session.Context, _session._sessionInfo, token).ConfigureAwait(false);

                return ProcessResults(command.Result);
            }
        }

        private Dictionary<string, SuggestionResult> ProcessResults(QueryResult queryResult)
        {
            InvokeAfterQueryExecuted(queryResult);

            var results = new Dictionary<string, SuggestionResult>();
            foreach (BlittableJsonReaderObject result in queryResult.Results)
            {
                var suggestionResult = DocumentConventions.Default.Serialization.DefaultConverter.FromBlittable<SuggestionResult>(result, "suggestion/result");
                results[suggestionResult.Name] = suggestionResult;
            }

            QueryOperation.EnsureIsAcceptable(queryResult, _query.WaitForNonStaleResults, _duration, _session);

            return results;
        }

        public Lazy<Dictionary<string, SuggestionResult>> ExecuteLazy(Action<Dictionary<string, SuggestionResult>> onEval = null)
        {
            _query = GetIndexQuery(isAsync: false);
            return ((DocumentSession)_session).AddLazyOperation(new LazySuggestionQueryOperation(_session, _query, InvokeAfterQueryExecuted, ProcessResults), onEval);
        }

        public Lazy<Task<Dictionary<string, SuggestionResult>>> ExecuteLazyAsync(Action<Dictionary<string, SuggestionResult>> onEval = null, CancellationToken token = default)
        {
            _query = GetIndexQuery(isAsync: true);
            return ((AsyncDocumentSession)_session).AddLazyOperation(new LazySuggestionQueryOperation(_session, _query, InvokeAfterQueryExecuted, ProcessResults), onEval, token);
        }

        protected abstract IndexQuery GetIndexQuery(bool isAsync, bool updateAfterQueryExecuted = true);

        protected abstract void InvokeAfterQueryExecuted(QueryResult result);

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
