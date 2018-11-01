//-----------------------------------------------------------------------
// <copyright file="RavenQueryProvider.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.Highlighting;
using Raven.Client.Documents.Session;
using Raven.Client.Extensions;

namespace Raven.Client.Documents.Linq
{
    /// <summary>
    /// An implementation of <see cref="IRavenQueryProvider"/>
    /// </summary>
    internal class RavenQueryProvider<T> : IRavenQueryProvider
    {
        private Action<QueryResult> _afterQueryExecuted;
        private Action<IDocumentQueryCustomization> _customizeQuery;
        private readonly string _indexName;
        private readonly string _collectionName;
        private readonly IDocumentQueryGenerator _queryGenerator;
        private readonly QueryStatistics _queryStatistics;
        private readonly LinqQueryHighlightings _highlightings;
        private readonly bool _isMapReduce;
        private DocumentConventions _conventions;

        /// <summary>
        /// Initializes a new instance of the <see cref="RavenQueryProvider{T}"/> class.
        /// </summary>
        public RavenQueryProvider(
#if FEATURE_HIGHLIGHTING
            QueryHighlightings highlightings,
#endif
            IDocumentQueryGenerator queryGenerator,
            string indexName,
            string collectionName,
            Type originalQueryType,
            QueryStatistics queryStatistics,
            LinqQueryHighlightings highlightings,
            bool isMapReduce, DocumentConventions conventions)
        {
            FieldsToFetch = new HashSet<FieldToFetch>();
            OriginalQueryType = originalQueryType;

            _queryGenerator = queryGenerator;
            _indexName = indexName;
            _collectionName = collectionName;
            _queryStatistics = queryStatistics;
            _highlightings = highlightings;
            _isMapReduce = isMapReduce;
            _conventions = conventions;
        }

        /// <summary>
        /// Gets the name of the index.
        /// </summary>
        /// <value>The name of the index.</value>
        public string IndexName => _indexName;

        /// <summary>
        /// Gets the name of the collection.
        /// </summary>
        /// <value>The name of the collection.</value>
        public string CollectionName => _collectionName;

        /// <summary>
        /// Get the query generator
        /// </summary>
        public IDocumentQueryGenerator QueryGenerator => _queryGenerator;

        /// <summary>
        /// Gets the actions for customizing the generated lucene query
        /// </summary>
        public Action<IDocumentQueryCustomization> CustomizeQuery => _customizeQuery;

        /// <summary>
        /// Set the fields to fetch
        /// </summary>
        public HashSet<FieldToFetch> FieldsToFetch { get; }

        public Type OriginalQueryType { get; }

        /// <summary>
        /// Change the result type for the query provider
        /// </summary>
        public IRavenQueryProvider For<TS>()
        {
            if (typeof(T) == typeof(TS))
                return this;

            var ravenQueryProvider = new RavenQueryProvider<TS>(
                _queryGenerator,
                _indexName,
                _collectionName,
                OriginalQueryType,
                _queryStatistics,
                _highlightings,
                _isMapReduce,
                _conventions);

            ravenQueryProvider.Customize(_customizeQuery);

            return ravenQueryProvider;
        }

        /// <summary>
        /// Executes the query represented by a specified expression tree.
        /// </summary>
        /// <param name="expression">An expression tree that represents a LINQ query.</param>
        /// <returns>
        /// The value that results from executing the specified query.
        /// </returns>
        public virtual object Execute(Expression expression)
        {
            return GetQueryProviderProcessor<T>().Execute(expression);
        }

        IQueryable<TS> IQueryProvider.CreateQuery<TS>(Expression expression)
        {
            var a = _queryGenerator.CreateRavenQueryInspector<TS>();

            a.Init(
                this,
                _queryStatistics,
                _highlightings,
                _indexName,
                _collectionName,
                expression,
                _queryGenerator.Session,
                _isMapReduce);

            return a;
        }

        IQueryable IQueryProvider.CreateQuery(Expression expression)
        {
            var elementType = ReflectionExtensions.GetElementType(expression.Type);
            try
            {
                var queryInspectorGenericType = typeof(RavenQueryInspector<>).MakeGenericType(elementType);

                InMemoryDocumentSessionOperations realSession = null;

                switch (_queryGenerator)
                {
                    case DocumentSession documentSession:
                        realSession = documentSession;
                        break;
                    case AsyncDocumentSession asyncDocumentSession:
                        realSession = asyncDocumentSession;
                        break;
                    case DocumentQuery<T> query:
                        realSession = query.Session as InMemoryDocumentSessionOperations;
                        break;
                    case AsyncDocumentQuery<T> asyncQuery:
                        realSession = asyncQuery.AsyncSession as InMemoryDocumentSessionOperations;
                        break;
                    default:
                        ThrowQueryGeneratorCastIsNotSupported();
                        break;
                }

                var args = new object[]
                {
                    this,
                    _queryStatistics,
                    _highlightings,
                    _indexName,
                    _collectionName,
                    expression,
                    realSession,
                    _isMapReduce
                };
                var queryInspectorInstance = Activator.CreateInstance(queryInspectorGenericType);
                var methodInfo = queryInspectorGenericType.GetMethod(nameof(RavenQueryInspector<T>.Init));
                methodInfo.Invoke(queryInspectorInstance, args);
                return (IQueryable)queryInspectorInstance;
            }
            catch (TargetInvocationException tie)
            {
                throw tie.InnerException;
            }
        }

        private void ThrowQueryGeneratorCastIsNotSupported()
        {
            throw new NotSupportedException($"Current operation is not supported, please use another query cast. {_queryGenerator?.GetType()?.FullName} cannot be casted to IDocumentSession");
        }

        /// <summary>
        /// Executes the specified expression.
        /// </summary>
        /// <typeparam name="TS"></typeparam>
        /// <param name="expression">The expression.</param>
        /// <returns></returns>
        TS IQueryProvider.Execute<TS>(Expression expression)
        {
            return (TS)Execute(expression);
        }

        /// <summary>
        /// Executes the query represented by a specified expression tree.
        /// </summary>
        /// <param name="expression">An expression tree that represents a LINQ query.</param>
        /// <returns>
        /// The value that results from executing the specified query.
        /// </returns>
        object IQueryProvider.Execute(Expression expression)
        {
            return Execute(expression);
        }

        /// <summary>
        /// Callback to get the results of the query
        /// </summary>
        public void AfterQueryExecuted(Action<QueryResult> afterQueryExecutedCallback)
        {
            _afterQueryExecuted = afterQueryExecutedCallback;
        }

        public void InvokeAfterQueryExecuted(QueryResult result)
        {
            _afterQueryExecuted?.Invoke(result);
        }

        /// <summary>
        /// Customizes the query using the specified action
        /// </summary>
        /// <param name="action">The action.</param>
        public virtual void Customize(Action<IDocumentQueryCustomization> action)
        {
            if (action == null)
                return;
            _customizeQuery += action;
        }

        /// <summary>
        /// Convert the expression to a Lucene query
        /// </summary>
        public IAsyncDocumentQuery<TResult> ToAsyncDocumentQuery<TResult>(Expression expression)
        {
            var processor = GetQueryProviderProcessor<T>();
            var documentQuery = (IAsyncDocumentQuery<TResult>)processor.GetAsyncDocumentQueryFor(expression);

            return documentQuery;
        }

        /// <summary>
        /// Register the query as a lazy query in the session and return a lazy
        /// instance that will evaluate the query only when needed
        /// </summary>
        public Lazy<IEnumerable<TS>> Lazily<TS>(Expression expression, Action<IEnumerable<TS>> onEval)
        {
            var processor = GetQueryProviderProcessor<TS>();
            var query = processor.GetDocumentQueryFor(expression);

            if (FieldsToFetch.Count > 0)
            {
                var (fields, projections) = processor.GetProjections();
                query = query.SelectFields<TS>(new QueryData(fields, projections));
            }

            return query.Lazily(onEval);
        }

        /// <summary>
        /// Register the query as a lazy async query in the session and return a lazy async 
        /// instance that will evaluate the query only when needed
        /// </summary>
        public Lazy<Task<IEnumerable<TS>>> LazilyAsync<TS>(Expression expression, Action<IEnumerable<TS>> onEval)
        {
            var processor = GetQueryProviderProcessor<TS>();
            var query = processor.GetAsyncDocumentQueryFor(expression);

            if (FieldsToFetch.Count > 0)
            {
                var (fields, projections) = processor.GetProjections();
                query = query.SelectFields<TS>(new QueryData(fields, projections));
            }

            return query.LazilyAsync(onEval);
        }

        /// <summary>
        /// Register the query as a lazy-count query in the session and return a lazy
        /// instance that will evaluate the query only when needed
        /// </summary>
        public Lazy<int> CountLazily<TS>(Expression expression)
        {
            var processor = GetQueryProviderProcessor<TS>();
            var query = processor.GetDocumentQueryFor(expression);

            return query.CountLazily();
        }

        /// <summary>
        /// Register the query as a lazy-count query in the session and return a lazy
        /// instance that will evaluate the query only when needed
        /// </summary>
        public Lazy<Task<int>> CountLazilyAsync<TS>(Expression expression, CancellationToken token = default(CancellationToken))
        {
            var processor = GetQueryProviderProcessor<TS>();
            var query = processor.GetAsyncDocumentQueryFor(expression);

            return query.CountLazilyAsync(token);
        }

        protected virtual RavenQueryProviderProcessor<TS> GetQueryProviderProcessor<TS>()
        {
            return new RavenQueryProviderProcessor<TS>(
                _queryGenerator,
                _customizeQuery,
                _afterQueryExecuted,
                _highlightings,
                _indexName,
                _collectionName,
                FieldsToFetch,
                _isMapReduce,
                OriginalQueryType,
                _conventions);
        }

        /// <summary>
        /// Convert the expression to a Lucene query
        /// </summary>
        public IDocumentQuery<TResult> ToDocumentQuery<TResult>(Expression expression)
        {
            var processor = GetQueryProviderProcessor<T>();
            var result = (IDocumentQuery<TResult>)processor.GetDocumentQueryFor(expression);

            return result;
        }
    }
}
