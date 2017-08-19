//-----------------------------------------------------------------------
// <copyright file="RavenQueryInspector.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Indexes.Spatial;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.Facets;
using Raven.Client.Documents.Queries.Spatial;
using Raven.Client.Documents.Session;
using Raven.Client.Extensions;

namespace Raven.Client.Documents.Linq
{
    /// <summary>
    /// Implements <see cref="IRavenQueryable{T}"/>
    /// </summary>
    public class RavenQueryInspector<T> : IRavenQueryable<T>, IRavenQueryInspector
    {
        private Expression _expression;
        private IRavenQueryProvider _provider;
        private QueryStatistics _queryStats;
        private QueryHighlightings _highlightings;
        private string _indexName;
        private string _collectionName;
        private InMemoryDocumentSessionOperations _session;
        private bool _isMapReduce;

        /// <summary>
        /// Initializes a new instance of the <see cref="RavenQueryInspector{T}"/> class.
        /// </summary>
        public void Init(
            IRavenQueryProvider provider,
            QueryStatistics queryStats,
            QueryHighlightings highlightings,
            string indexName,
            string collectionName,
            Expression expression,
            InMemoryDocumentSessionOperations session,
            bool isMapReduce)
        {
            _provider = provider?.For<T>() ?? throw new ArgumentNullException(nameof(provider));
            _queryStats = queryStats;
            _highlightings = highlightings;
            _indexName = indexName;
            _collectionName = collectionName;
            _session = session;
            _isMapReduce = isMapReduce;
            _provider.AfterQueryExecuted(AfterQueryExecuted);
            _expression = expression ?? Expression.Constant(this);
        }

        private void AfterQueryExecuted(QueryResult queryResult)
        {
            _queryStats.UpdateQueryStats(queryResult);
            _highlightings.Update(queryResult);
        }

        #region IOrderedQueryable<T> Members

        Expression IQueryable.Expression => _expression;

        Type IQueryable.ElementType => typeof(T);

        IQueryProvider IQueryable.Provider => _provider;

        /// <summary>
        /// Gets the enumerator.
        /// </summary>
        /// <returns></returns>
        public IEnumerator<T> GetEnumerator()
        {
            var execute = _provider.Execute(_expression);
            return ((IEnumerable<T>)execute).GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        #endregion

        /// <summary>
        /// Provide statistics about the query, such as total count of matching records
        /// </summary>
        public IRavenQueryable<T> Statistics(out QueryStatistics stats)
        {
            stats = _queryStats;
            return this;
        }

        /// <summary>
        /// Customizes the query using the specified action
        /// </summary>
        /// <param name="action">The action.</param>
        /// <returns></returns>
        public IRavenQueryable<T> Customize(Action<IDocumentQueryCustomization> action)
        {
            _provider.Customize(action);
            return this;
        }

        public IRavenQueryable<TResult> TransformWith<TResult>(string transformerName)
        {
            _provider.TransformWith(transformerName);
            var res = (IRavenQueryable<TResult>)this.As<TResult>();
            return res;
        }

        public IRavenQueryable<T> AddQueryInput(string input, object value)
        {
            return AddTransformerParameter(input, value);
        }

        public IRavenQueryable<T> AddTransformerParameter(string input, object value)
        {
            _provider.AddTransformerParameter(input, value);
            return this;
        }

        public IRavenQueryable<T> Spatial(Expression<Func<T, object>> path, Func<SpatialCriteriaFactory, SpatialCriteria> clause)
        {
            return Customize(x => x.Spatial(path.ToPropertyPath(), clause));
        }

        public IRavenQueryable<T> OrderByDistance(SpatialSort sortParamsClause)
        {
            if (string.IsNullOrEmpty(sortParamsClause.FieldName))
                return Customize(x => x.SortByDistance(sortParamsClause.Latitude, sortParamsClause.Longitude));

            return Customize(x => x.SortByDistance(sortParamsClause.Latitude, sortParamsClause.Longitude, sortParamsClause.FieldName));
        }

        /// <summary>
        /// Returns a <see cref="System.String"/> that represents this instance.
        /// </summary>
        /// <returns>
        /// A <see cref="System.String"/> that represents this instance.
        /// </returns>
        public override string ToString()
        {
            RavenQueryProviderProcessor<T> ravenQueryProvider = GetRavenQueryProvider();
            string query;
            if ((_session as AsyncDocumentSession) != null)
            {
                var asyncDocumentQuery = ravenQueryProvider.GetAsyncDocumentQueryFor(_expression);
                query = asyncDocumentQuery.GetIndexQuery().ToString();
            }
            else
            {
                var documentQuery = ravenQueryProvider.GetDocumentQueryFor(_expression);
                query = documentQuery.ToString();
            }

            string fields = "";
            if (ravenQueryProvider.FieldsToFetch.Count > 0)
                fields = "<" + string.Join(", ", ravenQueryProvider.FieldsToFetch.Select(x => x.Name).ToArray()) + ">: ";
            return fields + query;
        }

        public IndexQuery GetIndexQuery(bool isAsync = true)
        {
            RavenQueryProviderProcessor<T> ravenQueryProvider = GetRavenQueryProvider();
            if (isAsync == false)
            {
                var documentQuery = ravenQueryProvider.GetDocumentQueryFor(_expression);
                return documentQuery.GetIndexQuery();
            }
            var asyncDocumentQuery = ravenQueryProvider.GetAsyncDocumentQueryFor(_expression);
            return asyncDocumentQuery.GetIndexQuery();
        }

        public virtual FacetedQueryResult GetFacets(string facetSetupDoc, int start, int? pageSize)
        {
            var q = GetIndexQuery(false);
            var query = FacetQuery.Create(q, facetSetupDoc, null, start, pageSize, Session.Conventions);

            var command = new GetFacetsCommand(_session.Conventions, _session.Context, query);
            _session.RequestExecutor.Execute(command, _session.Context);
            return command.Result;
        }

        public virtual FacetedQueryResult GetFacets(List<Facet> facets, int start, int? pageSize)
        {
            var q = GetIndexQuery(false);
            var query = FacetQuery.Create(q, null, facets, start, pageSize, Session.Conventions);
            var command = new GetFacetsCommand(_session.Conventions, _session.Context, query);
            _session.RequestExecutor.Execute(command, _session.Context);
            return command.Result;
        }

        public virtual async Task<FacetedQueryResult> GetFacetsAsync(string facetSetupDoc, int start, int? pageSize, CancellationToken token = default(CancellationToken))
        {
            var q = GetIndexQuery();
            var query = FacetQuery.Create(q, facetSetupDoc, null, start, pageSize, Session.Conventions);

            var command = new GetFacetsCommand(_session.Conventions, _session.Context, query);
            await _session.RequestExecutor.ExecuteAsync(command, _session.Context, token).ConfigureAwait(false);

            return command.Result;
        }

        public virtual async Task<FacetedQueryResult> GetFacetsAsync(List<Facet> facets, int start, int? pageSize, CancellationToken token = default(CancellationToken))
        {
            var q = GetIndexQuery();
            var query = FacetQuery.Create(q, null, facets, start, pageSize, Session.Conventions);

            var command = new GetFacetsCommand(_session.Conventions, _session.Context, query);
            await _session.RequestExecutor.ExecuteAsync(command, _session.Context, token).ConfigureAwait(false);

            return command.Result;
        }

        private RavenQueryProviderProcessor<T> GetRavenQueryProvider()
        {
            return new RavenQueryProviderProcessor<T>(
                _provider.QueryGenerator,
                _provider.CustomizeQuery,
                null,
                _indexName,
                _collectionName,
                new HashSet<FieldToFetch>(),
                _isMapReduce,
                _provider.ResultTransformer,
                _provider.TransformerParameters,
                _provider.OriginalQueryType);
        }

        public string IndexName => _indexName;

        public InMemoryDocumentSessionOperations Session => _session;

        ///<summary>
        /// Get the last equality term for the query
        ///</summary>
        public KeyValuePair<string, object> GetLastEqualityTerm(bool isAsync = false)
        {
            var ravenQueryProvider = new RavenQueryProviderProcessor<T>(
                _provider.QueryGenerator,
                null,
                null,
                _indexName,
                _collectionName,
                new HashSet<FieldToFetch>(),
                _isMapReduce,
                _provider.ResultTransformer,
                _provider.TransformerParameters,
                _provider.OriginalQueryType);

            if (isAsync)
            {
                var asyncDocumentQuery = ravenQueryProvider.GetAsyncDocumentQueryFor(_expression);
                return asyncDocumentQuery.GetLastEqualityTerm(true);
            }

            var documentQuery = ravenQueryProvider.GetDocumentQueryFor(_expression);
            return documentQuery.GetLastEqualityTerm();
        }

        /// <summary>
        /// Set the fields to fetch
        /// </summary>
        public void FieldsToFetch(IEnumerable<string> fields)
        {
            foreach (var field in fields)
            {
                _provider.FieldsToFetch.Add(new FieldToFetch(field, null));
            }
        }
    }
}
