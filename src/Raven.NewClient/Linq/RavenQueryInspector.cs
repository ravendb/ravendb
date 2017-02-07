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
using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Abstractions.Extensions;
using Raven.NewClient.Abstractions.Indexing;
using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Connection;
using Raven.NewClient.Client.Document;

using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Data.Queries;
using Raven.NewClient.Client.Document.Async;
using Raven.NewClient.Client.Indexes;
using Raven.NewClient.Client.Spatial;


namespace Raven.NewClient.Client.Linq
{
    /// <summary>
    /// Implements <see cref="IRavenQueryable{T}"/>
    /// </summary>
    public class RavenQueryInspector<T> : IRavenQueryable<T>, IRavenQueryInspector
    {
        private Expression expression;
        private IRavenQueryProvider provider;
        private RavenQueryStatistics queryStats;
        private RavenQueryHighlightings highlightings;
        private string indexName;
        private InMemoryDocumentSessionOperations session;
        private bool isMapReduce;


        /// <summary>
        /// Initializes a new instance of the <see cref="RavenQueryInspector{T}"/> class.
        /// </summary>
        public void Init(
            IRavenQueryProvider provider,
            RavenQueryStatistics queryStats,
            RavenQueryHighlightings highlightings,
            string indexName,
            Expression expression,
            InMemoryDocumentSessionOperations session
            ,
            bool isMapReduce
            )
        {
            if (provider == null)
            {
                throw new ArgumentNullException("provider");
            }
            this.provider = provider.For<T>();
            this.queryStats = queryStats;
            this.highlightings = highlightings;
            this.indexName = indexName;
            this.session = session;
            this.isMapReduce = isMapReduce;
            this.provider.AfterQueryExecuted(this.AfterQueryExecuted);
            this.expression = expression ?? Expression.Constant(this);
        }

        private void AfterQueryExecuted(QueryResult queryResult)
        {
            this.queryStats.UpdateQueryStats(queryResult);
            this.highlightings.Update(queryResult);
        }

        #region IOrderedQueryable<T> Members

        Expression IQueryable.Expression
        {
            get { return expression; }
        }

        Type IQueryable.ElementType
        {
            get { return typeof(T); }
        }

        IQueryProvider IQueryable.Provider
        {
            get { return provider; }
        }

        /// <summary>
        /// Gets the enumerator.
        /// </summary>
        /// <returns></returns>
        public IEnumerator<T> GetEnumerator()
        {
            var execute = provider.Execute(expression);
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
        public IRavenQueryable<T> Statistics(out RavenQueryStatistics stats)
        {
            stats = queryStats;
            return this;
        }

        /// <summary>
        /// Customizes the query using the specified action
        /// </summary>
        /// <param name="action">The action.</param>
        /// <returns></returns>
        public IRavenQueryable<T> Customize(Action<IDocumentQueryCustomization> action)
        {
            provider.Customize(action);
            return this;
        }

        public IRavenQueryable<TResult> TransformWith<TTransformer, TResult>() where TTransformer : AbstractTransformerCreationTask, new()
        {
            var transformer = new TTransformer();
            provider.TransformWith(transformer.TransformerName);
            var res = (IRavenQueryable<TResult>)this.As<TResult>();
            res.OriginalQueryType = res.OriginalQueryType ?? typeof(T);
            var p = res.Provider as IRavenQueryProvider;
            if (null != p)
                p.OriginalQueryType = res.OriginalQueryType;
            return res;
        }

        public IRavenQueryable<TResult> TransformWith<TResult>(string transformerName)
        {
            provider.TransformWith(transformerName);
            var res = (IRavenQueryable<TResult>)this.As<TResult>();
            res.OriginalQueryType = res.OriginalQueryType ?? typeof(T);
            provider.OriginalQueryType = res.OriginalQueryType;
            var p = res.Provider as IRavenQueryProvider;
            if (null != p)
                p.OriginalQueryType = res.OriginalQueryType;
            return res;
        }

        public IRavenQueryable<T> AddQueryInput(string input, object value)
        {
            return AddTransformerParameter(input, value);
        }

        public IRavenQueryable<T> AddTransformerParameter(string input, object value)
        {
            provider.AddTransformerParameter(input, value);
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

        public Type OriginalQueryType { get; set; }

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
            if ((session as AsyncDocumentSession) != null)
            {
                var asyncDocumentQuery = ravenQueryProvider.GetAsyncDocumentQueryFor(expression);
                query = asyncDocumentQuery.GetIndexQuery(true).ToString();
            }
            else
            {
                var documentQuery = ravenQueryProvider.GetDocumentQueryFor(expression);
                query = documentQuery.ToString();
            }

            string fields = "";
            if (ravenQueryProvider.FieldsToFetch.Count > 0)
                fields = "<" + string.Join(", ", ravenQueryProvider.FieldsToFetch.ToArray()) + ">: ";
            return fields + query;
        }

        public IndexQuery GetIndexQuery(bool isAsync = true)
        {
            RavenQueryProviderProcessor<T> ravenQueryProvider = GetRavenQueryProvider();
            if (isAsync == false)
            {
                var documentQuery = ravenQueryProvider.GetDocumentQueryFor(expression);
                return documentQuery.GetIndexQuery(false);
            }
            var asyncDocumentQuery = ravenQueryProvider.GetAsyncDocumentQueryFor(expression);
            return asyncDocumentQuery.GetIndexQuery(true);
        }

        public virtual FacetedQueryResult GetFacets(string facetSetupDoc, int start, int? pageSize)
        {
            var q = GetIndexQuery(false);
            var query = FacetQuery.Create(indexName, q, facetSetupDoc, null, start, pageSize, q.Conventions);

            var command = new GetFacetsCommand(session.Context, query);
            session.RequestExecuter.Execute(command, session.Context);
            return command.Result;
        }

        public virtual FacetedQueryResult GetFacets(List<Facet> facets, int start, int? pageSize)
        {
            var q = GetIndexQuery(false);
            var query = FacetQuery.Create(indexName, q, null, facets, start, pageSize, q.Conventions);
            var command = new GetFacetsCommand(session.Context, query);
            session.RequestExecuter.Execute(command, session.Context);
            return command.Result;
        }

        public virtual async Task<FacetedQueryResult> GetFacetsAsync(string facetSetupDoc, int start, int? pageSize, CancellationToken token = default(CancellationToken))
        {
            var q = GetIndexQuery();
            var query = FacetQuery.Create(indexName, q, facetSetupDoc, null, start, pageSize, q.Conventions);

            var command = new GetFacetsCommand(session.Context, query);
            await session.RequestExecuter.ExecuteAsync(command, session.Context, token).ConfigureAwait(false);

            return command.Result;
        }

        public virtual async Task<FacetedQueryResult> GetFacetsAsync(List<Facet> facets, int start, int? pageSize, CancellationToken token = default(CancellationToken))
        {
            var q = GetIndexQuery();
            var query = FacetQuery.Create(indexName, q, null, facets, start, pageSize, q.Conventions);

            var command = new GetFacetsCommand(session.Context, query);
            await session.RequestExecuter.ExecuteAsync(command, session.Context, token).ConfigureAwait(false);

            return command.Result;
        }

        private RavenQueryProviderProcessor<T> GetRavenQueryProvider()
        {
            return new RavenQueryProviderProcessor<T>(provider.QueryGenerator, provider.CustomizeQuery, null, null, indexName,
                                                      new HashSet<string>(), new List<RenamedField>(), isMapReduce,
                                                      provider.ResultTransformer, provider.TransformerParameters, OriginalQueryType);
        }

        /// <summary>
        /// Get the name of the index being queried
        /// </summary>
        public string IndexQueried
        {
            get
            {
                var ravenQueryProvider = new RavenQueryProviderProcessor<T>(provider.QueryGenerator, null, null, null, indexName, new HashSet<string>(), new List<RenamedField>(), isMapReduce,
                    provider.ResultTransformer, provider.TransformerParameters, OriginalQueryType);
                var documentQuery = ravenQueryProvider.GetDocumentQueryFor(expression);
                return ((IRavenQueryInspector)documentQuery).IndexQueried;
            }
        }

        /// <summary>
        /// Get the name of the index being queried asynchronously
        /// </summary>
        public string AsyncIndexQueried
        {
            get
            {
                var ravenQueryProvider = new RavenQueryProviderProcessor<T>(provider.QueryGenerator, null, null, null, indexName, new HashSet<string>(), new List<RenamedField>(), isMapReduce,
                    provider.ResultTransformer, provider.TransformerParameters, OriginalQueryType);
                var documentQuery = ravenQueryProvider.GetAsyncDocumentQueryFor(expression);
                return ((IRavenQueryInspector)documentQuery).IndexQueried;
            }
        }

        public InMemoryDocumentSessionOperations Session
        {
            get
            {
                return session;
            }
        }

        ///<summary>
        /// Get the last equality term for the query
        ///</summary>
        public KeyValuePair<string, string> GetLastEqualityTerm(bool isAsync = false)
        {
            var ravenQueryProvider = new RavenQueryProviderProcessor<T>(provider.QueryGenerator, null, null, null, indexName, new HashSet<string>(),
                new List<RenamedField>(), isMapReduce, provider.ResultTransformer, provider.TransformerParameters, OriginalQueryType);
            if (isAsync)
                if (isAsync)
                {
                    var asyncDocumentQuery = ravenQueryProvider.GetAsyncDocumentQueryFor(expression);
                    return ((IRavenQueryInspector)asyncDocumentQuery).GetLastEqualityTerm(true);
                }

            var documentQuery = ravenQueryProvider.GetDocumentQueryFor(expression);
            return ((IRavenQueryInspector)documentQuery).GetLastEqualityTerm();
        }

        /// <summary>
        /// Set the fields to fetch
        /// </summary>
        public void FieldsToFetch(IEnumerable<string> fields)
        {
            foreach (var field in fields)
            {
                provider.FieldsToFetch.Add(field);
            }
        }
    }
}
