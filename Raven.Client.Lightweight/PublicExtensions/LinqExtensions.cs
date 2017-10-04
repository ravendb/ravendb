//-----------------------------------------------------------------------
// <copyright file="LinqExtensions.cs" company="Hibernating Rhinos LTD">
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
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Client.Document.Async;
using Raven.Client.Document.Batches;
using Raven.Client.Linq;

namespace Raven.Client
{
    ///<summary>
    /// Extensions to the linq syntax
    ///</summary>
    public static class LinqExtensions
    {
        /// <summary>
        /// Includes the specified path in the query, loading the document specified in that path
        /// </summary>
        /// <typeparam name="TResult">The type of the object that holds the id that you want to include.</typeparam>
        /// <param name="source">The source for querying</param>
        /// <param name="path">The path, which is name of the property that holds the id of the object to include.</param>
        /// <returns></returns>
        public static IRavenQueryable<TResult> Include<TResult>(this IRavenQueryable<TResult> source, Expression<Func<TResult, object>> path)
        {
            source.Customize(x => x.Include(path));
            return source;
        }

        /// <summary>
        /// Includes the specified path in the query, loading the document specified in that path
        /// </summary>
        /// <typeparam name="TResult">The type of the object that holds the id that you want to include.</typeparam>
        /// <typeparam name="TInclude">The type of the object that you want to include.</typeparam>
        /// <param name="source">The source for querying</param>
        /// <param name="path">The path, which is name of the property that holds the id of the object to include.</param>
        /// <returns></returns>
        public static IRavenQueryable<TResult> Include<TResult, TInclude>(this IRavenQueryable<TResult> source, Expression<Func<TResult, object>> path)
        {
            source.Customize(x => x.Include<TResult, TInclude>(path));
            return source;
        }

        /// <summary>
        /// Query the facets results for this query using aggregation
        /// </summary>
        public static DynamicAggregationQuery<T> AggregateBy<T>(this IQueryable<T> queryable, string path, string displayName= null)
        {
            return new DynamicAggregationQuery<T>(queryable, path, displayName);
        }

        /// <summary>
        /// Query the facets results for this query using aggregation
        /// </summary>
        public static DynamicAggregationQuery<T> AggregateBy<T>(this IQueryable<T> queryable, Expression<Func<T, object>> path)
        {
            return new DynamicAggregationQuery<T>(queryable, path);
        }

        /// <summary>
        /// Query the facets results for this query using aggregation with a specific display name
        /// </summary>
        public static DynamicAggregationQuery<T> AggregateBy<T>(this IQueryable<T> queryable, Expression<Func<T, object>> path, string displayName)
        {
            return new DynamicAggregationQuery<T>(queryable, path, displayName);
        }

        /// <summary>
        /// Query the facets results for this query using the specified facet document with the given start and pageSize
        /// </summary>
        /// <param name="facetSetupDoc">Name of the FacetSetup document</param>
        /// <param name="start">Start index for paging</param>
        /// <param name="pageSize">Paging PageSize. If set, overrides Facet.MaxResults</param>
        /// <param name="queryable">The queryable interface for the function to be applied to</param>
        public static FacetResults ToFacets<T>( this IQueryable<T> queryable, string facetSetupDoc, int start = 0, int? pageSize = null )
        {
            var ravenQueryInspector = ((IRavenQueryInspector)queryable);
            return ravenQueryInspector.GetFacets(facetSetupDoc, start, pageSize );
        }

        /// <summary>
        /// Transforms the query to the facet query that will allow you to execute multi faceted search
        /// </summary>
        /// <param name="facetSetupDoc">Name of the FacetSetup document</param>
        /// <param name="start">Start index for paging</param>
        /// <param name="pageSize">>Paging PageSize. If set, overrides Facet.MaxResults</param>
        ///  <param name="queryable">The queryable interface for the function to be applied to</param>
        public static FacetQuery ToFacetQuery<T>(this IQueryable<T> queryable, string facetSetupDoc, int start = 0, int? pageSize = null)
        {
            var ravenQueryInspector = ((IRavenQueryInspector)queryable);
            return new FacetQuery
            {
                IndexName = ravenQueryInspector.IndexQueried,
                Query = ravenQueryInspector.GetIndexQuery(false),
                FacetSetupDoc = facetSetupDoc,
                PageStart = start,
                PageSize = pageSize
            };
        }


        /// <summary>
        /// Query the facets results for this query using the specified list of facets with the given start and pageSize
        /// </summary>
        /// <param name="facets">List of facets</param>
        /// <param name="start">Start index for paging</param>
        /// <param name="pageSize">Paging PageSize. If set, overrides Facet.MaxResults</param>
        ///  <param name="queryable">The queryable interface for the function to be applied to</param>
        public static FacetResults ToFacets<T>(this IQueryable<T> queryable, IEnumerable<Facet> facets, int start = 0, int? pageSize = null)
        {
            var facetsList = facets.ToList();

            if (!facetsList.Any())
                throw new ArgumentException("Facets must contain at least one entry", "facets");

            var ravenQueryInspector = ((IRavenQueryInspector)queryable);

            return ravenQueryInspector.GetFacets(facetsList, start, pageSize);
        }

        /// <summary>
        /// Query the facets results for this query using the specified list of facets with the given start and pageSize
        /// </summary>
        /// <param name="facets">List of facets</param>
        /// <param name="start">Start index for paging</param>
        /// <param name="pageSize">Paging PageSize. If set, overrides Facet.MaxResults</param>
        ///  <param name="queryable">The queryable interface for the function to be applied to</param>
        public static FacetQuery ToFacetQuery<T>(this IQueryable<T> queryable, IEnumerable<Facet> facets, int start = 0, int? pageSize = null)
        {
            var facetsList = facets.ToList();

            if (!facetsList.Any())
                throw new ArgumentException("Facets must contain at least one entry", "facets");

            var ravenQueryInspector = ((IRavenQueryInspector)queryable);
            return new FacetQuery
            {
                IndexName = ravenQueryInspector.IndexQueried,
                Query = ravenQueryInspector.GetIndexQuery(false),
                Facets = facetsList,
                PageStart = start,
                PageSize = pageSize
            };
        }

        /// <summary>
        /// Query the facets results for this query using the specified facet document with the given start and pageSize
        /// </summary>
        /// <param name="facetSetupDoc">Name of the FacetSetup document</param>
        /// <param name="start">Start index for paging</param>
        /// <param name="pageSize">Paging PageSize. If set, overrides Facet.MaxResults</param>
        /// <param name="query">The document query interface for the function to be applied to</param>
        public static FacetResults ToFacets<T>(this IDocumentQuery<T> query, string facetSetupDoc, int start = 0, int? pageSize = null)
        {
            var documentQuery = ((DocumentQuery<T>) query);
            return documentQuery.GetFacets(facetSetupDoc, start, pageSize);
        }

        /// <summary>
        /// Query the facets results for this query using the specified list of facets with the given start and pageSize
        /// </summary>
        /// <param name="facets">List of facets</param>
        /// <param name="start">Start index for paging</param>
        /// <param name="pageSize">Paging PageSize. If set, overrides Facet.MaxResults</param>
        /// <param name="query">The document query interface for the function to be applied to</param>
        public static FacetResults ToFacets<T>(this IDocumentQuery<T> query, IEnumerable<Facet> facets, int start = 0, int? pageSize = null)
        {
            var facetsList = facets.ToList();

            if (!facetsList.Any())
                throw new ArgumentException("Facets must contain at least one entry", "facets");

            var documentQuery = ((DocumentQuery<T>)query);

            return documentQuery.GetFacets(facetsList, start, pageSize);
        }

        /// <summary>
        /// Lazily Query the facets results for this query using the specified facet document with the given start and pageSize
        /// </summary>
        /// <param name="facetSetupDoc">Name of the FacetSetup document</param>
        /// <param name="start">Start index for paging</param>
        /// <param name="pageSize">Paging PageSize. If set, overrides Facet.MaxResults</param>
        /// <param name="queryable">The queryable interface for the function to be applied to</param>
        public static Lazy<FacetResults> ToFacetsLazy<T>(this IQueryable<T> queryable, string facetSetupDoc, int start = 0, int? pageSize = null )
        {
            var ravenQueryInspector = ((IRavenQueryInspector)queryable);
            var query = ravenQueryInspector.GetIndexQuery(isAsync: false);

            var lazyOperation = new LazyFacetsOperation( ravenQueryInspector.IndexQueried, facetSetupDoc, query, start, pageSize );

            var documentSession = ((DocumentSession)ravenQueryInspector.Session);
            return documentSession.AddLazyOperation<FacetResults>(lazyOperation, null);
        }



        /// <summary>
        /// LazilyAsync Query the facets results for this query using the specified facet document with the given start and pageSize
        /// </summary>
        /// <param name="facetSetupDoc">Name of the FacetSetup document</param>
        /// <param name="start">Start index for paging</param>
        /// <param name="pageSize">Paging PageSize. If set, overrides Facet.MaxResults</param>
        /// <param name="queryable">The queryable interface for the function to be applied to</param>
        public static Lazy<Task<FacetResults>> ToFacetsLazyAsync<T>(this IQueryable<T> queryable, string facetSetupDoc, int start = 0, int? pageSize = null)
        {
            var ravenQueryInspector = ((IRavenQueryInspector)queryable);
            var query = ravenQueryInspector.GetIndexQuery(true);

            var lazyOperation = new LazyFacetsOperation(ravenQueryInspector.AsyncIndexQueried, facetSetupDoc, query, start, pageSize);

            var documentSession = ((AsyncDocumentSession)ravenQueryInspector.Session);
            return documentSession.AddLazyOperation<FacetResults>(lazyOperation, null);
        }

        /// <summary>
        /// LazilyAsync Query the facets results for this query using the specified facet document with the given start and pageSize
        /// </summary>
        /// <param name="facets">List of facets</param>
        /// <param name="start">Start index for paging</param>
        /// <param name="pageSize">Paging PageSize. If set, overrides Facet.MaxResults</param>
        /// <param name="queryable">The queryable interface for the function to be applied to</param>
        public static Lazy<Task<FacetResults>> ToFacetsLazyAsync<T>(this IQueryable<T> queryable, IEnumerable<Facet> facets, int start = 0, int? pageSize = null)
        {
            var ravenQueryInspector = ((IRavenQueryInspector)queryable);
            var query = ravenQueryInspector.GetIndexQuery(true);

            var lazyOperation = new LazyFacetsOperation(ravenQueryInspector.AsyncIndexQueried, facets.ToList(), query, start, pageSize);

            var documentSession = ((AsyncDocumentSession)ravenQueryInspector.Session);
            return documentSession.AddLazyOperation<FacetResults>(lazyOperation, null);
        }

        /// <summary>
        /// Lazily Query the facets results for this query using the specified list of facets with the given start and pageSize
        /// </summary>
        public static Lazy<FacetResults> ToFacetsLazy<T>(this IQueryable<T> queryable, IEnumerable<Facet> facets, int start = 0, int? pageSize = null)
        {
            var facetsList = facets.ToList();

            if (!facetsList.Any())
                throw new ArgumentException("Facets must contain at least one entry", "facets");

            var ravenQueryInspector = ((IRavenQueryInspector)queryable);
            var query = ravenQueryInspector.GetIndexQuery(isAsync: false);

            var lazyOperation = new LazyFacetsOperation(ravenQueryInspector.IndexQueried, facetsList, query, start, pageSize);

            var documentSession = ((DocumentSession)ravenQueryInspector.Session);
            return documentSession.AddLazyOperation<FacetResults>(lazyOperation, null);
        }

        /// <summary>
        /// Lazily Query the facets results for this query using the specified facet document with the given start and pageSize
        /// </summary>
        /// <param name="facetSetupDoc">Name of the FacetSetup document</param>
        /// <param name="start">Start index for paging</param>
        /// <param name="pageSize">Paging PageSize. If set, overrides Facet.MaxResults</param>
        /// <param name="query">The document query interface for the function to be applied to</param>
        public static Lazy<FacetResults> ToFacetsLazy<T>(this IDocumentQuery<T> query, string facetSetupDoc, int start = 0, int? pageSize = null)
        {
            var indexQuery = query.GetIndexQuery(isAsync: false);
            var documentQuery = ((DocumentQuery<T>)query);

            var lazyOperation = new LazyFacetsOperation(documentQuery.IndexQueried, facetSetupDoc, indexQuery, start, pageSize);

            var documentSession = ((DocumentSession)documentQuery.Session);
            return documentSession.AddLazyOperation<FacetResults>(lazyOperation, null);
        }

        /// <summary>
        /// Lazily Query the facets results for this query using the specified list of facets with the given start and pageSize
        /// </summary>
        /// <param name="facets">List of facets</param>
        /// <param name="start">Start index for paging</param>
        /// <param name="pageSize">Paging PageSize. If set, overrides Facet.MaxResults</param>
        /// <param name="query">The document query interface for the function to be applied to</param>
        public static Lazy<FacetResults> ToFacetsLazy<T>(this IDocumentQuery<T> query, IEnumerable<Facet> facets, int start = 0, int? pageSize = null)
        {
            var facetsList = facets.ToList();

            if (!facetsList.Any())
                throw new ArgumentException("Facets must contain at least one entry", "facets");

            var indexQuery = query.GetIndexQuery(isAsync: false);
            var documentQuery = ((DocumentQuery<T>)query);

            var lazyOperation = new LazyFacetsOperation(documentQuery.IndexQueried, facetsList, indexQuery, start, pageSize);

            var documentSession = ((DocumentSession)documentQuery.Session);
            return documentSession.AddLazyOperation<FacetResults>(lazyOperation, null);
        }

        /// <summary>
        /// Async Query the facets results for this query using the specified facet document with the given start and pageSize
        /// </summary>
        /// <param name="facetSetupDoc">Name of the FacetSetup document</param>
        /// <param name="start">Start index for paging</param>
        /// <param name="pageSize">Paging PageSize. If set, overrides Facet.MaxResults</param>
        /// <param name="queryable">The queryable interface for the function to be applied to</param>
        /// <param name="token">The cancellation token</param>
        public static Task<FacetResults> ToFacetsAsync<T>(this IQueryable<T> queryable, string facetSetupDoc, int start = 0, int? pageSize = null, CancellationToken token = default (CancellationToken))
        {
            var ravenQueryInspector = ((IRavenQueryInspector)queryable);
            return ravenQueryInspector.GetFacetsAsync(facetSetupDoc, start, pageSize, token);
        }

        /// <summary>
        /// Async Query the facets results for this query using the specified list of facets with the given start and pageSize
        /// </summary>
        /// <param name="facets">List of facets</param>
        /// <param name="start">Start index for paging</param>
        /// <param name="pageSize">Paging PageSize. If set, overrides Facet.MaxResults</param>
        /// <param name="queryable">The queryable interface for the function to be applied to</param>
        /// <param name="token">The cancellation token</param>
        public static Task<FacetResults> ToFacetsAsync<T>(this IQueryable<T> queryable, IEnumerable<Facet> facets, int start = 0, int? pageSize = null, CancellationToken token = default (CancellationToken))
        {
            var facetsList = facets.ToList();

            if (!facetsList.Any())
                throw new ArgumentException("Facets must contain at least one entry", "facets");

            var ravenQueryInspector = ((IRavenQueryInspector)queryable);

            return ravenQueryInspector.GetFacetsAsync(facetsList, start, pageSize, token);
        }

        /// <summary>
        /// Async Query the facets results for this query using the specified facet document with the given start and pageSize
        /// </summary>
        /// <param name="facetSetupDoc">Name of the FacetSetup document</param>
        /// <param name="start">Start index for paging</param>
        /// <param name="pageSize">Paging PageSize. If set, overrides Facet.MaxResults</param>
        /// <param name="queryable">The queryable interface for the function to be applied to</param>
        /// <param name="token">The cancellation token</param>
        public static Task<FacetResults> ToFacetsAsync<T>(this IAsyncDocumentQuery<T> queryable, string facetSetupDoc, int start = 0, int? pageSize = null, CancellationToken token = default (CancellationToken))
        {
            return queryable.GetFacetsAsync(facetSetupDoc, start, pageSize, token);
        }

        /// <summary>
        /// Async Query the facets results for this query using the specified list of facets with the given start and pageSize
        /// </summary>
        /// <param name="facets">List of facets</param>
        /// <param name="start">Start index for paging</param>
        /// <param name="pageSize">Paging PageSize. If set, overrides Facet.MaxResults</param>
        /// <param name="queryable">The queryable interface for the function to be applied to</param>
        /// <param name="token">The cancellation token</param>
        public static Task<FacetResults> ToFacetsAsync<T>(this IAsyncDocumentQuery<T> queryable, IEnumerable<Facet> facets, int start = 0, int? pageSize = null, CancellationToken token = default (CancellationToken))
        {
            var facetsList = facets.ToList();
            return queryable.GetFacetsAsync(facetsList, start, pageSize, token);
        }

        /// <summary>
        /// Project into a different type.
        /// </summary>
        public static IQueryable<TResult> As<TResult>(this IQueryable queryable)
        {
            var ofType = queryable.OfType<TResult>();
            var results = queryable.Provider.CreateQuery<TResult>(ofType.Expression);
            return results;
        }

        /// <summary>
        /// Partition the query so we can intersect different parts of the query
        /// across different index entries.
        /// </summary>
        public static IRavenQueryable<T> Intersect<T>(this IQueryable<T> self)
        {
#if !DNXCORE50
            var currentMethod = (MethodInfo)MethodBase.GetCurrentMethod();
#else
            var currentMethod = typeof(LinqExtensions).GetMethod("Intersect");
#endif

            Expression expression = self.Expression;
            if (expression.Type != typeof(IRavenQueryable<T>))
            {
                expression = Expression.Convert(expression, typeof(IRavenQueryable<T>));
            }
            var queryable =
                self.Provider.CreateQuery(Expression.Call(null, currentMethod.MakeGenericMethod(typeof(T)), expression));
            return (IRavenQueryable<T>)queryable;
        }

        /// <summary>
        /// Project from index fields (must be stored) into different type. If fields are not stored in index, document fields will be used.
        /// </summary>
        public static IRavenQueryable<TResult> ProjectFromIndexFieldsInto<TResult>(this IQueryable queryable)
        {
            var ofType = queryable.OfType<TResult>();
            var results = queryable.Provider.CreateQuery<TResult>(ofType.Expression);
            var ravenQueryInspector = ((RavenQueryInspector<TResult>)results);

            var membersList = ReflectionUtil.GetPropertiesAndFieldsFor<TResult>(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic).ToList();
            ravenQueryInspector.FieldsToFetch(membersList.Select(x => x.Name));			
            return (IRavenQueryable<TResult>)results;
        }

        /// <summary>
        /// Suggest alternative values for the queried term
        /// </summary>
        public static SuggestionQueryResult Suggest(this IQueryable queryable)
        {
            return Suggest(queryable, new SuggestionQuery());
        }

        /// <summary>
        /// Suggest alternative values for the queried term
        /// </summary>
        public static SuggestionQueryResult Suggest(this IQueryable queryable, SuggestionQuery query)
        {
            var ravenQueryInspector = ((IRavenQueryInspector)queryable);
            SetSuggestionQueryFieldAndTerm(ravenQueryInspector, query);
            return ravenQueryInspector.DatabaseCommands.Suggest(ravenQueryInspector.IndexQueried, query);
        }

        /// <summary>
        /// Lazy Suggest alternative values for the queried term
        /// </summary>
        public static Lazy<SuggestionQueryResult> SuggestLazy(this IQueryable queryable)
        {
            return SuggestLazy(queryable, new SuggestionQuery());
        }

        /// <summary>
        /// Lazy Suggest alternative values for the queried term
        /// </summary>
        public static Lazy<SuggestionQueryResult> SuggestLazy(this IQueryable queryable, SuggestionQuery query)
        {
            var ravenQueryInspector = ((IRavenQueryInspector)queryable);
            SetSuggestionQueryFieldAndTerm(ravenQueryInspector, query);

            var lazyOperation = new LazySuggestOperation(ravenQueryInspector.IndexQueried, query);

            var documentSession = ((DocumentSession)ravenQueryInspector.Session);
            return documentSession.AddLazyOperation<SuggestionQueryResult>(lazyOperation, null);
        }

        /// <summary>
        /// LazyAsync Suggest alternative values for the queried term
        /// </summary>
        public static Lazy<Task<SuggestionQueryResult>> SuggestLazyAsync(this IQueryable queryable)
        {
            return SuggestLazyAsync(queryable, new SuggestionQuery());
        }

        /// <summary>
        /// LazyAsync Suggest alternative values for the queried term
        /// </summary>
        public static Lazy<Task<SuggestionQueryResult>> SuggestLazyAsync(this IQueryable queryable, SuggestionQuery query)
        {
            var ravenQueryInspector = ((IRavenQueryInspector)queryable);
            SetSuggestionQueryFieldAndTerm(ravenQueryInspector, query, true);

            var lazyOperation = new LazySuggestOperation(ravenQueryInspector.AsyncIndexQueried, query);

            var documentSession = ((AsyncDocumentSession)ravenQueryInspector.Session);
            return documentSession.AddLazyOperation<SuggestionQueryResult>(lazyOperation, null);
        }

        private static void SetSuggestionQueryFieldAndTerm(IRavenQueryInspector queryInspector, SuggestionQuery query, bool isAsync = false)
        {
            if (string.IsNullOrEmpty(query.Field) == false && string.IsNullOrEmpty(query.Term) == false)
                return;

            var lastEqualityTerm = queryInspector.GetLastEqualityTerm(isAsync);
            if (lastEqualityTerm.Key == null)
                throw new InvalidOperationException("Could not suggest on a query that doesn't have a single equality check");

            query.Field = lastEqualityTerm.Key;
            query.Term = lastEqualityTerm.Value;
        }

        /// <summary>
        /// Suggest alternative values for the queried term
        /// </summary>
        public static Task<SuggestionQueryResult> SuggestAsync(this IQueryable queryable, SuggestionQuery query, CancellationToken token = default (CancellationToken))
        {
            var ravenQueryInspector = ((IRavenQueryInspector)queryable);
            SetSuggestionQueryFieldAndTerm(ravenQueryInspector, query, true);

            return ravenQueryInspector.AsyncDatabaseCommands.SuggestAsync(ravenQueryInspector.AsyncIndexQueried, query, token);
        }

        /// <summary>
        /// Suggest alternative values for the queried term
        /// </summary>
        public static Task<SuggestionQueryResult> SuggestAsync(this IQueryable queryable, CancellationToken token = default (CancellationToken))
        {
            return SuggestAsync(queryable, new SuggestionQuery(), token);
        }

        /// <summary>
        /// Register the query as a lazy async query in the session and return a lazy async
        /// instance that will evaluate the query only when needed
        /// </summary>
          public static Lazy<Task<IEnumerable<T>>> LazilyAsync<T>(this IQueryable<T> source)  
        {
            return LazilyAsync(source, null);
        }

        /// <summary>
        /// Register the query as a lazy async query in the session and return a lazy async
        /// instance that will evaluate the query only when needed
        /// As well as a function to execute when the value is evaluated
        /// </summary>
    
        public static Lazy<Task<IEnumerable<T>>> LazilyAsync<T>(this IQueryable<T> source, Action<IEnumerable<T>> onEval)  
        {
            var provider = source.Provider as IRavenQueryProvider;
            if (provider == null)
                throw new ArgumentException("You can only use Raven Queryable with Lazily");

            return provider.LazilyAsync(source.Expression, onEval);
        }

        /// <summary>
        /// Register the query as a lazy query in the session and return a lazy
        /// instance that will evaluate the query only when needed
        /// </summary>
        public static Lazy<IEnumerable<T>> Lazily<T>(this IQueryable<T> source)
        {
            return Lazily(source, null);
        }

        /// <summary>
        /// Register the query as a lazy query in the session and return a lazy
        /// instance that will evaluate the query only when needed
        /// As well as a function to execute when the value is evaluated
        /// </summary>
        public static Lazy<IEnumerable<T>> Lazily<T>(this IQueryable<T> source, Action<IEnumerable<T>> onEval)
        {
            var provider = source.Provider as IRavenQueryProvider;
            if (provider == null)
                throw new ArgumentException("You can only use Raven Queryable with Lazily");

            return provider.Lazily(source.Expression, onEval);
        }

        /// <summary>
        /// Register the query as a lazy-count query in the session and return a lazy
        /// instance that will evaluate the query only when needed
        /// </summary>
        public static Lazy<int> CountLazily<T>(this IQueryable<T> source)
        {
            if (source == null)
                throw new ArgumentNullException("source");

            var provider = source.Provider as IRavenQueryProvider;

            if (provider == null)
                throw new InvalidOperationException("CountLazily only be used with IRavenQueryable");

            return provider.CountLazily<T>(source.Expression);
        }


        /// <summary>
        /// Register the query as a lazy-count query in the session and return a lazy
        /// instance that will evaluate the query only when needed
        /// </summary>
        public static Lazy<Task<int>> CountLazilyAsync<T>(this IQueryable<T> source, CancellationToken token = default (CancellationToken))
        {
            if (source == null)
                throw new ArgumentNullException("source");

            var provider = source.Provider as IRavenQueryProvider;

            if (provider == null)
                throw new InvalidOperationException("CountLazily only be used with IRavenQueryable");

            return provider.CountLazilyAsync<T>(source.Expression,token);
        }

        /// <summary>
        /// Returns a list of results for a query asynchronously. 
        /// </summary>
        public static Task<IList<T>> ToListAsync<T>(this IQueryable<T> source,CancellationToken token = default (CancellationToken))
        {
            var provider = source.Provider as IRavenQueryProvider;
            if (provider == null)
                throw new ArgumentException("You can only use Raven Queryable with ToListAsync");

            var documentQuery = provider.ToAsyncDocumentQuery<T>(source.Expression);
            provider.MoveAfterQueryExecuted(documentQuery);
            return documentQuery.ToListAsync(token);
        }

        /// <summary>
        /// Determines whether a sequence contains any elements.
        /// </summary>
        /// 
        /// <typeparam name="TSource">
        /// The type of the elements of source.
        /// </typeparam>
        /// 
        /// <param name="source">
        /// The <see cref="IRavenQueryable{T}"/> that contains the elements to be counted.
        /// </param>
        /// <param name="token">The cancellation token.</param>
        /// 
        /// <returns>
        /// true if the source sequence contains any elements; otherwise, false.
        /// </returns>
        /// 
        /// <exception cref="ArgumentNullException">
        /// source is null.
        /// </exception>
        public static async Task<bool> AnyAsync<TSource>(this IQueryable<TSource> source, CancellationToken token = default (CancellationToken))
        {
            if (source == null)
                throw new ArgumentNullException("source");

            var provider = source.Provider as IRavenQueryProvider;

            if (provider == null)
                throw new InvalidOperationException("AnyAsync only be used with IRavenQueryable");

            var query = provider.ToAsyncDocumentQuery<TSource>(source.Expression)
                                .Take(0);

            provider.MoveAfterQueryExecuted(query);
            RavenQueryStatistics stats;
            query.Statistics(out stats);

            await query.ToListAsync(token).ConfigureAwait(false);

            return stats.TotalResults > 0;
        }

        /// <summary>
        /// Determines whether any element of a sequence satisfies a condition.
        /// </summary>
        /// 
        /// <typeparam name="TSource">
        /// The type of the elements of source.
        /// </typeparam>
        /// 
        /// <param name="source">
        /// The <see cref="IRavenQueryable{T}"/> that contains the elements to be counted.
        /// </param>
        /// 
        /// <param name="predicate">
        /// A function to test each element for a condition.
        /// </param>
        /// 
        /// <param name="token">The cancellation token.</param>
        /// 
        /// <returns>
        /// true if any elements in the source sequence pass the test in the specified
        /// predicate; otherwise, false.
        /// </returns>
        /// 
        /// <exception cref="ArgumentNullException">
        /// source or predicate is null.
        /// </exception>
        public static async Task<bool> AnyAsync<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, bool>> predicate, CancellationToken token = default (CancellationToken))
        {
            if (source == null)
                throw new ArgumentNullException("source");

            var filtered = source.Where(predicate);
            var provider = source.Provider as IRavenQueryProvider;

            if (provider == null)
                throw new InvalidOperationException("AnyAsync only be used with IRavenQueryable");

            var query = provider.ToAsyncDocumentQuery<TSource>(filtered.Expression)
                                .Take(0);

            provider.MoveAfterQueryExecuted(query);

            RavenQueryStatistics stats;
            query.Statistics(out stats);

            await query.ToListAsync(token).ConfigureAwait(false);

            return stats.TotalResults > 0;
        }

        /// <summary>
        /// Returns the number of elements in a sequence.
        /// </summary>
        /// 
        /// <typeparam name="TSource">
        /// The type of the elements of source.
        /// </typeparam>
        /// 
        /// <param name="source">
        /// The <see cref="IRavenQueryable{T}"/> that contains the elements to be counted.
        /// </param>
        /// <param name="token">The cancellation token.</param>
        /// 
        /// <returns>
        /// The number of elements in the input sequence.
        /// </returns>
        /// 
        /// <exception cref="ArgumentNullException">
        /// source is null.
        /// </exception>
        /// 
        /// <exception cref="OverflowException">
        /// The number of elements in source is larger than <see cref="Int32.MaxValue"/>.
        /// </exception>
        public static async Task<int> CountAsync<TSource>(this IQueryable<TSource> source, CancellationToken token = default (CancellationToken))
        {
            if (source == null)
                throw new ArgumentNullException("source");

            var provider = source.Provider as IRavenQueryProvider;

            if (provider == null)
                throw new InvalidOperationException("CountAsync only be used with IRavenQueryable");

            var query = provider.ToAsyncDocumentQuery<TSource>(source.Expression)
                                .Take(0);

            provider.MoveAfterQueryExecuted(query);

            RavenQueryStatistics stats;
            query.Statistics(out stats);

            await query.ToListAsync(token).ConfigureAwait(false);

            return stats.TotalResults;
        }

        /// <summary>
        /// Returns the number of elements in the specified sequence that satisfies a condition.
        /// </summary>
        /// 
        /// <typeparam name="TSource">
        /// The type of the elements of source.
        /// </typeparam>
        /// 
        /// <param name="source">
        /// The <see cref="IRavenQueryable{T}"/> that contains the elements to be counted.
        /// </param>
        /// 
        /// <param name="predicate">
        /// A function to test each element for a condition.
        /// </param>
        /// 
        /// <param name="token">The cancellation token.</param>
        /// 
        /// <returns>
        /// The number of elements in the sequence that satisfies the condition in
        /// the predicate function.
        /// </returns>
        /// 
        /// <exception cref="ArgumentNullException">
        /// source or predicate is null.
        /// </exception>
        /// 
        /// <exception cref="OverflowException">
        /// The number of elements in source is larger than <see cref="Int32.MaxValue"/>.
        /// </exception>
        public static async Task<int> CountAsync<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, bool>> predicate, CancellationToken token = default (CancellationToken))
        {
            if (source == null)
                throw new ArgumentNullException("source");

            var filtered = source.Where(predicate);
            var provider = source.Provider as IRavenQueryProvider;

            if (provider == null)
                throw new InvalidOperationException("CountAsync only be used with IRavenQueryable");

            var query = provider.ToAsyncDocumentQuery<TSource>(filtered.Expression)
                                .Take(0);

            provider.MoveAfterQueryExecuted(query);
            RavenQueryStatistics stats;
            query.Statistics(out stats);

            await query.ToListAsync(token).ConfigureAwait(false);

            return stats.TotalResults;
        }

        /// <summary>
        /// Asynchronously returns the first element of a sequence.
        /// </summary>
        /// 
        /// <typeparam name="TSource">
        /// The type of the elements of source.
        /// </typeparam>
        /// 
        /// <param name="source">
        /// The <see cref="IRavenQueryable{T}"/> to return the first element of.
        /// </param>
        /// <param name="token">The cancellation token.</param>
        /// 
        /// <returns>
        /// The first element in source.
        /// </returns>
        /// 
        /// <exception cref="ArgumentNullException">
        /// source is null.
        /// </exception>
        /// 
        /// <exception cref="InvalidOperationException">
        /// The source sequence is empty or source
        /// is not of type <see cref="IRavenQueryable{T}"/>.
        /// </exception>
        public static async Task<TSource> FirstAsync<TSource>(this IQueryable<TSource> source, CancellationToken token = default (CancellationToken))
        {
            if (source == null)
                throw new ArgumentNullException("source");

            var provider = source.Provider as IRavenQueryProvider;

            if (provider == null)
                throw new InvalidOperationException("FirstAsync only be used with IRavenQueryable");

            var query = provider.ToAsyncDocumentQuery<TSource>(source.Expression)
                                .Take(1);

            provider.MoveAfterQueryExecuted(query);

            var result = await query.ToListAsync(token).ConfigureAwait(false);

            return result.First();
        }

        /// <summary>
        /// Asynchronously returns the first element of a sequence that satisfies a specified condition.
        /// </summary>
        /// 
        /// <typeparam name="TSource">
        /// The type of the elements of source.
        /// </typeparam>
        /// 
        /// <param name="source">
        /// The <see cref="IRavenQueryable{T}"/> to return the first element of.
        /// </param>
        /// 
        /// <param name="predicate">
        /// A function to test each element for a condition.
        /// </param>
        /// <param name="token">The cancellation token.</param>
        /// 
        /// <returns>
        /// The first element in source.
        /// </returns>
        /// 
        /// <exception cref="ArgumentNullException">
        /// source or predicate is null.
        /// </exception>
        /// 
        /// <exception cref="InvalidOperationException">
        /// No element satisfies the condition in predicate,
        /// the source sequence is empty or source
        /// is not of type <see cref="IRavenQueryable{T}"/>.
        /// </exception>
        public static async Task<TSource> FirstAsync<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, bool>> predicate, CancellationToken token = default (CancellationToken))
        {
            if (source == null)
                throw new ArgumentNullException("source");

            var filtered = source.Where(predicate);
            var provider = source.Provider as IRavenQueryProvider;

            if (provider == null)
                throw new InvalidOperationException("FirstAsync only be used with IRavenQueryable");

            var query = provider.ToAsyncDocumentQuery<TSource>(filtered.Expression)
                                .Take(1);

            provider.MoveAfterQueryExecuted(query);

            var result = await query.ToListAsync(token).ConfigureAwait(false);

            return result.First();
        }

        /// <summary>
        /// Asynchronously returns the first element of a sequence, or a default value if the sequence contains no elements.
        /// </summary>
        /// 
        /// <typeparam name="TSource">
        /// The type of the elements of source.
        /// </typeparam>
        /// 
        /// <param name="source">
        /// The <see cref="IRavenQueryable{T}"/> to return the first element of.
        /// </param>
        /// <param name="token">The cancellation token.</param>
        /// 
        /// <returns>
        /// default(TSource) if source is empty; otherwise,
        /// the first element in source.
        /// </returns>
        /// 
        /// <exception cref="ArgumentNullException">
        /// source is null.
        /// </exception>
        /// 
        /// <exception cref="InvalidOperationException">
        /// source is not of type <see cref="IRavenQueryable{T}"/>.
        /// </exception>
        public static async Task<TSource> FirstOrDefaultAsync<TSource>(this IQueryable<TSource> source, CancellationToken token = default (CancellationToken))
        {
            if (source == null)
                throw new ArgumentNullException("source");

            var provider = source.Provider as IRavenQueryProvider;

            if (provider == null)
                throw new InvalidOperationException("FirstOrDefaultAsync only be used with IRavenQueryable");

            var query = provider.ToAsyncDocumentQuery<TSource>(source.Expression)
                                .Take(1);

            provider.MoveAfterQueryExecuted(query);

            var result = await query.ToListAsync(token).ConfigureAwait(false);

            return result.FirstOrDefault();
        }

        /// <summary>
        /// Asynchronously returns the first element of a sequence that satisfies a specified
        /// condition or a default value if no such element is found.
        /// </summary>
        /// 
        /// <typeparam name="TSource">
        /// The type of the elements of source.
        /// </typeparam>
        /// 
        /// <param name="source">
        /// The <see cref="IRavenQueryable{T}"/> to return the first element of.
        /// </param>
        /// 
        /// <param name="predicate">
        /// A function to test each element for a condition.
        /// </param>
        /// <param name="token">The cancellation token.</param>
        /// 
        /// <returns>
        /// default(TSource) if source is empty or
        /// if no element passes the test specified by predicate;
        /// otherwise, the first element in source that passes
        /// the test specified by predicate.
        /// </returns>
        /// 
        /// <exception cref="ArgumentNullException">
        /// source or predicate is null.
        /// </exception>
        /// 
        /// <exception cref="InvalidOperationException">
        /// source is not of type <see cref="IRavenQueryable{T}"/>.
        /// </exception>
        public static async Task<TSource> FirstOrDefaultAsync<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, bool>> predicate, CancellationToken token = default (CancellationToken))
        {
            if (source == null)
                throw new ArgumentNullException("source");

            var filtered = source.Where(predicate);
            var provider = source.Provider as IRavenQueryProvider;

            if (provider == null)
                throw new InvalidOperationException("FirstOrDefaultAsync only be used with IRavenQueryable");

            var query = provider.ToAsyncDocumentQuery<TSource>(filtered.Expression)
                                .Take(1);

            provider.MoveAfterQueryExecuted(query);

            var result = await query.ToListAsync(token).ConfigureAwait(false);

            return result.FirstOrDefault();
        }

        /// <summary>
        /// Asynchronously returns the only element of a sequence, and throws an exception if there
        /// is not exactly one element in the sequence.
        /// </summary>
        /// 
        /// <typeparam name="TSource">
        /// The type of the elements of source.
        /// </typeparam>
        /// 
        /// <param name="source">
        /// The <see cref="IRavenQueryable{T}"/> to return the single element of.
        /// </param>
        /// <param name="token">The cancellation token.</param>
        /// 
        /// <returns>
        /// The single element of the input sequence.
        /// </returns>
        /// 
        /// <exception cref="ArgumentNullException">
        /// source is null.
        /// </exception>
        /// 
        /// <exception cref="InvalidOperationException">
        /// The source sequence is empty, has more than one element or
        /// is not of type <see cref="IRavenQueryable{T}"/>.
        /// </exception>
        public static async Task<TSource> SingleAsync<TSource>(this IQueryable<TSource> source, CancellationToken token = default (CancellationToken))
        {
            if (source == null)
                throw new ArgumentNullException("source");

            var provider = source.Provider as IRavenQueryProvider;

            if (provider == null)
                throw new InvalidOperationException("SingleAsync only be used with IRavenQueryable");

            var query = provider.ToAsyncDocumentQuery<TSource>(source.Expression)
                                .Take(2);

            provider.MoveAfterQueryExecuted(query);

            var result = await query.ToListAsync(token).ConfigureAwait(false);

            return result.Single();
        }

        /// <summary>
        /// Asynchronously returns the only element of a sequence, and throws an exception if there
        /// is not exactly one element in the sequence.
        /// </summary>
        /// 
        /// <typeparam name="TSource">
        /// The type of the elements of source.
        /// </typeparam>
        /// 
        /// <param name="source">
        /// The <see cref="IRavenQueryable{T}"/> to return the single element of.
        /// </param>
        /// 
        /// <param name="predicate">
        /// A function to test each element for a condition.
        /// </param>
        /// <param name="token">The cancellation token.</param>
        /// 
        /// <returns>
        /// The single element of the input sequence that satisfies the condition in predicate.
        /// </returns>
        /// 
        /// <exception cref="ArgumentNullException">
        /// source or predicate is null.
        /// </exception>
        /// 
        /// <exception cref="InvalidOperationException">
        /// No element satisfies the condition in predicate, more than
        /// one element satisfies the condition, the source sequence is empty or
        /// source is not of type <see cref="IRavenQueryable{T}"/>.
        /// </exception>
        public static async Task<TSource> SingleAsync<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, bool>> predicate, CancellationToken token = default (CancellationToken))
        {
            if (source == null)
                throw new ArgumentNullException("source");

            var filtered = source.Where(predicate);
            var provider = source.Provider as IRavenQueryProvider;

            if (provider == null)
                throw new InvalidOperationException("SingleAsync only be used with IRavenQueryable");

            var query = provider.ToAsyncDocumentQuery<TSource>(filtered.Expression)
                                .Take(2);

            provider.MoveAfterQueryExecuted(query);

            var result = await query.ToListAsync(token).ConfigureAwait(false);

            return result.Single();
        }

        /// <summary>
        /// Asynchronously returns the only element of a sequence, or a default value if the
        /// sequence is empty; this method throws an exception if there is more than one
        /// element in the sequence.
        /// </summary>
        /// 
        /// <typeparam name="TSource">
        /// The type of the elements of source.
        /// </typeparam>
        /// 
        /// <param name="source">
        /// The <see cref="IRavenQueryable{T}"/> to return the first element of.
        /// </param>
        /// <param name="token">The cancellation token.</param>
        /// 
        /// <returns>
        /// The single element of the input sequence, or default(TSource)
        /// if the sequence contains no elements.
        /// </returns>
        /// 
        /// <exception cref="ArgumentNullException">
        /// source is null.
        /// </exception>
        /// 
        /// <exception cref="InvalidOperationException">
        /// source has more than one element or
        /// is not of type <see cref="IRavenQueryable{T}"/>.
        /// </exception>
        public static async Task<TSource> SingleOrDefaultAsync<TSource>(this IQueryable<TSource> source, CancellationToken token = default (CancellationToken))
        {
            if (source == null)
                throw new ArgumentNullException("source");

            var provider = source.Provider as IRavenQueryProvider;

            if (provider == null)
                throw new InvalidOperationException("SingleOrDefaultAsync only be used with IRavenQueryable");

            var query = provider.ToAsyncDocumentQuery<TSource>(source.Expression)
                                .Take(2);

            provider.MoveAfterQueryExecuted(query);

            var result = await query.ToListAsync(token).ConfigureAwait(false);

            return result.SingleOrDefault();
        }

        /// <summary>
        /// Asynchronously returns the only element of a sequence that satisfies a specified
        /// condition or a default value if no such element exists; this method throws an
        /// exception if more than one element satisfies the condition.
        /// </summary>
        /// 
        /// <typeparam name="TSource">
        /// The type of the elements of source.
        /// </typeparam>
        /// 
        /// <param name="source">
        /// The <see cref="IRavenQueryable{T}"/> to return the first element of.
        /// </param>
        /// 
        /// <param name="predicate">
        /// A function to test each element for a condition.
        /// </param>
        /// 
        /// <param name="token">The cancellation token.</param>
        /// 
        /// <returns>
        /// The single element of the input sequence that satisfies the condition in predicate,
        /// or default(TSource) if no such element is found.
        /// </returns>
        /// 
        /// <exception cref="ArgumentNullException">
        /// source or predicate is null.
        /// </exception>
        /// 
        /// <exception cref="InvalidOperationException">
        /// More than one element satisfies the condition in predicate
        /// or source is not of type <see cref="IRavenQueryable{T}"/>.
        /// </exception>
        public static async Task<TSource> SingleOrDefaultAsync<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, bool>> predicate, CancellationToken token = default (CancellationToken))
        {
            if (source == null)
                throw new ArgumentNullException("source");

            var filtered = source.Where(predicate);
            var provider = source.Provider as IRavenQueryProvider;

            if (provider == null)
                throw new InvalidOperationException("SingleOrDefaultAsync only be used with IRavenQueryable");

            var query = provider.ToAsyncDocumentQuery<TSource>(filtered.Expression)
                                .Take(2);

            provider.MoveAfterQueryExecuted(query);

            var result = await query.ToListAsync(token).ConfigureAwait(false);

            return result.SingleOrDefault();
        }

        /// <summary>
        /// Perform a search for documents which fields that match the searchTerms.
        /// If there is more than a single term, each of them will be checked independently.
        /// </summary>
        public static IRavenQueryable<T> Search<T>(this IQueryable<T> self, Expression<Func<T, object>> fieldSelector, string searchTerms,
                                                   decimal boost = 1,
                                                   SearchOptions options = SearchOptions.Guess,
                                                   EscapeQueryOptions escapeQueryOptions = EscapeQueryOptions.EscapeAll)
        {
#if !DNXCORE50
            var currentMethod = (MethodInfo)MethodBase.GetCurrentMethod();
#else
            var currentMethod = typeof(LinqExtensions).GetMethod("Search");
#endif

            Expression expression = self.Expression;
            if (expression.Type != typeof(IRavenQueryable<T>))
            {
                expression = Expression.Convert(expression, typeof(IRavenQueryable<T>));
            }
            var queryable = self.Provider.CreateQuery(Expression.Call(null, currentMethod.MakeGenericMethod(typeof(T)), expression,
                                                                      fieldSelector,
                                                                      Expression.Constant(searchTerms),
                                                                      Expression.Constant(boost),
                                                                      Expression.Constant(options),
                                                                      Expression.Constant(escapeQueryOptions)));
            return (IRavenQueryable<T>)queryable;
        }

        /// <summary>
        /// Perform an initial sort by lucene score.
        /// </summary>
        public static IOrderedQueryable<T> OrderByScore<T>(this IQueryable<T> self)
        {
#if !DNXCORE50
            var currentMethod = (MethodInfo)MethodBase.GetCurrentMethod();
#else
            var currentMethod = typeof(LinqExtensions).GetMethod("OrderByScore");
#endif

            Expression expression = self.Expression;
            if (expression.Type != typeof(IRavenQueryable<T>))
            {
                expression = Expression.Convert(expression, typeof(IRavenQueryable<T>));
            }
            var queryable = self.Provider.CreateQuery(Expression.Call(null, currentMethod.MakeGenericMethod(typeof(T)), expression));
            return (IOrderedQueryable<T>)queryable;
        }

        /// <summary>
        /// Perform an initial sort by lucene score descending.
        /// </summary>
        public static IOrderedQueryable<T> OrderByScoreDescending<T>(this IQueryable<T> self)
        {
#if !DNXCORE50
            var currentMethod = (MethodInfo)MethodBase.GetCurrentMethod();
#else
            var currentMethod = typeof(LinqExtensions).GetMethod("OrderByScoreDescending");
#endif

            Expression expression = self.Expression;
            if (expression.Type != typeof(IRavenQueryable<T>))
            {
                expression = Expression.Convert(expression, typeof(IRavenQueryable<T>));
            }
            var queryable = self.Provider.CreateQuery(Expression.Call(null, currentMethod.MakeGenericMethod(typeof(T)), expression));
            return (IOrderedQueryable<T>)queryable;
        }
    }
}
