//-----------------------------------------------------------------------
// <copyright file="DocumentQuery.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using Raven.NewClient.Abstractions.Spatial;
using System.Threading.Tasks;
using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Client.Connection;
using Raven.NewClient.Client.Linq;
using Raven.Abstractions.Extensions;
using Raven.NewClient.Abstractions.Extensions;
using Raven.NewClient.Abstractions.Indexing;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Spatial;
using Raven.NewClient.Client.Indexing;
using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Data.Queries;
using Raven.NewClient.Client.Util;
using Raven.NewClient.Client.Document.Async;
using Raven.NewClient.Client.Document.Batches;
using Sparrow.Json;

namespace Raven.NewClient.Client.Document
{
    /// <summary>
    ///   A query against a Raven index
    /// </summary>
    public abstract class AbstractDocumentQuery<T, TSelf> : IDocumentQueryCustomization, IRavenQueryInspector, IAbstractDocumentQuery<T>
                                                            where TSelf : AbstractDocumentQuery<T, TSelf>
    {
        protected bool isSpatialQuery;
        protected string spatialFieldName, queryShape;
        protected SpatialUnits? spatialUnits;
        protected SpatialRelation spatialRelation;
        protected double distanceErrorPct;
        private readonly LinqPathProvider linqPathProvider;
        protected Action<IndexQuery> beforeQueryExecutionAction;

        protected readonly HashSet<Type> rootTypes = new HashSet<Type>
        {
            typeof (T)
        };

        static Dictionary<Type, Func<object, string>> implicitStringsCache = new Dictionary<Type, Func<object, string>>();

        /// <summary>
        /// Whatever to negate the next operation
        /// </summary>
        protected bool negate;

        /// <summary>
        /// The index to query
        /// </summary>
        protected readonly string indexName;

        protected Func<IndexQuery, IEnumerable<object>, IEnumerable<object>> transformResultsFunc;

        protected string defaultField;

        private int currentClauseDepth;

        protected KeyValuePair<string, string> lastEquality;

        protected Dictionary<string, object> transformerParameters = new Dictionary<string, object>();

        /// <summary>
        ///   The list of fields to project directly from the results
        /// </summary>
        protected readonly string[] projectionFields;

        /// <summary>
        ///   The list of fields to project directly from the index on the server
        /// </summary>
        protected readonly string[] fieldsToFetch;

        protected bool isMapReduce;
        /// <summary>
        /// The session for this query
        /// </summary>
        protected readonly InMemoryDocumentSessionOperations theSession;

        /// <summary>
        ///   The fields to order the results by
        /// </summary>
        protected string[] orderByFields = new string[0];

        /// <summary>
        /// The fields of dynamic map-reduce query
        /// </summary>
        protected DynamicMapReduceField[] dynamicMapReduceFields = new DynamicMapReduceField[0];

        /// <summary>
        ///   The fields to highlight
        /// </summary>
        protected List<HighlightedField> highlightedFields = new List<HighlightedField>();

        /// <summary>
        ///   Highlighter pre tags
        /// </summary>
        protected string[] highlighterPreTags = new string[0];

        /// <summary>
        ///   Highlighter post tags
        /// </summary>
        protected string[] highlighterPostTags = new string[0];

        /// <summary>
        ///   Highlighter key
        /// </summary>
        protected string highlighterKeyName;

        /// <summary>
        ///   The page size to use when querying the index
        /// </summary>
        protected int? pageSize;

        protected QueryOperation queryOperation;

        /// <summary>
        /// The query to use
        /// </summary>
        protected StringBuilder queryText = new StringBuilder();

        /// <summary>
        ///   which record to start reading from
        /// </summary>
        protected int start;

        private readonly DocumentConvention conventions;
        /// <summary>
        /// Timeout for this query
        /// </summary>
        protected TimeSpan? timeout;
        /// <summary>
        /// Should we wait for non stale results
        /// </summary>
        protected bool theWaitForNonStaleResults;
        /// <summary>
        /// Should we wait for non stale results as of now?
        /// </summary>
        protected bool theWaitForNonStaleResultsAsOfNow;
        /// <summary>
        /// The paths to include when loading the query
        /// </summary>
        protected HashSet<string> includes = new HashSet<string>();

        /// <summary>
        /// Holds the query stats
        /// </summary>
        protected RavenQueryStatistics queryStats = new RavenQueryStatistics();

        /// <summary>
        /// Holds the query highlightings
        /// </summary>
        protected RavenQueryHighlightings highlightings = new RavenQueryHighlightings();

        /// <summary>
        /// The name of the results transformer to use after executing this query
        /// </summary>
        protected string resultsTransformer;

        /// <summary>
        /// Determines if entities should be tracked and kept in memory
        /// </summary>
        protected bool disableEntitiesTracking;

        /// <summary>
        /// Determine if query results should be cached.
        /// </summary>
        protected bool disableCaching;

        /// <summary>
        /// Indicates if detailed timings should be calculated for various query parts (Lucene search, loading documents, transforming results). Default: false
        /// </summary>
        protected bool showQueryTimings;

        /// <summary>
        /// Determine if scores of query results should be explained
        /// </summary>
        protected bool shouldExplainScores;

        /// <summary>
        ///   Get the name of the index being queried
        /// </summary>
        public string IndexQueried
        {
            get { return indexName; }
        }

        /// <summary>
        ///   Get the name of the index being queried
        /// </summary>
        public string AsyncIndexQueried
        {
            get { return indexName; }
        }

        /// <summary>
        /// Gets the document convention from the query session
        /// </summary>
        public DocumentConvention DocumentConvention
        {
            get { return conventions; }
        }

        /// <summary>
        ///   Gets the session associated with this document query
        /// </summary>
        public IDocumentSession Session
        {
            get { return (IDocumentSession)theSession; }
        }

        InMemoryDocumentSessionOperations IRavenQueryInspector.Session
        {
            get { return theSession; }
        }

        public DocumentConvention Conventions => conventions;

        public bool IsDynamicMapReduce => dynamicMapReduceFields.Length > 0;

        protected Action<QueryResult> afterQueryExecutedCallback;
        protected AfterStreamExecutedDelegate afterStreamExecutedCallback;
        protected long? cutoffEtag;

        private int? _defaultTimeout;

        private TimeSpan DefaultTimeout
        {
            get
            {
                if (Debugger.IsAttached) // increase timeout if we are debugging
                    return TimeSpan.FromMinutes(15);

                if (_defaultTimeout.HasValue)
                    return TimeSpan.FromSeconds(_defaultTimeout.Value);

                return TimeSpan.FromSeconds(15);
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AbstractDocumentQuery{T, TSelf}"/> class.
        /// </summary>
        protected AbstractDocumentQuery(InMemoryDocumentSessionOperations theSession,
                                     string indexName,
                                     string[] fieldsToFetch,
                                     string[] projectionFields,
                                     bool isMapReduce)
        {

            this.projectionFields = projectionFields;
            this.fieldsToFetch = fieldsToFetch;
            this.isMapReduce = isMapReduce;
            this.indexName = indexName;
            this.theSession = theSession;

            AfterQueryExecuted(UpdateStatsAndHighlightings);

            conventions = theSession == null ? new DocumentConvention() : theSession.Conventions;
            linqPathProvider = new LinqPathProvider(conventions);

            var timeoutAsString = Environment.GetEnvironmentVariable(Constants.RavenDefaultQueryTimeout);
            int defaultTimeout;
            if (!string.IsNullOrEmpty(timeoutAsString) && int.TryParse(timeoutAsString, out defaultTimeout))
            {
                _defaultTimeout = defaultTimeout;
            }
        }

        private void UpdateStatsAndHighlightings(QueryResult queryResult)
        {
            queryStats.UpdateQueryStats(queryResult);
            highlightings.Update(queryResult);
        }

        /// <summary>
        ///   Initializes a new instance of the <see cref = "IDocumentQuery{T}" /> class.
        /// </summary>
        /// <param name = "other">The other.</param>
        protected AbstractDocumentQuery(AbstractDocumentQuery<T, TSelf> other)
        {
            indexName = other.indexName;
            linqPathProvider = other.linqPathProvider;
            allowMultipleIndexEntriesForSameDocumentToResultTransformer =
                other.allowMultipleIndexEntriesForSameDocumentToResultTransformer;
            projectionFields = other.projectionFields;
            theSession = other.theSession;
            conventions = other.conventions;
            orderByFields = other.orderByFields;
            dynamicMapReduceFields = other.dynamicMapReduceFields;
            pageSize = other.pageSize;
            queryText = other.queryText;
            start = other.start;
            timeout = other.timeout;
            theWaitForNonStaleResults = other.theWaitForNonStaleResults;
            theWaitForNonStaleResultsAsOfNow = other.theWaitForNonStaleResultsAsOfNow;
            includes = other.includes;
            queryStats = other.queryStats;
            defaultOperator = other.defaultOperator;
            defaultField = other.defaultField;
            highlightedFields = other.highlightedFields;
            highlighterPreTags = other.highlighterPreTags;
            highlighterPostTags = other.highlighterPostTags;
            transformerParameters = other.transformerParameters;
            disableEntitiesTracking = other.disableEntitiesTracking;
            disableCaching = other.disableCaching;
            showQueryTimings = other.showQueryTimings;
            shouldExplainScores = other.shouldExplainScores;

            AfterQueryExecuted(UpdateStatsAndHighlightings);
        }

        #region TSelf Members

        /// <summary>
        ///   Includes the specified path in the query, loading the document specified in that path
        /// </summary>
        /// <param name = "path">The path.</param>
        IDocumentQueryCustomization IDocumentQueryCustomization.Include(string path)
        {
            Include(path);
            return this;
        }

        /// <summary>
        ///   EXPERT ONLY: Instructs the query to wait for non stale results for the specified wait timeout.
        ///   This shouldn't be used outside of unit tests unless you are well aware of the implications
        /// </summary>
        /// <param name = "waitTimeout">The wait timeout.</param>
        IDocumentQueryCustomization IDocumentQueryCustomization.WaitForNonStaleResults(TimeSpan waitTimeout)
        {
            WaitForNonStaleResults(waitTimeout);
            return this;
        }

        /// <summary>
        /// When using spatial queries, instruct the query to sort by the distance from the origin point
        /// </summary>
        IDocumentQueryCustomization IDocumentQueryCustomization.SortByDistance()
        {
            OrderBy(Constants.Indexing.Fields.DistanceFieldName);
            return this;
        }

        /// <summary>
        /// When using spatial queries, instruct the query to sort by the distance from the origin point
        /// </summary>
        IDocumentQueryCustomization IDocumentQueryCustomization.SortByDistance(double lat, double lng)
        {
            OrderBy(string.Format("{0};{1};{2}", Constants.Indexing.Fields.DistanceFieldName, lat.ToInvariantString(), lng.ToInvariantString()));
            return this;
        }

        /// <summary>
        /// When using spatial queries, instruct the query to sort by the distance from the origin point
        /// </summary>
        IDocumentQueryCustomization IDocumentQueryCustomization.SortByDistance(double lat, double lng, string sortedFieldName)
        {
            OrderBy(string.Format("{0};{1};{2};{3}", Constants.Indexing.Fields.DistanceFieldName, lat.ToInvariantString(), lng.ToInvariantString(), sortedFieldName));
            return this;
        }

        /// <summary>
        ///   Filter matches to be inside the specified radius
        /// </summary>
        /// <param name = "radius">The radius.</param>
        /// <param name = "latitude">The latitude.</param>
        /// <param name = "longitude">The longitude.</param>
        /// <param name="distErrorPercent">Gets the error distance that specifies how precise the query shape is.</param>
        IDocumentQueryCustomization IDocumentQueryCustomization.WithinRadiusOf(double radius, double latitude, double longitude, double distErrorPercent)
        {
            GenerateQueryWithinRadiusOf(Constants.Indexing.Fields.DefaultSpatialFieldName, radius, latitude, longitude, distErrorPercent);
            return this;
        }

        IDocumentQueryCustomization IDocumentQueryCustomization.WithinRadiusOf(string fieldName, double radius, double latitude, double longitude, double distErrorPercent)
        {
            GenerateQueryWithinRadiusOf(fieldName, radius, latitude, longitude, distErrorPercent);
            return this;
        }

        /// <summary>
        ///   Filter matches to be inside the specified radius
        /// </summary>
        /// <param name = "radius">The radius.</param>
        /// <param name = "latitude">The latitude.</param>
        /// <param name = "longitude">The longitude.</param>
        /// <param name = "radiusUnits">The units of the <paramref name="radius"/></param>
        /// <param name="distErrorPercent">Gets the error distance that specifies how precise the query shape is</param>
        IDocumentQueryCustomization IDocumentQueryCustomization.WithinRadiusOf(double radius, double latitude, double longitude, SpatialUnits radiusUnits, double distErrorPercent)
        {
            GenerateQueryWithinRadiusOf(Constants.Indexing.Fields.DefaultSpatialFieldName, radius, latitude, longitude, distErrorPercent, radiusUnits);
            return this;
        }

        IDocumentQueryCustomization IDocumentQueryCustomization.WithinRadiusOf(string fieldName, double radius, double latitude, double longitude, SpatialUnits radiusUnits, double distErrorPercent)
        {
            GenerateQueryWithinRadiusOf(fieldName, radius, latitude, longitude, distErrorPercent, radiusUnits);
            return this;
        }

        public IDocumentQueryCustomization WithinRadiusOf(string fieldName, double radius, double latitude, double longitude, double distErrorPercent = 0.025)
        {
            GenerateQueryWithinRadiusOf(fieldName, radius, latitude, longitude, distErrorPercent);
            return this;
        }


        IDocumentQueryCustomization IDocumentQueryCustomization.RelatesToShape(string fieldName, string shapeWKT, SpatialRelation rel, double distErrorPercent)
        {
            GenerateSpatialQueryData(fieldName, shapeWKT, rel, distErrorPercent);
            return this;
        }

        IDocumentQueryCustomization IDocumentQueryCustomization.Spatial(string fieldName, Func<SpatialCriteriaFactory, SpatialCriteria> clause)
        {
            var criteria = clause(new SpatialCriteriaFactory());
            GenerateSpatialQueryData(fieldName, criteria);
            return this;
        }

        /// <summary>
        ///   Filter matches to be inside the specified radius
        /// </summary>
        protected TSelf GenerateQueryWithinRadiusOf(string fieldName, double radius, double latitude, double longitude, double distanceErrorPct = 0.025, SpatialUnits? radiusUnits = null)
        {
            return GenerateSpatialQueryData(fieldName, SpatialIndexQuery.GetQueryShapeFromLatLon(latitude, longitude, radius), SpatialRelation.Within, distanceErrorPct, radiusUnits);
        }

        protected TSelf GenerateSpatialQueryData(string fieldName, string shapeWKT, SpatialRelation relation, double distanceErrorPct = 0.025, SpatialUnits? radiusUnits = null)
        {
            isSpatialQuery = true;
            spatialFieldName = fieldName;
            queryShape = new WktSanitizer().Sanitize(shapeWKT);
            spatialRelation = relation;
            this.distanceErrorPct = distanceErrorPct;
            spatialUnits = radiusUnits;
            return (TSelf)this;
        }

        protected TSelf GenerateSpatialQueryData(string fieldName, SpatialCriteria criteria)
        {
            throw new NotImplementedException();
            var wkt = criteria.Shape as string;
            if (wkt == null && criteria.Shape != null)
            {
                var jsonSerializer = DocumentConvention.CreateSerializer();

                /*using (var jsonWriter = new RavenJTokenWriter())
                {
                    var converter = new ShapeConverter();
                    jsonSerializer.Serialize(jsonWriter, criteria.Shape);
                    if (!converter.TryConvert(jsonWriter.Token, out wkt))
                        throw new ArgumentException("Shape");
                }*/
            }

            if (wkt == null)
                throw new ArgumentException("Shape");

            isSpatialQuery = true;
            spatialFieldName = fieldName;
            queryShape = new WktSanitizer().Sanitize(wkt);
            spatialRelation = criteria.Relation;
            this.distanceErrorPct = criteria.DistanceErrorPct;
            return (TSelf)this;
        }

        /// <summary>
        ///   EXPERT ONLY: Instructs the query to wait for non stale results.
        ///   This shouldn't be used outside of unit tests unless you are well aware of the implications
        /// </summary>
        IDocumentQueryCustomization IDocumentQueryCustomization.WaitForNonStaleResults()
        {
            WaitForNonStaleResults();
            return this;
        }

        public void UsingDefaultField(string field)
        {
            defaultField = field;
        }

        public void UsingDefaultOperator(QueryOperator @operator)
        {
            defaultOperator = @operator;
        }

        /// <summary>
        ///   Includes the specified path in the query, loading the document specified in that path
        /// </summary>
        /// <param name = "path">The path.</param>
        IDocumentQueryCustomization IDocumentQueryCustomization.Include<TResult>(Expression<Func<TResult, object>> path)
        {
            var body = path.Body as UnaryExpression;
            if (body != null)
            {
                switch (body.NodeType)
                {
                    case ExpressionType.Convert:
                    case ExpressionType.ConvertChecked:
                        throw new InvalidOperationException("You cannot use Include<TResult> on value type. Please use the Include<TResult, TInclude> overload.");
                }
            }

            Include(path.ToPropertyPath());
            return this;
        }

        public IDocumentQueryCustomization Include<TResult, TInclude>(Expression<Func<TResult, object>> path)
        {
            Include(path.ToPropertyPath());
            return this;
        }

        /// <summary>
        ///   Instruct the query to wait for non stale result for the specified wait timeout.
        /// </summary>
        /// <param name = "waitTimeout">The wait timeout.</param>
        /// <returns></returns>
        public void WaitForNonStaleResults(TimeSpan waitTimeout)
        {
            theWaitForNonStaleResults = true;
            cutoffEtag = null;
            timeout = waitTimeout;
        }

        protected internal QueryOperation InitializeQueryOperation(bool isAsync = false)
        {
            var indexQuery = GetIndexQuery(isAsync: isAsync);

            if (beforeQueryExecutionAction != null)
                beforeQueryExecutionAction(indexQuery);

            return new QueryOperation(theSession,
                indexName,
                indexQuery,
                projectionFields,
                theWaitForNonStaleResults,
                timeout,
                transformResultsFunc,
                includes,
                disableEntitiesTracking);
        }

        public IndexQuery GetIndexQuery(bool isAsync)
        {
            var query = queryText.ToString();
            var indexQuery = GenerateIndexQuery(query);
            return indexQuery;
        }
        public FacetedQueryResult GetFacets(string facetSetupDoc, int facetStart, int? facetPageSize)
        {
            var q = GetIndexQuery(false);
            var query = FacetQuery.Create(indexName, q, facetSetupDoc, null, facetStart, facetPageSize, q.Conventions);

            var command = new GetFacetsCommand(theSession.Context, query);
            theSession.RequestExecuter.Execute(command, theSession.Context);
            return command.Result;
        }

        public FacetedQueryResult GetFacets(List<Facet> facets, int facetStart, int? facetPageSize)
        {
            var q = GetIndexQuery(false);
            var query = FacetQuery.Create(indexName, q, null, facets, facetStart, facetPageSize, q.Conventions);

            var command = new GetFacetsCommand(theSession.Context, query);
            theSession.RequestExecuter.Execute(command, theSession.Context);
            return command.Result;
        }

        public async Task<FacetedQueryResult> GetFacetsAsync(string facetSetupDoc, int facetStart, int? facetPageSize, CancellationToken token = default(CancellationToken))
        {
            var q = GetIndexQuery(true);
            var query = FacetQuery.Create(indexName, q, facetSetupDoc, null, facetStart, facetPageSize, q.Conventions);

            var command = new GetFacetsCommand(theSession.Context, query);
            await theSession.RequestExecuter.ExecuteAsync(command, theSession.Context, token).ConfigureAwait(false);

            return command.Result;
        }

        public async Task<FacetedQueryResult> GetFacetsAsync(List<Facet> facets, int facetStart, int? facetPageSize, CancellationToken token = default(CancellationToken))
        {
            var q = GetIndexQuery(true);
            var query = FacetQuery.Create(indexName, q, null, facets, facetStart, facetPageSize, q.Conventions);

            var command = new GetFacetsCommand(theSession.Context, query);
            await theSession.RequestExecuter.ExecuteAsync(command, theSession.Context, token).ConfigureAwait(false);

            return command.Result;
        }

        /// <summary>
        ///   Gets the query result
        ///   Execute the query the first time that this is called.
        /// </summary>
        /// <value>The query result.</value>
        public QueryResult QueryResult
        {
            get
            {
                InitSync();

                return queryOperation.CurrentQueryResults.CreateSnapshot();
            }
        }

        protected virtual void InitSync()
        {
            if (queryOperation != null)
                return;
            //TODO - We need this??
            //ClearSortHints();

            var beforeQueryExecutedEventArgs = new BeforeQueryExecutedEventArgs(theSession, this);
            theSession.OnBeforeQueryExecutedInvoke(beforeQueryExecutedEventArgs);

            queryOperation = InitializeQueryOperation();
            ExecuteActualQuery();
        }

        protected void ClearSortHints()
        {
            throw new NotImplementedException();
            /*foreach (var key in dbCommands.OperationsHeaders.AllKeys.Where(key => key.StartsWith("SortHint")).ToArray())
            {
                dbCommands.OperationsHeaders.Remove(key);
            }*/
        }

        protected virtual void ExecuteActualQuery()
        {
            using (queryOperation.EnterQueryContext())
            {
                queryOperation.LogQuery();
                var command = queryOperation.CreateRequest();
                theSession.RequestExecuter.Execute(command, theSession.Context);
                queryOperation.SetResult(command.Result);
            }

            InvokeAfterQueryExecuted(queryOperation.CurrentQueryResults);
        }

        /// <summary>
        /// Register the query as a lazy query in the session and return a lazy
        /// instance that will evaluate the query only when needed
        /// </summary>
        public Lazy<IEnumerable<T>> Lazily()
        {
            return Lazily(null);
        }

        /// <summary>
        /// Register the query as a lazy query in the session and return a lazy
        /// instance that will evaluate the query only when needed
        /// </summary>
        public virtual Lazy<IEnumerable<T>> Lazily(Action<IEnumerable<T>> onEval)
        {
            if (queryOperation == null)
            {
                queryOperation = InitializeQueryOperation();
            }

            var lazyQueryOperation = new LazyQueryOperation<T>(queryOperation, afterQueryExecutedCallback);
            return ((DocumentSession)theSession).AddLazyOperation(lazyQueryOperation, onEval);
        }

        /// <summary>
        /// Register the query as a lazy query in the session and return a lazy
        /// instance that will evaluate the query only when needed
        /// </summary>
        public virtual Lazy<Task<IEnumerable<T>>> LazilyAsync(Action<IEnumerable<T>> onEval)
        {
            if (queryOperation == null)
            {
                queryOperation = InitializeQueryOperation();
            }

            var lazyQueryOperation = new LazyQueryOperation<T>(queryOperation, afterQueryExecutedCallback);
            return ((AsyncDocumentSession)theSession).AddLazyOperation(lazyQueryOperation, onEval);
        }


        /// <summary>
        /// Register the query as a lazy-count query in the session and return a lazy
        /// instance that will evaluate the query only when needed
        /// </summary>
        public virtual Lazy<int> CountLazily()
        {

            if (queryOperation == null)
            {
                Take(0);
                queryOperation = InitializeQueryOperation();
            }

            var lazyQueryOperation = new LazyQueryOperation<T>(queryOperation, afterQueryExecutedCallback);

            return ((DocumentSession)theSession).AddLazyCountOperation(lazyQueryOperation);
        }

        /// <summary>
        ///   Gets the query result
        ///   Execute the query the first time that this is called.
        /// </summary>
        /// <value>The query result.</value>
        public async Task<QueryResult> QueryResultAsync(CancellationToken token = default(CancellationToken))
        {
            var result = await InitAsync(token).ConfigureAwait(false);
            return result.CurrentQueryResults.CreateSnapshot();
        }

        protected virtual async Task<QueryOperation> InitAsync(CancellationToken token = default(CancellationToken))
        {
            if (queryOperation != null)
                return queryOperation;
            //TODO - We need this??
            //ClearSortHints();

            var beforeQueryExecutedEventArgs = new BeforeQueryExecutedEventArgs(theSession, this);
            theSession.OnBeforeQueryExecutedInvoke(beforeQueryExecutedEventArgs);

            queryOperation = InitializeQueryOperation(isAsync: true);
            return await ExecuteActualQueryAsync(token).ConfigureAwait(false);
        }

        /// <summary>
        ///   Gets the fields for projection
        /// </summary>
        /// <returns></returns>
        public IEnumerable<string> GetProjectionFields()
        {
            return projectionFields ?? Enumerable.Empty<string>();
        }

        /// <summary>
        /// Order the search results in alphanumeric order
        /// </summary>
        public void AlphaNumericOrdering(string fieldName, bool descending)
        {
            AddOrder(Constants.Indexing.Fields.AlphaNumericFieldName + ";" + fieldName, descending);
        }

        /// <summary>
        /// Order the search results randomly
        /// </summary>
        public void RandomOrdering()
        {
            AddOrder(Constants.Indexing.Fields.RandomFieldName + ";" + Guid.NewGuid(), false);
        }

        /// <summary>
        /// Order the search results randomly using the specified seed
        /// this is useful if you want to have repeatable random queries
        /// </summary>
        public void RandomOrdering(string seed)
        {
            AddOrder(Constants.Indexing.Fields.RandomFieldName + ";" + seed, false);
        }

        public void CustomSortUsing(string typeName, bool descending)
        {
            AddOrder(Constants.Indexing.Fields.CustomSortFieldName + ";" + typeName, descending);
        }

        public IDocumentQueryCustomization BeforeQueryExecution(Action<IndexQuery> action)
        {
            beforeQueryExecutionAction += action;
            return this;
        }

        public IDocumentQueryCustomization TransformResults(Func<IndexQuery, IEnumerable<object>, IEnumerable<object>> resultsTransformer)
        {
            this.transformResultsFunc = resultsTransformer;
            return this;
        }

        IDocumentQueryCustomization IDocumentQueryCustomization.Highlight(
            string fieldName, int fragmentLength, int fragmentCount, string fragmentsField)
        {
            this.Highlight(fieldName, fragmentLength, fragmentCount, fragmentsField);
            return this;
        }

        IDocumentQueryCustomization IDocumentQueryCustomization.Highlight(
            string fieldName, int fragmentLength, int fragmentCount, out FieldHighlightings fieldHighlightings)
        {
            this.Highlight(fieldName, fragmentLength, fragmentCount, out fieldHighlightings);
            return this;
        }

        IDocumentQueryCustomization IDocumentQueryCustomization.Highlight(
            string fieldName, string fieldKeyName, int fragmentLength, int fragmentCount, out FieldHighlightings fieldHighlightings)
        {
            this.Highlight(fieldName, fieldKeyName, fragmentLength, fragmentCount, out fieldHighlightings);
            return this;
        }

        IDocumentQueryCustomization IDocumentQueryCustomization.SetAllowMultipleIndexEntriesForSameDocumentToResultTransformer(bool val)
        {
            this.SetAllowMultipleIndexEntriesForSameDocumentToResultTransformer(val);
            return this;
        }

        public void SetOriginalQueryType(Type originalType)
        {
            this.originalType = originalType;
        }

        public void AddMapReduceField(DynamicMapReduceField field)
        {
            isMapReduce = true;

            dynamicMapReduceFields = dynamicMapReduceFields.Concat(new[] { field }).ToArray();
        }

        public DynamicMapReduceField[] GetGroupByFields()
        {
            return dynamicMapReduceFields.Where(x => x.IsGroupBy).ToArray();
        }

        IDocumentQueryCustomization IDocumentQueryCustomization.SetHighlighterTags(string preTag, string postTag)
        {
            this.SetHighlighterTags(preTag, postTag);
            return this;
        }

        IDocumentQueryCustomization IDocumentQueryCustomization.SetHighlighterTags(string[] preTags, string[] postTags)
        {
            this.SetHighlighterTags(preTags, postTags);
            return this;
        }

        public IDocumentQueryCustomization NoTracking()
        {
            disableEntitiesTracking = true;
            return this;
        }

        public IDocumentQueryCustomization NoCaching()
        {
            disableCaching = true;
            return this;
        }

        public IDocumentQueryCustomization ShowTimings()
        {
            showQueryTimings = true;
            return this;
        }

        public void SetHighlighterTags(string preTag, string postTag)
        {
            this.SetHighlighterTags(new[] { preTag }, new[] { postTag });
        }

        /// <summary>
        ///   Adds an ordering for a specific field to the query
        /// </summary>
        /// <param name = "fieldName">Name of the field.</param>
        /// <param name = "descending">if set to <c>true</c> [descending].</param>
        public void AddOrder(string fieldName, bool descending)
        {
            AddOrder(fieldName, descending, null);
        }

        /// <summary>
        ///   Adds an ordering for a specific field to the query and specifies the type of field for sorting purposes
        /// </summary>
        /// <param name = "fieldName">Name of the field.</param>
        /// <param name = "descending">if set to <c>true</c> [descending].</param>
        /// <param name = "fieldType">the type of the field to be sorted.</param>
        public void AddOrder(string fieldName, bool descending, Type fieldType)
        {
            fieldName = EnsureValidFieldName(new WhereParams
            {
                FieldName = fieldName
            });
            fieldName = descending ? "-" + fieldName : fieldName;
            orderByFields = orderByFields.Concat(new[] { fieldName }).ToArray();
        }

        public void Highlight(string fieldName, int fragmentLength, int fragmentCount, string fragmentsField)
        {
            highlightedFields.Add(new HighlightedField(fieldName, fragmentLength, fragmentCount, fragmentsField));
        }

        public void Highlight(string fieldName, int fragmentLength, int fragmentCount, out FieldHighlightings fieldHighlightings)
        {
            highlightedFields.Add(new HighlightedField(fieldName, fragmentLength, fragmentCount, null));
            fieldHighlightings = highlightings.AddField(fieldName);
        }

        public void Highlight(string fieldName, string fieldKeyName, int fragmentLength, int fragmentCount, out FieldHighlightings fieldHighlightings)
        {
            highlighterKeyName = fieldKeyName;
            highlightedFields.Add(new HighlightedField(fieldName, fragmentLength, fragmentCount, null));
            fieldHighlightings = highlightings.AddField(fieldName);
        }

        public void SetHighlighterTags(string[] preTags, string[] postTags)
        {
            highlighterPreTags = preTags;
            highlighterPostTags = postTags;
        }

        /// <summary>
        ///   Gets the enumerator.
        /// </summary>
        public virtual IEnumerator<T> GetEnumerator()
        {
            InitSync();

            return queryOperation.Complete<T>().GetEnumerator();
        }

        private Task<Tuple<QueryResult, IList<T>>> ProcessEnumerator(QueryOperation currentQueryOperation)
        {
            var list = currentQueryOperation.Complete<T>();
            return Task.FromResult(Tuple.Create(currentQueryOperation.CurrentQueryResults, list));
        }

        /// <summary>
        ///   Includes the specified path in the query, loading the document specified in that path
        /// </summary>
        /// <param name = "path">The path.</param>
        public void Include(string path)
        {
            includes.Add(path);
        }

        /// <summary>
        ///   This function exists solely to forbid in memory where clause on IDocumentQuery, because
        ///   that is nearly always a mistake.
        /// </summary>
        [Obsolete(
            @"
You cannot issue an in memory filter - such as Where(x=>x.Name == ""Ayende"") - on IDocumentQuery. 
This is likely a bug, because this will execute the filter in memory, rather than in RavenDB.
Consider using session.Query<T>() instead of session.Advanced.DocumentQuery<T>. The session.Query<T>() method fully supports Linq queries, while session.Advanced.DocumentQuery<T>() is intended for lower level API access.
If you really want to do in memory filtering on the data returned from the query, you can use: session.Advanced.DocumentQuery<T>().ToList().Where(x=>x.Name == ""Ayende"")
"
            , true)]
        public IEnumerable<T> Where(Func<T, bool> predicate)
        {
            throw new NotSupportedException();
        }


        /// <summary>
        ///   This function exists solely to forbid in memory where clause on IDocumentQuery, because
        ///   that is nearly always a mistake.
        /// </summary>
        [Obsolete(
            @"
You cannot issue an in memory filter - such as Count(x=>x.Name == ""Ayende"") - on IDocumentQuery. 
This is likely a bug, because this will execute the filter in memory, rather than in RavenDB.
Consider using session.Query<T>() instead of session.Advanced.DocumentQuery<T>. The session.Query<T>() method fully supports Linq queries, while session.Advanced.DocumentQuery<T>() is intended for lower level API access.
If you really want to do in memory filtering on the data returned from the query, you can use: session.Advanced.DocumentQuery<T>().ToList().Count(x=>x.Name == ""Ayende"")
"
            , true)]
        public int Count(Func<T, bool> predicate)
        {
            throw new NotSupportedException();
        }

        /// <summary>
        ///   This function exists solely to forbid in memory where clause on IDocumentQuery, because
        ///   that is nearly always a mistake.
        /// </summary>
        [Obsolete(
            @"
You cannot issue an in memory filter - such as Count() - on IDocumentQuery. 
This is likely a bug, because this will execute the filter in memory, rather than in RavenDB.
Consider using session.Query<T>() instead of session.Advanced.DocumentQuery<T>. The session.Query<T>() method fully supports Linq queries, while session.Advanced.DocumentQuery<T>() is intended for lower level API access.
If you really want to do in memory filtering on the data returned from the query, you can use: session.Advanced.DocumentQuery<T>().ToList().Count()
"
            , true)]
        public int Count()
        {
            throw new NotSupportedException();
        }

        /// <summary>
        ///   Includes the specified path in the query, loading the document specified in that path
        /// </summary>
        /// <param name = "path">The path.</param>
        public void Include(Expression<Func<T, object>> path)
        {
            Include(path.ToPropertyPath());
        }

        /// <summary>
        ///   Takes the specified count.
        /// </summary>
        /// <param name = "count">The count.</param>
        /// <returns></returns>
        public void Take(int count)
        {
            pageSize = count;
        }

        /// <summary>
        ///   Skips the specified count.
        /// </summary>
        /// <param name = "count">The count.</param>
        /// <returns></returns>
        public void Skip(int count)
        {
            start = count;
        }

        public T First()
        {
            return ExecuteQueryOperation(1).First();
        }

        public T FirstOrDefault()
        {
            return ExecuteQueryOperation(1).FirstOrDefault();
        }

        public T Single()
        {
            return ExecuteQueryOperation(2).Single();
        }

        public T SingleOrDefault()
        {
            return ExecuteQueryOperation(2).SingleOrDefault();
        }

        private IEnumerable<T> ExecuteQueryOperation(int take)
        {
            if (!pageSize.HasValue || pageSize > take)
                Take(take);

            InitSync();

            return queryOperation.Complete<T>();
        }

        /// <summary>
        ///   Filter the results from the index using the specified where clause.
        /// </summary>
        /// <param name = "whereClause">The where clause.</param>
        public void Where(string whereClause)
        {
            AppendSpaceIfNeeded(queryText.Length > 0 && queryText[queryText.Length - 1] != '(');
            queryText.Append(whereClause);
        }

        private void AppendSpaceIfNeeded(bool shouldAppendSpace)
        {
            if (shouldAppendSpace)
            {
                queryText.Append(" ");
            }
        }

        /// <summary>
        ///   Matches exact value
        /// </summary>
        /// <remarks>
        ///   Defaults to NotAnalyzed
        /// </remarks>
        public void WhereEquals(string fieldName, object value)
        {
            WhereEquals(new WhereParams
            {
                FieldName = fieldName,
                Value = value
            });
        }

        /// <summary>
        ///   Matches exact value
        /// </summary>
        /// <remarks>
        ///   Defaults to allow wildcards only if analyzed
        /// </remarks>
        public void WhereEquals(string fieldName, object value, bool isAnalyzed)
        {
            WhereEquals(new WhereParams
            {
                AllowWildcards = isAnalyzed,
                IsAnalyzed = isAnalyzed,
                FieldName = fieldName,
                Value = value
            });
        }


        /// <summary>
        ///   Simplified method for opening a new clause within the query
        /// </summary>
        /// <returns></returns>
        public void OpenSubclause()
        {
            currentClauseDepth++;
            AppendSpaceIfNeeded(queryText.Length > 0 && queryText[queryText.Length - 1] != '(');
            NegateIfNeeded();
            queryText.Append("(");
        }

        /// <summary>
        ///   Simplified method for closing a clause within the query
        /// </summary>
        /// <returns></returns>
        public void CloseSubclause()
        {
            currentClauseDepth--;
            queryText.Append(")");
        }

        /// <summary>
        ///   Matches exact value
        /// </summary>
        public void WhereEquals(WhereParams whereParams)
        {
            EnsureValidFieldName(whereParams);

            var transformToEqualValue = TransformToEqualValue(whereParams);
            lastEquality = new KeyValuePair<string, string>(whereParams.FieldName, transformToEqualValue);

            AppendSpaceIfNeeded(queryText.Length > 0 && queryText[queryText.Length - 1] != '(');
            NegateIfNeeded();

            queryText.Append(RavenQuery.EscapeField(whereParams.FieldName));
            queryText.Append(":");
            queryText.Append(transformToEqualValue);
        }

        private string EnsureValidFieldName(WhereParams whereParams)
        {
            if (theSession == null || theSession.Conventions == null || whereParams.IsNestedPath)
                return whereParams.FieldName;

            if (isMapReduce)
            {
                if (IsDynamicMapReduce)
                {
                    DynamicMapReduceField renamedField;

                    if (whereParams.FieldName.EndsWith(Constants.Indexing.Fields.RangeFieldSuffix))
                    {
                        var name = whereParams.FieldName.Substring(0, whereParams.FieldName.Length - 6);

                        renamedField = dynamicMapReduceFields.FirstOrDefault(x => x.ClientSideName == name);

                        if (renamedField != null)
                        {
                            return whereParams.FieldName = renamedField.Name + Constants.Indexing.Fields.RangeFieldSuffix;
                        }
                    }
                    else
                    {
                        renamedField = dynamicMapReduceFields.FirstOrDefault(x => x.ClientSideName == whereParams.FieldName);

                        if (renamedField != null)
                            return whereParams.FieldName = renamedField.Name;
                    }
                }

                return whereParams.FieldName;
            }

            foreach (var rootType in rootTypes)
            {
                var identityProperty = theSession.Conventions.GetIdentityProperty(rootType);
                if (identityProperty != null && identityProperty.Name == whereParams.FieldName)
                {
                    whereParams.FieldTypeForIdentifier = rootType;
                    return whereParams.FieldName = Constants.Indexing.Fields.DocumentIdFieldName;
                }
            }

            return whereParams.FieldName;
        }

        ///<summary>
        /// Negate the next operation
        ///</summary>
        public void NegateNext()
        {
            negate = !negate;
        }

        private void NegateIfNeeded()
        {
            if (negate == false)
                return;
            negate = false;
            queryText.Append("-");
        }

        private IEnumerable<object> UnpackEnumerable(IEnumerable items)
        {
            foreach (var item in items)
            {
                var enumerable = item as IEnumerable;
                if (enumerable != null && item is string == false)
                {
                    foreach (var nested in UnpackEnumerable(enumerable))
                    {
                        yield return nested;
                    }
                }
                else
                {
                    yield return item;
                }
            }
        }

        /// <summary>
        /// Check that the field has one of the specified value
        /// </summary>
        public void WhereIn(string fieldName, IEnumerable<object> values)
        {
            AppendSpaceIfNeeded(queryText.Length > 0 && char.IsWhiteSpace(queryText[queryText.Length - 1]) == false);
            NegateIfNeeded();

            var whereParams = new WhereParams
            {
                FieldName = fieldName
            };
            fieldName = EnsureValidFieldName(whereParams);

            var list = UnpackEnumerable(values).ToList();

            if (list.Count == 0)
            {
                queryText.Append("@emptyIn<")
                    .Append(RavenQuery.EscapeField(fieldName))
                    .Append(">:(no-results)");
                return;
            }

            queryText.Append("@in<")
                .Append(RavenQuery.EscapeField(fieldName))
                .Append(">:(");

            var first = true;
            AddItemToInClause(whereParams, list, first);
            queryText.Append(") ");
        }

        private void AddItemToInClause(WhereParams whereParams, IEnumerable<object> list, bool first)
        {
            foreach (var value in list)
            {
                var enumerable = value as IEnumerable;
                if (enumerable != null && value is string == false)
                {
                    AddItemToInClause(whereParams, enumerable.Cast<object>(), first);
                    return;
                }
                if (first == false)
                {
                    queryText.Append(" , ");
                }
                first = false;
                var nestedWhereParams = new WhereParams
                {
                    AllowWildcards = true,
                    IsAnalyzed = true,
                    FieldName = whereParams.FieldName,
                    FieldTypeForIdentifier = whereParams.FieldTypeForIdentifier,
                    Value = value
                };
                queryText.Append(TransformToEqualValue(nestedWhereParams).Replace(",", "`,`"));
            }
        }

        /// <summary>
        ///   Matches fields which starts with the specified value.
        /// </summary>
        /// <param name = "fieldName">Name of the field.</param>
        /// <param name = "value">The value.</param>
        public void WhereStartsWith(string fieldName, object value)
        {
            // NOTE: doesn't fully match StartsWith semantics
            WhereEquals(
                new WhereParams
                {
                    FieldName = fieldName,
                    Value = String.Concat(value, "*"),
                    IsAnalyzed = true,
                    AllowWildcards = true
                });
        }

        /// <summary>
        ///   Matches fields which ends with the specified value.
        /// </summary>
        /// <param name = "fieldName">Name of the field.</param>
        /// <param name = "value">The value.</param>
        public void WhereEndsWith(string fieldName, object value)
        {
            // http://lucene.apache.org/java/2_4_0/queryparsersyntax.html#Wildcard%20Searches
            // You cannot use a * or ? symbol as the first character of a search

            // NOTE: doesn't fully match EndsWith semantics
            WhereEquals(
                new WhereParams
                {
                    FieldName = fieldName,
                    Value = String.Concat("*", value),
                    AllowWildcards = true,
                    IsAnalyzed = true
                });
        }

        /// <summary>
        ///   Matches fields where the value is between the specified start and end, exclusive
        /// </summary>
        /// <param name = "fieldName">Name of the field.</param>
        /// <param name = "start">The start.</param>
        /// <param name = "end">The end.</param>
        /// <returns></returns>
        public void WhereBetween(string fieldName, object start, object end)
        {
            AppendSpaceIfNeeded(queryText.Length > 0);

            NegateIfNeeded();

            fieldName = GetFieldNameForRangeQueries(fieldName, start, end);

            queryText.Append(RavenQuery.EscapeField(fieldName)).Append(":{");
            queryText.Append(start == null ? "*" : TransformToRangeValue(new WhereParams { Value = start, FieldName = fieldName }));
            queryText.Append(" TO ");
            queryText.Append(end == null ? "NULL" : TransformToRangeValue(new WhereParams { Value = end, FieldName = fieldName }));
            queryText.Append("}");
        }

        /// <summary>
        ///   Matches fields where the value is between the specified start and end, inclusive
        /// </summary>
        /// <param name = "fieldName">Name of the field.</param>
        /// <param name = "start">The start.</param>
        /// <param name = "end">The end.</param>
        /// <returns></returns>
        public void WhereBetweenOrEqual(string fieldName, object start, object end)
        {
            AppendSpaceIfNeeded(queryText.Length > 0);

            NegateIfNeeded();

            fieldName = GetFieldNameForRangeQueries(fieldName, start, end);

            queryText.Append(RavenQuery.EscapeField(fieldName)).Append(":[");
            queryText.Append(start == null ? "*" : TransformToRangeValue(new WhereParams { Value = start, FieldName = fieldName }));
            queryText.Append(" TO ");
            queryText.Append(end == null ? "NULL" : TransformToRangeValue(new WhereParams { Value = end, FieldName = fieldName }));
            queryText.Append("]");
        }

        private string GetFieldNameForRangeQueries(string fieldName, object start, object end)
        {
            fieldName = EnsureValidFieldName(new WhereParams { FieldName = fieldName });

            if (fieldName == Constants.Indexing.Fields.DocumentIdFieldName)
                return fieldName;

            var val = (start ?? end);
            if (conventions.UsesRangeType(val) && !fieldName.EndsWith(Constants.Indexing.Fields.RangeFieldSuffix))
                fieldName = fieldName + Constants.Indexing.Fields.RangeFieldSuffix;
            return fieldName;
        }

        /// <summary>
        ///   Matches fields where the value is greater than the specified value
        /// </summary>
        /// <param name = "fieldName">Name of the field.</param>
        /// <param name = "value">The value.</param>
        public void WhereGreaterThan(string fieldName, object value)
        {
            WhereBetween(fieldName, value, null);
        }

        /// <summary>
        ///   Matches fields where the value is greater than or equal to the specified value
        /// </summary>
        /// <param name = "fieldName">Name of the field.</param>
        /// <param name = "value">The value.</param>
        public void WhereGreaterThanOrEqual(string fieldName, object value)
        {
            WhereBetweenOrEqual(fieldName, value, null);
        }

        /// <summary>
        ///   Matches fields where the value is less than the specified value
        /// </summary>
        /// <param name = "fieldName">Name of the field.</param>
        /// <param name = "value">The value.</param>
        public void WhereLessThan(string fieldName, object value)
        {
            WhereBetween(fieldName, null, value);
        }

        /// <summary>
        ///   Matches fields where the value is less than or equal to the specified value
        /// </summary>
        /// <param name = "fieldName">Name of the field.</param>
        /// <param name = "value">The value.</param>
        public void WhereLessThanOrEqual(string fieldName, object value)
        {
            WhereBetweenOrEqual(fieldName, null, value);
        }

        /// <summary>
        ///   Add an AND to the query
        /// </summary>
        public void AndAlso()
        {
            if (queryText.Length < 1)
                return;

            queryText.Append(" AND");
        }

        /// <summary>
        ///   Add an OR to the query
        /// </summary>
        public void OrElse()
        {
            if (queryText.Length < 1)
                return;

            queryText.Append(" OR");
        }

        /// <summary>
        ///   Specifies a boost weight to the last where clause.
        ///   The higher the boost factor, the more relevant the term will be.
        /// </summary>
        /// <param name = "boost">boosting factor where 1.0 is default, less than 1.0 is lower weight, greater than 1.0 is higher weight</param>
        /// <returns></returns>
        /// <remarks>
        ///   http://lucene.apache.org/java/2_4_0/queryparsersyntax.html#Boosting%20a%20Term
        /// </remarks>
        public void Boost(decimal boost)
        {
            if (queryText.Length < 1)
            {
                throw new InvalidOperationException("Missing where clause");
            }

            if (boost <= 0m)
            {
                throw new ArgumentOutOfRangeException("boost", "Boost factor must be a positive number");
            }

            if (boost != 1m)
            {
                // 1.0 is the default
                queryText.Append("^").Append(boost.ToString(CultureInfo.InvariantCulture));
            }
        }

        /// <summary>
        ///   Specifies a fuzziness factor to the single word term in the last where clause
        /// </summary>
        /// <param name = "fuzzy">0.0 to 1.0 where 1.0 means closer match</param>
        /// <returns></returns>
        /// <remarks>
        ///   http://lucene.apache.org/java/2_4_0/queryparsersyntax.html#Fuzzy%20Searches
        /// </remarks>
        public void Fuzzy(decimal fuzzy)
        {
            if (queryText.Length < 1)
            {
                throw new InvalidOperationException("Missing where clause");
            }

            if (fuzzy < 0m || fuzzy > 1m)
            {
                throw new ArgumentOutOfRangeException("Fuzzy distance must be between 0.0 and 1.0");
            }

            var ch = queryText[queryText.Length - 1];
            if (ch == '"' || ch == ']')
            {
                // this check is overly simplistic
                throw new InvalidOperationException("Fuzzy factor can only modify single word terms");
            }

            queryText.Append("~");
            if (fuzzy != 0.5m)
            {
                // 0.5 is the default
                queryText.Append(fuzzy.ToString(CultureInfo.InvariantCulture));
            }
        }

        /// <summary>
        ///   Specifies a proximity distance for the phrase in the last where clause
        /// </summary>
        /// <param name = "proximity">number of words within</param>
        /// <returns></returns>
        /// <remarks>
        ///   http://lucene.apache.org/java/2_4_0/queryparsersyntax.html#Proximity%20Searches
        /// </remarks>
        public void Proximity(int proximity)
        {
            if (queryText.Length < 1)
            {
                throw new InvalidOperationException("Missing where clause");
            }

            if (proximity < 1)
            {
                throw new ArgumentOutOfRangeException("proximity", "Proximity distance must be a positive number");
            }

            if (queryText[queryText.Length - 1] != '"')
            {
                // this check is overly simplistic
                throw new InvalidOperationException("Proximity distance can only modify a phrase");
            }

            queryText.Append("~").Append(proximity.ToString(CultureInfo.InvariantCulture));
        }

        /// <summary>
        ///   Order the results by the specified fields
        ///   The fields are the names of the fields to sort, defaulting to sorting by ascending.
        ///   You can prefix a field name with '-' to indicate sorting by descending or '+' to sort by ascending
        /// </summary>
        /// <param name = "fields">The fields.</param>
        public void OrderBy(params string[] fields)
        {
            orderByFields = orderByFields.Concat(fields).ToArray();
        }

        /// <summary>
        ///   Order the results by the specified fields
        ///   The fields are the names of the fields to sort, defaulting to sorting by descending.
        ///   You can prefix a field name with '-' to indicate sorting by descending or '+' to sort by ascending
        /// </summary>
        /// <param name = "fields">The fields.</param>
        public void OrderByDescending(params string[] fields)
        {
            fields = fields.Select(MakeFieldSortDescending).ToArray();
            OrderBy(fields);
        }

        protected string MakeFieldSortDescending(string field)
        {
            if (string.IsNullOrWhiteSpace(field) || field.StartsWith("+") || field.StartsWith("-"))
            {
                return field;
            }

            return "-" + field;
        }

        /// <summary>
        ///   Instructs the query to wait for non stale results as of now.
        /// </summary>
        /// <returns></returns>
        public void WaitForNonStaleResultsAsOfNow()
        {
            theWaitForNonStaleResults = true;
            theWaitForNonStaleResultsAsOfNow = true;
            timeout = DefaultTimeout;
        }

        /// <summary>
        ///   Instructs the query to wait for non stale results as of now for the specified timeout.
        /// </summary>
        /// <param name = "waitTimeout">The wait timeout.</param>
        /// <returns></returns>
        IDocumentQueryCustomization IDocumentQueryCustomization.WaitForNonStaleResultsAsOfNow(TimeSpan waitTimeout)
        {
            WaitForNonStaleResultsAsOfNow(waitTimeout);
            return this;
        }

        /// <summary>
        /// Instructs the query to wait for non stale results as of the cutoff etag.
        /// </summary>
        /// <param name="cutOffEtag">The cut off etag.</param>
        IDocumentQueryCustomization IDocumentQueryCustomization.WaitForNonStaleResultsAsOf(long? cutOffEtag)
        {
            WaitForNonStaleResultsAsOf(cutOffEtag);
            return this;
        }

        /// <summary>
        /// Instructs the query to wait for non stale results as of the cutoff etag for the specified timeout.
        /// </summary>
        /// <param name="cutOffEtag">The cut off etag.</param>
        /// <param name="waitTimeout">The wait timeout.</param>
        IDocumentQueryCustomization IDocumentQueryCustomization.WaitForNonStaleResultsAsOf(long? cutOffEtag, TimeSpan waitTimeout)
        {
            WaitForNonStaleResultsAsOf(cutOffEtag, waitTimeout);
            return this;
        }

        /// <summary>
        ///   Instructs the query to wait for non stale results as of now.
        /// </summary>
        /// <returns></returns>
        IDocumentQueryCustomization IDocumentQueryCustomization.WaitForNonStaleResultsAsOfNow()
        {
            WaitForNonStaleResultsAsOfNow();
            return this;
        }

        /// <summary>
        ///   Instructs the query to wait for non stale results as of now for the specified timeout.
        /// </summary>
        /// <param name = "waitTimeout">The wait timeout.</param>
        /// <returns></returns>
        public void WaitForNonStaleResultsAsOfNow(TimeSpan waitTimeout)
        {
            theWaitForNonStaleResults = true;
            theWaitForNonStaleResultsAsOfNow = true;
            timeout = waitTimeout;
        }

        /// <summary>
        /// Instructs the query to wait for non stale results as of the cutoff etag.
        /// </summary>
        public void WaitForNonStaleResultsAsOf(long? cutOffEtag)
        {
            WaitForNonStaleResultsAsOf(cutOffEtag, DefaultTimeout);
        }

        /// <summary>
        /// Instructs the query to wait for non stale results as of the cutoff etag.
        /// </summary>
        public void WaitForNonStaleResultsAsOf(long? cutOffEtag, TimeSpan waitTimeout)
        {
            theWaitForNonStaleResults = true;
            timeout = waitTimeout;
            cutoffEtag = cutOffEtag;
        }

        /// <summary>
        ///   EXPERT ONLY: Instructs the query to wait for non stale results.
        ///   This shouldn't be used outside of unit tests unless you are well aware of the implications
        /// </summary>
        public void WaitForNonStaleResults()
        {
            WaitForNonStaleResults(DefaultTimeout);
        }

        /// <summary>
        /// Provide statistics about the query, such as total count of matching records
        /// </summary>
        public void Statistics(out RavenQueryStatistics stats)
        {
            stats = queryStats;
        }

        /// <summary>
        /// Callback to get the results of the query
        /// </summary>
        public void AfterQueryExecuted(Action<QueryResult> afterQueryExecutedCallback)
        {
            this.afterQueryExecutedCallback += afterQueryExecutedCallback;
        }

        /// <summary>
        /// Callback to get the results of the stream
        /// </summary>
        public void AfterStreamExecuted(AfterStreamExecutedDelegate afterStreamExecutedCallback)
        {
            this.afterStreamExecutedCallback += afterStreamExecutedCallback;
        }

        /// <summary>
        /// Called externally to raise the after query executed callback
        /// </summary>
        public void InvokeAfterQueryExecuted(QueryResult result)
        {
            var queryExecuted = afterQueryExecutedCallback;
            if (queryExecuted != null)
                queryExecuted(result);
        }

        /// <summary>
        /// Called externally to raise the after stream executed callback
        /// </summary>
        public void InvokeAfterStreamExecuted(BlittableJsonReaderObject result)
        {
            var streamExecuted = afterStreamExecutedCallback;
            if (streamExecuted != null)
                streamExecuted(result);
        }

        #endregion

        protected virtual async Task<QueryOperation> ExecuteActualQueryAsync(CancellationToken token = new CancellationToken())
        {
            using (queryOperation.EnterQueryContext())
            {
                queryOperation.LogQuery();
                var command = queryOperation.CreateRequest();
                await theSession.RequestExecuter.ExecuteAsync(command, theSession.Context, token);
                queryOperation.SetResult(command.Result);

                InvokeAfterQueryExecuted(queryOperation.CurrentQueryResults);
                return queryOperation;
            }
        }

        /// <summary>
        /// Generates the index query.
        /// </summary>
        /// <param name = "query">The query.</param>
        /// <returns></returns>
        protected virtual IndexQuery GenerateIndexQuery(string query)
        {
            if (isSpatialQuery)
            {
                if (indexName == "dynamic" || indexName.StartsWith("dynamic/"))
                    throw new NotSupportedException("Dynamic indexes do not support spatial queries. A static index, with spatial field(s), must be defined.");

                var spatialQuery = new SpatialIndexQuery(conventions)
                {
                    IsDistinct = isDistinct,
                    Query = query,
                    Start = start,
                    WaitForNonStaleResultsAsOfNow = theWaitForNonStaleResultsAsOfNow,
                    WaitForNonStaleResults = theWaitForNonStaleResults,
                    WaitForNonStaleResultsTimeout = timeout,
                    CutoffEtag = cutoffEtag,
                    SortedFields = orderByFields.Select(x => new SortedField(x)).ToArray(),
                    DynamicMapReduceFields = dynamicMapReduceFields,
                    FieldsToFetch = fieldsToFetch,
                    SpatialFieldName = spatialFieldName,
                    QueryShape = queryShape,
                    RadiusUnitOverride = spatialUnits,
                    SpatialRelation = spatialRelation,
                    DistanceErrorPercentage = distanceErrorPct,
                    DefaultField = defaultField,
                    DefaultOperator = defaultOperator,
                    HighlightedFields = highlightedFields.Select(x => x.Clone()).ToArray(),
                    HighlighterPreTags = highlighterPreTags.ToArray(),
                    HighlighterPostTags = highlighterPostTags.ToArray(),
                    HighlighterKeyName = highlighterKeyName,
                    Transformer = resultsTransformer,
                    AllowMultipleIndexEntriesForSameDocumentToResultTransformer = allowMultipleIndexEntriesForSameDocumentToResultTransformer,
                    TransformerParameters = transformerParameters,
                    DisableCaching = disableCaching,
                    ShowTimings = showQueryTimings,
                    ExplainScores = shouldExplainScores,
                    Includes = includes.ToArray()
                };

                if (pageSize.HasValue)
                    spatialQuery.PageSize = pageSize.Value;

                return spatialQuery;
            }

            var indexQuery = new IndexQuery(conventions)
            {
                IsDistinct = isDistinct,
                Query = query,
                Start = start,
                CutoffEtag = cutoffEtag,
                WaitForNonStaleResultsAsOfNow = theWaitForNonStaleResultsAsOfNow,
                WaitForNonStaleResults = theWaitForNonStaleResults,
                WaitForNonStaleResultsTimeout = timeout,
                SortedFields = orderByFields.Select(x => new SortedField(x)).ToArray(),
                DynamicMapReduceFields = dynamicMapReduceFields,
                FieldsToFetch = fieldsToFetch,
                DefaultField = defaultField,
                DefaultOperator = defaultOperator,
                HighlightedFields = highlightedFields.Select(x => x.Clone()).ToArray(),
                HighlighterPreTags = highlighterPreTags.ToArray(),
                HighlighterPostTags = highlighterPostTags.ToArray(),
                HighlighterKeyName = highlighterKeyName,
                Transformer = resultsTransformer,
                TransformerParameters = transformerParameters,
                AllowMultipleIndexEntriesForSameDocumentToResultTransformer = allowMultipleIndexEntriesForSameDocumentToResultTransformer,
                DisableCaching = disableCaching,
                ShowTimings = showQueryTimings,
                ExplainScores = shouldExplainScores,
                Includes = includes.ToArray()
            };

            if (pageSize != null)
                indexQuery.PageSize = pageSize.Value;

            return indexQuery;
        }

        private static readonly Regex escapePostfixWildcard = new Regex(@"\\\*(\s|$)",
            RegexOptions.Compiled
            );
        protected QueryOperator defaultOperator;
        protected bool isDistinct;
        protected bool allowMultipleIndexEntriesForSameDocumentToResultTransformer;
        private Type originalType;

        /// <summary>
        /// Perform a search for documents which fields that match the searchTerms.
        /// If there is more than a single term, each of them will be checked independently.
        /// </summary>
        public void Search(string fieldName, string searchTerms, EscapeQueryOptions escapeQueryOptions = EscapeQueryOptions.RawQuery)
        {
            queryText.Append(' ');

            NegateIfNeeded();
            switch (escapeQueryOptions)
            {
                case EscapeQueryOptions.EscapeAll:
                    searchTerms = RavenQuery.Escape(searchTerms, false, false);
                    break;
                case EscapeQueryOptions.AllowPostfixWildcard:
                    searchTerms = RavenQuery.Escape(searchTerms, false, false);
                    searchTerms = escapePostfixWildcard.Replace(searchTerms, "*${1}");
                    break;
                case EscapeQueryOptions.AllowAllWildcards:
                    searchTerms = RavenQuery.Escape(searchTerms, false, false);
                    searchTerms = searchTerms.Replace("\\*", "*");
                    break;
                case EscapeQueryOptions.RawQuery:
                    break;
                default:
                    throw new ArgumentOutOfRangeException("escapeQueryOptions", "Value: " + escapeQueryOptions);
            }
            bool hasWhiteSpace = searchTerms.Any(char.IsWhiteSpace);
            lastEquality = new KeyValuePair<string, string>(fieldName,
                hasWhiteSpace ? "(" + searchTerms + ")" : searchTerms
                );

            queryText.Append(fieldName).Append(":").Append("(").Append(searchTerms).Append(")");
        }

        private string TransformToEqualValue(WhereParams whereParams)
        {
            if (whereParams.Value == null)
            {
                return Constants.NullValueNotAnalyzed;
            }
            if (Equals(whereParams.Value, string.Empty))
            {
                return Constants.EmptyStringNotAnalyzed;
            }

            var type = TypeSystem.GetNonNullableType(whereParams.Value.GetType());

            if (conventions.SaveEnumsAsIntegers && type.GetTypeInfo().IsEnum)
            {
                return ((int)whereParams.Value).ToString();
            }

            if (type == typeof(bool))
            {
                return (bool)whereParams.Value ? "true" : "false";
            }
            if (type == typeof(DateTime))
            {
                var val = (DateTime)whereParams.Value;
                var s = val.GetDefaultRavenFormat();
                if (val.Kind == DateTimeKind.Utc)
                    s += "Z";
                return s;
            }
            if (type == typeof(DateTimeOffset))
            {
                var val = (DateTimeOffset)whereParams.Value;
                return val.UtcDateTime.GetDefaultRavenFormat(true);
            }

            if (type == typeof(decimal))
            {
                return RavenQuery.Escape(((double)((decimal)whereParams.Value)).ToString(CultureInfo.InvariantCulture), false, false);
            }

            if (type == typeof(double))
            {
                return RavenQuery.Escape(((double)(whereParams.Value)).ToString("r", CultureInfo.InvariantCulture), false, false);
            }

            var strValue = whereParams.Value as string;
            if (strValue != null)
            {
                strValue = RavenQuery.Escape(strValue,
                        whereParams.AllowWildcards && whereParams.IsAnalyzed, whereParams.IsAnalyzed);

                return whereParams.IsAnalyzed ? strValue : String.Concat("[[", strValue, "]]");
            }

            if (conventions.TryConvertValueForQuery(whereParams.FieldName, whereParams.Value, QueryValueConvertionType.Equality, out strValue))
                return strValue;

            if (whereParams.Value is ValueType)
            {
                var escaped = RavenQuery.Escape(Convert.ToString(whereParams.Value, CultureInfo.InvariantCulture),
                                                whereParams.AllowWildcards && whereParams.IsAnalyzed, true);

                return escaped;
            }

            var result = GetImplicitStringConvertion(whereParams.Value.GetType());
            if (result != null)
            {
                return RavenQuery.Escape(result(whereParams.Value), whereParams.AllowWildcards && whereParams.IsAnalyzed, true);
            }

            throw new NotImplementedException();

            //var jsonSerializer = conventions.CreateSerializer();
            //var ravenJTokenWriter = new RavenJTokenWriter();
            //jsonSerializer.Serialize(ravenJTokenWriter, whereParams.Value);
            //var term = ravenJTokenWriter.Token.ToString(Formatting.None);
            //if (term.Length > 1 && term[0] == '"' && term[term.Length - 1] == '"')
            //{
            //    term = term.Substring(1, term.Length - 2);
            //}
            //switch (ravenJTokenWriter.Token.Type)
            //{
            //    case JTokenType.Object:
            //    case JTokenType.Array:
            //        return "[[" + RavenQuery.Escape(term, whereParams.AllowWildcards && whereParams.IsAnalyzed, false) + "]]";

            //    default:
            //        return RavenQuery.Escape(term, whereParams.AllowWildcards && whereParams.IsAnalyzed, true);
            //}
        }

        private Func<object, string> GetImplicitStringConvertion(Type type)
        {
            if (type == null)
                return null;

            Func<object, string> value;
            var localStringsCache = implicitStringsCache;
            if (localStringsCache.TryGetValue(type, out value))
                return value;

            var methodInfo = type.GetMethod("op_Implicit", new[] { type });

            if (methodInfo == null || methodInfo.ReturnType != typeof(string))
            {
                implicitStringsCache = new Dictionary<Type, Func<object, string>>(localStringsCache)
                {
                    {type, null}
                };
                return null;
            }

            var arg = Expression.Parameter(typeof(object), "self");

            var func = (Func<object, string>)Expression.Lambda(Expression.Call(methodInfo, Expression.Convert(arg, type)), arg).Compile();

            implicitStringsCache = new Dictionary<Type, Func<object, string>>(localStringsCache)
                {
                    {type, func}
                };
            return func;
        }

        private string TransformToRangeValue(WhereParams whereParams)
        {
            if (whereParams.Value == null)
                return Constants.NullValueNotAnalyzed;
            if (Equals(whereParams.Value, string.Empty))
                return Constants.EmptyStringNotAnalyzed;

            if (whereParams.Value is DateTime)
            {
                var dateTime = (DateTime)whereParams.Value;
                var dateStr = dateTime.GetDefaultRavenFormat();
                if (dateTime.Kind == DateTimeKind.Utc)
                    dateStr += "Z";
                return dateStr;
            }
            if (whereParams.Value is DateTimeOffset)
                return ((DateTimeOffset)whereParams.Value).UtcDateTime.GetDefaultRavenFormat(true);

            if (whereParams.Value is int)
                return NumberUtil.NumberToString((int)whereParams.Value);
            if (whereParams.Value is long)
                return NumberUtil.NumberToString((long)whereParams.Value);
            if (whereParams.Value is decimal)
                return NumberUtil.NumberToString((double)(decimal)whereParams.Value);
            if (whereParams.Value is double)
                return NumberUtil.NumberToString((double)whereParams.Value);
            if (whereParams.Value is TimeSpan)
                return NumberUtil.NumberToString(((TimeSpan)whereParams.Value).Ticks);
            if (whereParams.Value is float)
                return NumberUtil.NumberToString((float)whereParams.Value);
            if (whereParams.Value is string)
                return RavenQuery.Escape(whereParams.Value.ToString(), false, true);

            string strVal;
            if (conventions.TryConvertValueForQuery(whereParams.FieldName, whereParams.Value, QueryValueConvertionType.Range,
                                                    out strVal))
                return strVal;

            if (whereParams.Value is ValueType)
                return RavenQuery.Escape(Convert.ToString(whereParams.Value, CultureInfo.InvariantCulture),
                                         false, true);

            var stringWriter = new StringWriter();
            conventions.CreateSerializer().Serialize(stringWriter, whereParams.Value);

            var sb = stringWriter.GetStringBuilder();
            if (sb.Length > 1 && sb[0] == '"' && sb[sb.Length - 1] == '"')
            {
                sb.Remove(sb.Length - 1, 1);
                sb.Remove(0, 1);
            }

            return RavenQuery.Escape(sb.ToString(), false, true);
        }

        /// <summary>
        ///   Returns a <see cref = "System.String" /> that represents the query for this instance.
        /// </summary>
        /// <returns>
        ///   A <see cref = "System.String" /> that represents the query for this instance.
        /// </returns>
        public override string ToString()
        {
            if (currentClauseDepth != 0)
                throw new InvalidOperationException(string.Format("A clause was not closed correctly within this query, current clause depth = {0}", currentClauseDepth));

            return queryText.ToString().Trim();
        }

        /// <summary>
        /// The last term that we asked the query to use equals on
        /// </summary>
        public KeyValuePair<string, string> GetLastEqualityTerm(bool isAsync = false)
        {
            return lastEquality;
        }

        public void Intersect()
        {
            queryText.Append(Constants.IntersectSeparator);
        }

        public void ContainsAny(string fieldName, IEnumerable<object> values)
        {
            ContainsAnyAllProcessor(fieldName, values, "OR");
        }

        public void ContainsAll(string fieldName, IEnumerable<object> values)
        {
            ContainsAnyAllProcessor(fieldName, values, "AND");
        }

        private void ContainsAnyAllProcessor(string fieldName, IEnumerable<object> values, string seperator)
        {
            AppendSpaceIfNeeded(queryText.Length > 0 && char.IsWhiteSpace(queryText[queryText.Length - 1]) == false);
            NegateIfNeeded();

            var list = UnpackEnumerable(values).ToList();
            if (list.Count == 0)
            {
                queryText.Append("*:*");
                return;
            }

            var first = true;
            queryText.Append("(");
            foreach (var value in list)
            {
                if (first == false)
                {
                    queryText.Append(" " + seperator + " ");
                }
                first = false;
                var whereParams = new WhereParams
                {
                    AllowWildcards = true,
                    IsAnalyzed = true,
                    FieldName = fieldName,
                    Value = value
                };
                EnsureValidFieldName(whereParams);
                queryText.Append(fieldName)
                         .Append(":")
                         .Append(TransformToEqualValue(whereParams));
            }
            queryText.Append(")");
        }

        public void AddRootType(Type type)
        {
            rootTypes.Add(type);
        }

        IDocumentQueryCustomization IDocumentQueryCustomization.AddOrder(string fieldName, bool descending)
        {
            AddOrder(fieldName, descending);
            return this;
        }

        IDocumentQueryCustomization IDocumentQueryCustomization.AddOrder<TResult>(Expression<Func<TResult, object>> propertySelector, bool descending)
        {
            AddOrder(GetMemberQueryPath(propertySelector.Body), descending);
            return this;
        }

        IDocumentQueryCustomization IDocumentQueryCustomization.AddOrder(string fieldName, bool descending, Type fieldType)
        {
            AddOrder(fieldName, descending, fieldType);
            return this;
        }

        IDocumentQueryCustomization IDocumentQueryCustomization.AlphaNumericOrdering(string fieldName, bool descending)
        {
            AlphaNumericOrdering(fieldName, descending);
            return this;
        }

        IDocumentQueryCustomization IDocumentQueryCustomization.AlphaNumericOrdering<TResult>(Expression<Func<TResult, object>> propertySelector, bool descending)
        {
            AlphaNumericOrdering(GetMemberQueryPath(propertySelector.Body), descending);
            return this;
        }

        /// <summary>
        /// Order the search results randomly
        /// </summary>
        IDocumentQueryCustomization IDocumentQueryCustomization.RandomOrdering()
        {
            RandomOrdering();
            return this;
        }

        /// <summary>
        /// Order the search results randomly using the specified seed
        /// this is useful if you want to have repeatable random queries
        /// </summary>
        IDocumentQueryCustomization IDocumentQueryCustomization.RandomOrdering(string seed)
        {
            RandomOrdering(seed);
            return this;
        }

        IDocumentQueryCustomization IDocumentQueryCustomization.CustomSortUsing(string typeName)
        {
            CustomSortUsing(typeName, false);
            return this;
        }

        IDocumentQueryCustomization IDocumentQueryCustomization.CustomSortUsing(string typeName, bool descending)
        {
            CustomSortUsing(typeName, descending);
            return this;
        }

        /// <summary>
        /// Returns a list of results for a query asynchronously. 
        /// </summary>
        public async Task<IList<T>> ToListAsync(CancellationToken token = default(CancellationToken))
        {
            var currentQueryOperation = await InitAsync(token).ConfigureAwait(false);
            var tuple = await ProcessEnumerator(currentQueryOperation).WithCancellation(token).ConfigureAwait(false);
            return tuple.Item2;
        }

        /// <summary>
        /// Gets the total count of records for this query
        /// </summary>
        public async Task<int> CountAsync(CancellationToken token = default(CancellationToken))
        {
            Take(0);
            var result = await QueryResultAsync(token).ConfigureAwait(false);
            return result.TotalResults;
        }

        public string GetMemberQueryPathForOrderBy(Expression expression)
        {
            var memberQueryPath = GetMemberQueryPath(expression);
            var memberExpression = linqPathProvider.GetMemberExpression(expression);
            if (DocumentConvention.UsesRangeType(memberExpression.Type))
                return memberQueryPath + Constants.Indexing.Fields.RangeFieldSuffix;
            return memberQueryPath;
        }

        public string GetMemberQueryPath(Expression expression)
        {
            var result = linqPathProvider.GetPath(expression);
            result.Path = result.Path.Substring(result.Path.IndexOf('.') + 1);

            if (expression.NodeType == ExpressionType.ArrayLength)
                result.Path += ".Length";

            var propertyName = indexName == null || indexName.StartsWith("dynamic/", StringComparison.OrdinalIgnoreCase)
                ? conventions.FindPropertyNameForDynamicIndex(typeof(T), indexName, "", result.Path)
                : conventions.FindPropertyNameForIndex(typeof(T), indexName, "", result.Path);
            return propertyName;
        }

        public void SetAllowMultipleIndexEntriesForSameDocumentToResultTransformer(
            bool val)
        {
            this.allowMultipleIndexEntriesForSameDocumentToResultTransformer =
                val;
        }

        public void SetResultTransformer(string transformer)
        {
            this.resultsTransformer = transformer;
        }

        public void Distinct()
        {
            isDistinct = true;
        }

    }


}
