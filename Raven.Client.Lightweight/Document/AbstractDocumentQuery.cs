//-----------------------------------------------------------------------
// <copyright file="DocumentQuery.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using Raven.Abstractions.Spatial;
using Raven.Abstractions.Util;
using Raven.Client.Connection.Async;
using System.Threading.Tasks;
using Raven.Client.Document.Batches;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Client.Document.SessionOperations;
using Raven.Client.Linq;
using Raven.Client.Listeners;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Client.Spatial;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;
using Raven.Client.Document.Async;

namespace Raven.Client.Document
{
    /// <summary>
    ///   A query against a Raven index
    /// </summary>
    public abstract class AbstractDocumentQuery<T, TSelf> : IDocumentQueryCustomization, IAbstractDocumentQuery<T>
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
        /// The database commands to use
        /// </summary>
        protected readonly IDatabaseCommands theDatabaseCommands;

        /// <summary>
        /// Async database commands to use
        /// </summary>
        protected readonly IAsyncDatabaseCommands theAsyncDatabaseCommands;

        /// <summary>
        /// The index to query
        /// </summary>
        protected readonly string indexName;

        protected Func<IndexQuery, IEnumerable<object>, IEnumerable<object>> transformResultsFunc;

        protected string defaultField;

        private int currentClauseDepth;

        protected KeyValuePair<string, string> lastEquality;

        protected Dictionary<string, RavenJToken> transformerParameters = new Dictionary<string, RavenJToken>();

        /// <summary>
        ///   The list of fields to project directly from the results
        /// </summary>
        protected readonly string[] projectionFields;

        /// <summary>
        ///   The list of fields to project directly from the index on the server
        /// </summary>
        protected readonly string[] fieldsToFetch;

        /// <summary>
        /// The query listeners for this query
        /// </summary>
        protected readonly IDocumentQueryListener[] queryListeners;
        protected readonly bool isMapReduce;
        /// <summary>
        /// The session for this query
        /// </summary>
        protected readonly InMemoryDocumentSessionOperations theSession;

        /// <summary>
        ///   The cutoff date to use for detecting staleness in the index
        /// </summary>
        protected DateTime? cutoff;

        /// <summary>
        ///   The fields to order the results by
        /// </summary>
        protected string[] orderByFields = new string[0];

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
        ///   The types to sort the fields by (NULL if not specified)
        /// </summary>
        protected HashSet<KeyValuePair<string, SortOptions?>> sortByHints = new HashSet<KeyValuePair<string, SortOptions?>>(SortOptionsEqualityProvider.Instance);

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
        protected TimeSpan timeout;
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

        public bool IsDistinct { get { return isDistinct; } }

        /// <summary>
        ///   Grant access to the database commands
        /// </summary>
        public virtual IDatabaseCommands DatabaseCommands
        {
            get { return theDatabaseCommands; }
        }

        /// <summary>
        ///   Grant access to the async database commands
        /// </summary>
        public virtual IAsyncDatabaseCommands AsyncDatabaseCommands
        {
            get { return theAsyncDatabaseCommands; }
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

        protected Action<QueryResult> afterQueryExecutedCallback;
        protected AfterStreamExecutedDelegate afterStreamExecutedCallback;
        protected Etag cutoffEtag;

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
        ///   Initializes a new instance of the <see cref = "DocumentQuery{T}" /> class.
        /// </summary>
        protected AbstractDocumentQuery(InMemoryDocumentSessionOperations theSession,
                                     IDatabaseCommands databaseCommands,
                                     string indexName,
                                     string[] fieldsToFetch,
                                     string[] projectionFields,
                                     IDocumentQueryListener[] queryListeners,
                                     bool isMapReduce)
            : this(theSession, databaseCommands, null, indexName, fieldsToFetch, projectionFields, queryListeners, isMapReduce)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AbstractDocumentQuery{T, TSelf}"/> class.
        /// </summary>
        protected AbstractDocumentQuery(InMemoryDocumentSessionOperations theSession,
                                     IDatabaseCommands databaseCommands,
                                     IAsyncDatabaseCommands asyncDatabaseCommands,
                                     string indexName,
                                     string[] fieldsToFetch,
                                     string[] projectionFields,
                                     IDocumentQueryListener[] queryListeners,
                                     bool isMapReduce)
        {
            theDatabaseCommands = databaseCommands;
            this.projectionFields = projectionFields;
            this.fieldsToFetch = fieldsToFetch;
            this.queryListeners = queryListeners;
            this.isMapReduce = isMapReduce;
            this.indexName = indexName;
            this.theSession = theSession;
            theAsyncDatabaseCommands = asyncDatabaseCommands;
            AfterQueryExecuted(UpdateStatsAndHighlightings);

            conventions = theSession == null ? new DocumentConvention() : theSession.Conventions;
            linqPathProvider = new LinqPathProvider(conventions);

            var timeoutAsString = Environment.GetEnvironmentVariable(Constants.RavenDefaultQueryTimeout);
            int defaultTimeout;
            if (!string.IsNullOrEmpty(timeoutAsString) && int.TryParse(timeoutAsString, out defaultTimeout))
            {
                _defaultTimeout = defaultTimeout;
            }

            if (conventions.DefaultQueryingConsistency == ConsistencyOptions.AlwaysWaitForNonStaleResultsAsOfLastWrite)
            {
                WaitForNonStaleResultsAsOfLastWrite();
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
            theDatabaseCommands = other.theDatabaseCommands;
            theAsyncDatabaseCommands = other.theAsyncDatabaseCommands;
            indexName = other.indexName;
            linqPathProvider = other.linqPathProvider;
            allowMultipleIndexEntriesForSameDocumentToResultTransformer =
                other.allowMultipleIndexEntriesForSameDocumentToResultTransformer;
            projectionFields = other.projectionFields;
            theSession = other.theSession;
            conventions = other.conventions;
            cutoff = other.cutoff;
            orderByFields = other.orderByFields;
            sortByHints = other.sortByHints;
            pageSize = other.pageSize;
            queryText = other.queryText;
            start = other.start;
            timeout = other.timeout;
            theWaitForNonStaleResults = other.theWaitForNonStaleResults;
            theWaitForNonStaleResultsAsOfNow = other.theWaitForNonStaleResultsAsOfNow;
            includes = other.includes;
            queryListeners = other.queryListeners;
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
            OrderBy(Constants.DistanceFieldName);
            return this;
        }

        /// <summary>
        /// When using spatial queries, instruct the query to sort by the distance from the origin point
        /// </summary>
        IDocumentQueryCustomization IDocumentQueryCustomization.SortByDistance(double lat, double lng)
        {
            OrderBy(string.Format("{0};{1};{2}", Constants.DistanceFieldName, lat.ToInvariantString(), lng.ToInvariantString()));
            return this;
        }

        /// <summary>
        /// When using spatial queries, instruct the query to sort by the distance from the origin point
        /// </summary>
        IDocumentQueryCustomization IDocumentQueryCustomization.SortByDistance(double lat, double lng, string sortedFieldName)
        {
            OrderBy(string.Format("{0};{1};{2};{3}", Constants.DistanceFieldName, lat.ToInvariantString(), lng.ToInvariantString(), sortedFieldName));
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
            GenerateQueryWithinRadiusOf(Constants.DefaultSpatialFieldName, radius, latitude, longitude, distErrorPercent);
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
            GenerateQueryWithinRadiusOf(Constants.DefaultSpatialFieldName, radius, latitude, longitude, distErrorPercent, radiusUnits);
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
            var wkt = criteria.Shape as string;
            if (wkt == null && criteria.Shape != null)
            {
                var jsonSerializer = DocumentConvention.CreateSerializer();

                using (var jsonWriter = new RavenJTokenWriter())
                {
                    var converter = new ShapeConverter();
                    jsonSerializer.Serialize(jsonWriter, criteria.Shape);
                    if (!converter.TryConvert(jsonWriter.Token, out wkt))
                        throw new ArgumentException("Shape");
                }
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
            var fullId = DocumentConvention.FindFullDocumentKeyFromNonStringIdentifier(-1, typeof(TInclude), false);
            var idPrefix = fullId.Replace("-1", string.Empty);

            var id = path.ToPropertyPath() + "(" + idPrefix + ")";
            Include(id);
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
            cutoff = null;
            timeout = waitTimeout;
        }

        protected internal QueryOperation InitializeQueryOperation()
        {
            var query = queryText.ToString();
            var indexQuery = GenerateIndexQuery(query);

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
            ExecuteBeforeQueryListeners();

            var query = queryText.ToString();
            var indexQuery = GenerateIndexQuery(query);
            if (beforeQueryExecutionAction != null)
                beforeQueryExecutionAction(indexQuery);

            return indexQuery;
        }

        protected void ClearSortHints(IDatabaseCommands dbCommands)
        {
            foreach (var key in dbCommands.OperationsHeaders.AllKeys.Where(key => key.StartsWith("SortHint")).ToArray())
            {
                dbCommands.OperationsHeaders.Remove(key);
            }
        }

        protected void ClearSortHints(IAsyncDatabaseCommands dbCommands)
        {
            foreach (var key in dbCommands.OperationsHeaders.AllKeys.Where(key => key.StartsWith("SortHint")).ToArray())
            {
                dbCommands.OperationsHeaders.Remove(key);
            }
        }

        //the assumption here that there is only one of them is not null
        //and even if not, they should have the same operation headers 
        protected NameValueCollection GetOperationHeaders()
        {
            if (DatabaseCommands != null)
                return DatabaseCommands.OperationsHeaders;

            return AsyncDatabaseCommands != null ?
                AsyncDatabaseCommands.OperationsHeaders : new NameValueCollection(0);
        }


        protected void ExecuteBeforeQueryListeners()
        {
            foreach (var documentQueryListener in queryListeners)
            {
                documentQueryListener.BeforeQueryExecuted(this);
            }
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
            AddOrder(Constants.AlphaNumericFieldName + ";" + fieldName, descending);
        }

        /// <summary>
        /// Order the search results randomly
        /// </summary>
        public void RandomOrdering()
        {
            AddOrder(Constants.RandomFieldName + ";" + Guid.NewGuid(), false);
        }

        /// <summary>
        /// Order the search results randomly using the specified seed
        /// this is useful if you want to have repeatable random queries
        /// </summary>
        public void RandomOrdering(string seed)
        {
            AddOrder(Constants.RandomFieldName + ";" + seed, false);
        }

        public void CustomSortUsing(string typeName, bool descending)
        {
            AddOrder(Constants.CustomSortFieldName + ";" + typeName, descending);
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
            if (theSession != null)
                sortByHints.Add(new KeyValuePair<string, SortOptions?>(fieldName, theSession.Conventions.GetDefaultSortOption(fieldType)));
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

            if (theSession != null && whereParams.Value != null && !(whereParams.Value is string))
                sortByHints.Add(new KeyValuePair<string, SortOptions?>(whereParams.FieldName, theSession.Conventions.GetDefaultSortOption(whereParams.Value.GetType())));

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
            if (theSession == null || theSession.Conventions == null || whereParams.IsNestedPath || isMapReduce)
                return whereParams.FieldName;

            foreach (var rootType in rootTypes)
            {
                var identityProperty = theSession.Conventions.GetIdentityProperty(rootType);
                if (identityProperty != null && identityProperty.Name == whereParams.FieldName)
                {
                    whereParams.FieldTypeForIdentifier = rootType;
                    return whereParams.FieldName = Constants.DocumentIdFieldName;
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
            // https://lucene.apache.org/core/2_9_4/queryparsersyntax.html#Wildcard%20Searches
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

            if ((start ?? end) != null && theSession != null)
                sortByHints.Add(new KeyValuePair<string, SortOptions?>(fieldName, theSession.Conventions.GetDefaultSortOption((start ?? end).GetType())));

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
            if ((start ?? end) != null && theSession != null)
                sortByHints.Add(new KeyValuePair<string, SortOptions?>(fieldName, theSession.Conventions.GetDefaultSortOption((start ?? end).GetType())));

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

            if (fieldName == Constants.DocumentIdFieldName)
                return fieldName;

            var val = (start ?? end);
            if (conventions.UsesRangeType(val) && !fieldName.EndsWith("_Range"))
                fieldName = fieldName + "_Range";
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
            cutoff = SystemTime.UtcNow;
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
        /// Instructs the query to wait for non stale results as of the last write made by any session belonging to the 
        /// current document store.
        /// This ensures that you'll always get the most relevant results for your scenarios using simple indexes (map only or dynamic queries).
        /// However, when used to query map/reduce indexes, it does NOT guarantee that the document that this etag belong to is actually considered for the results. 
        /// </summary>
        IDocumentQueryCustomization IDocumentQueryCustomization.WaitForNonStaleResultsAsOfLastWrite()
        {
            WaitForNonStaleResultsAsOfLastWrite();
            return this;
        }

        /// <summary>
        /// Instructs the query to wait for non stale results as of the last write made by any session belonging to the 
        /// current document store.
        /// This ensures that you'll always get the most relevant results for your scenarios using simple indexes (map only or dynamic queries).
        /// However, when used to query map/reduce indexes, it does NOT guarantee that the document that this etag belong to is actually considered for the results. 
        /// </summary>
        IDocumentQueryCustomization IDocumentQueryCustomization.WaitForNonStaleResultsAsOfLastWrite(TimeSpan waitTimeout)
        {
            WaitForNonStaleResultsAsOfLastWrite(waitTimeout);
            return this;
        }

        /// <summary>
        ///   Instructs the query to wait for non stale results as of the cutoff date.
        /// </summary>
        /// <param name = "cutOff">The cut off.</param>
        /// <returns></returns>
        IDocumentQueryCustomization IDocumentQueryCustomization.WaitForNonStaleResultsAsOf(DateTime cutOff)
        {
            WaitForNonStaleResultsAsOf(cutOff);
            return this;
        }

        /// <summary>
        ///   Instructs the query to wait for non stale results as of the cutoff date for the specified timeout
        /// </summary>
        /// <param name = "cutOff">The cut off.</param>
        /// <param name = "waitTimeout">The wait timeout.</param>
        IDocumentQueryCustomization IDocumentQueryCustomization.WaitForNonStaleResultsAsOf(DateTime cutOff,
                                                                                           TimeSpan waitTimeout)
        {
            WaitForNonStaleResultsAsOf(cutOff, waitTimeout);
            return this;
        }

        /// <summary>
        /// Instructs the query to wait for non stale results as of the cutoff etag.
        /// </summary>
        /// <param name="cutOffEtag">The cut off etag.</param>
        IDocumentQueryCustomization IDocumentQueryCustomization.WaitForNonStaleResultsAsOf(Etag cutOffEtag)
        {
            WaitForNonStaleResultsAsOf(cutOffEtag);
            return this;
        }

        /// <summary>
        /// Instructs the query to wait for non stale results as of the cutoff etag for the specified timeout.
        /// </summary>
        /// <param name="cutOffEtag">The cut off etag.</param>
        /// <param name="waitTimeout">The wait timeout.</param>
        IDocumentQueryCustomization IDocumentQueryCustomization.WaitForNonStaleResultsAsOf(Etag cutOffEtag, TimeSpan waitTimeout)
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
            cutoff = SystemTime.UtcNow;
            timeout = waitTimeout;
        }

        /// <summary>
        ///   Instructs the query to wait for non stale results as of the cutoff date.
        /// </summary>
        /// <param name = "cutOff">The cut off.</param>
        /// <returns></returns>
        public void WaitForNonStaleResultsAsOf(DateTime cutOff)
        {
            WaitForNonStaleResultsAsOf(cutOff, DefaultTimeout);
        }

        /// <summary>
        ///   Instructs the query to wait for non stale results as of the cutoff date for the specified timeout
        /// </summary>
        /// <param name = "cutOff">The cut off.</param>
        /// <param name = "waitTimeout">The wait timeout.</param>
        public void WaitForNonStaleResultsAsOf(DateTime cutOff, TimeSpan waitTimeout)
        {
            theWaitForNonStaleResults = true;
            cutoff = cutOff.ToUniversalTime();
            timeout = waitTimeout;
        }

        /// <summary>
        /// Instructs the query to wait for non stale results as of the cutoff etag.
        /// </summary>
        public void WaitForNonStaleResultsAsOf(Etag cutOffEtag)
        {
            WaitForNonStaleResultsAsOf(cutOffEtag, DefaultTimeout);
        }

        /// <summary>
        /// Instructs the query to wait for non stale results as of the cutoff etag.
        /// </summary>
        public void WaitForNonStaleResultsAsOf(Etag cutOffEtag, TimeSpan waitTimeout)
        {
            theWaitForNonStaleResults = true;
            timeout = waitTimeout;
            cutoffEtag = cutOffEtag;
        }

        /// <summary>
        /// Instructs the query to wait for non stale results as of the last write made by any session belonging to the 
        /// current document store.
        /// This ensures that you'll always get the most relevant results for your scenarios using simple indexes (map only or dynamic queries).
        /// However, when used to query map/reduce indexes, it does NOT guarantee that the document that this etag belong to is actually considered for the results. 
        /// </summary>
        public void WaitForNonStaleResultsAsOfLastWrite()
        {
            WaitForNonStaleResultsAsOfLastWrite(DefaultTimeout);
        }

        /// <summary>
        /// Instructs the query to wait for non stale results as of the last write made by any session belonging to the 
        /// current document store.
        /// This ensures that you'll always get the most relevant results for your scenarios using simple indexes (map only or dynamic queries).
        /// However, when used to query map/reduce indexes, it does NOT guarantee that the document that this etag belong to is actually considered for the results. 
        /// </summary>
        public void WaitForNonStaleResultsAsOfLastWrite(TimeSpan waitTimeout)
        {
            var lastWrittenEtag = theSession.DocumentStore.GetLastWrittenEtag();
            if (lastWrittenEtag != null)
            {
                theWaitForNonStaleResults = true;
                timeout = waitTimeout;
                cutoffEtag = theSession.DocumentStore.GetLastWrittenEtag();
            }
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
        public void InvokeAfterStreamExecuted(ref RavenJObject result)
        {
            var streamExecuted = afterStreamExecutedCallback;
            if (streamExecuted != null)
                streamExecuted(ref result);
        }

        #endregion

        protected virtual async Task<QueryOperation> ExecuteActualQueryAsync()
        {
            theSession.IncrementRequestCount();
            while (true)
            {
                using (queryOperation.EnterQueryContext())
                {
                    queryOperation.LogQuery();
                    var result = await theAsyncDatabaseCommands.QueryAsync(indexName, queryOperation.IndexQuery, includes.ToArray()).ConfigureAwait(false);

                    if (queryOperation.IsAcceptable(result) == false)
                    {
                        await Task.Delay(100).ConfigureAwait(false);
                        continue;
                    }
                    InvokeAfterQueryExecuted(queryOperation.CurrentQueryResults);
                    return queryOperation;
                }
            }
        }

        /// <summary>
        ///   Generates the index query.
        /// </summary>
        /// <param name = "query">The query.</param>
        /// <returns></returns>
        protected virtual IndexQuery GenerateIndexQuery(string query)
        {
            if (isSpatialQuery)
            {
                if (indexName == "dynamic" || indexName.StartsWith("dynamic/"))
                    throw new NotSupportedException("Dynamic indexes do not support spatial queries. A static index, with spatial field(s), must be defined.");

                var spatialQuery = new SpatialIndexQuery
                {
                    IsDistinct = isDistinct,
                    Query = query,
                    Start = start,
                    Cutoff = cutoff,
                    WaitForNonStaleResultsAsOfNow = theWaitForNonStaleResultsAsOfNow,
                    WaitForNonStaleResults = theWaitForNonStaleResults,
                    CutoffEtag = cutoffEtag,
                    SortedFields = orderByFields.Select(x => new SortedField(x)).ToArray(),
                    SortHints = sortByHints.Where(x => x.Value != null).ToDictionary(x => x.Key, x => x.Value.Value),
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
                    ResultsTransformer = resultsTransformer,
                    AllowMultipleIndexEntriesForSameDocumentToResultTransformer = allowMultipleIndexEntriesForSameDocumentToResultTransformer,
                    TransformerParameters = transformerParameters,
                    DisableCaching = disableCaching,
                    ShowTimings = showQueryTimings,
                    ExplainScores = shouldExplainScores
                };

                if (pageSize.HasValue)
                    spatialQuery.PageSize = pageSize.Value;

                return spatialQuery;
            }

            var indexQuery = new IndexQuery
            {
                IsDistinct = isDistinct,
                Query = query,
                Start = start,
                Cutoff = cutoff,
                CutoffEtag = cutoffEtag,
                WaitForNonStaleResultsAsOfNow = theWaitForNonStaleResultsAsOfNow,
                WaitForNonStaleResults = theWaitForNonStaleResults,
                SortedFields = orderByFields.Select(x => new SortedField(x)).ToArray(),
                SortHints = sortByHints.Where(x => x.Value != null).ToDictionary(x => x.Key, x => x.Value.Value),
                FieldsToFetch = fieldsToFetch,
                DefaultField = defaultField,
                DefaultOperator = defaultOperator,
                HighlightedFields = highlightedFields.Select(x => x.Clone()).ToArray(),
                HighlighterPreTags = highlighterPreTags.ToArray(),
                HighlighterPostTags = highlighterPostTags.ToArray(),
                HighlighterKeyName = highlighterKeyName,
                ResultsTransformer = resultsTransformer,
                TransformerParameters = transformerParameters,
                AllowMultipleIndexEntriesForSameDocumentToResultTransformer = allowMultipleIndexEntriesForSameDocumentToResultTransformer,
                DisableCaching = disableCaching,
                ShowTimings = showQueryTimings,
                ExplainScores = shouldExplainScores
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

            if (conventions.SaveEnumsAsIntegers && type.IsEnum())
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
            if (whereParams.FieldName == Constants.DocumentIdFieldName && whereParams.Value is string == false)
            {
                return theSession.Conventions.FindFullDocumentKeyFromNonStringIdentifier(whereParams.Value,
                    originalType ?? whereParams.FieldTypeForIdentifier ?? typeof(T), false);
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

            var jsonSerializer = conventions.CreateSerializer();
            var ravenJTokenWriter = new RavenJTokenWriter();
            jsonSerializer.Serialize(ravenJTokenWriter, whereParams.Value);
            var term = ravenJTokenWriter.Token.ToString(Formatting.None);
            if (term.Length > 1 && term[0] == '"' && term[term.Length - 1] == '"')
            {
                term = term.Substring(1, term.Length - 2);
            }
            switch (ravenJTokenWriter.Token.Type)
            {
                case JTokenType.Object:
                case JTokenType.Array:
                    return "[[" + RavenQuery.Escape(term, whereParams.AllowWildcards && whereParams.IsAnalyzed, false) + "]]";

                default:
                    return RavenQuery.Escape(term, whereParams.AllowWildcards && whereParams.IsAnalyzed, true);
            }
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

            if (whereParams.FieldName == Constants.DocumentIdFieldName && whereParams.Value is string == false)
            {
                return theSession.Conventions.FindFullDocumentKeyFromNonStringIdentifier(whereParams.Value, typeof(T), false);
            }
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

        public string GetMemberQueryPathForOrderBy(Expression expression)
        {
            var memberQueryPath = GetMemberQueryPath(expression);
            var memberExpression = linqPathProvider.GetMemberExpression(expression);
            if (DocumentConvention.UsesRangeType(memberExpression.Type))
                return memberQueryPath + "_Range";
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
