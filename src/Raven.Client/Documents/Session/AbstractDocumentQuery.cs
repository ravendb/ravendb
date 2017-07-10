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
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Spatial;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.Spatial;
using Raven.Client.Documents.Session.Operations;
using Raven.Client.Documents.Session.Tokens;
using Raven.Client.Extensions;
using Sparrow.Extensions;
using Sparrow.Json;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    ///   A query against a Raven index
    /// </summary>
    public abstract class AbstractDocumentQuery<T, TSelf> : IDocumentQueryCustomization, IAbstractDocumentQuery<T>
                                                            where TSelf : AbstractDocumentQuery<T, TSelf>
    {

        private static readonly Regex EscapePostfixWildcard = new Regex(@"\\\*(\s|$)", RegexOptions.Compiled);

        protected QueryOperator DefaultOperator;
        protected bool AllowMultipleIndexEntriesForSameDocumentToResultTransformer;

        protected bool IsSpatialQuery;
        protected string SpatialFieldName, QueryShape;
        protected SpatialUnits? SpatialUnits;
        protected SpatialRelation SpatialRelation;
        protected double DistanceErrorPct;
        private readonly LinqPathProvider _linqPathProvider;
        protected Action<IndexQuery> BeforeQueryExecutionAction;

        protected readonly HashSet<Type> RootTypes = new HashSet<Type>
        {
            typeof (T)
        };

        private static Dictionary<Type, Func<object, string>> _implicitStringsCache = new Dictionary<Type, Func<object, string>>();

        /// <summary>
        /// Whether to negate the next operation
        /// </summary>
        protected bool Negate;

        /// <summary>
        /// The index to query
        /// </summary>
        public string IndexName { get; }

        protected Func<IndexQuery, IEnumerable<object>, IEnumerable<object>> TransformResultsFunc;

        private int _currentClauseDepth;

        protected KeyValuePair<string, object> LastEquality;

        protected Dictionary<string, object> TransformerParameters = new Dictionary<string, object>();

        /// <summary>
        ///   The list of fields to project directly from the results
        /// </summary>
        protected internal readonly string[] ProjectionFields;

        /// <summary>
        ///   The list of fields to project directly from the index on the server
        /// </summary>
        protected readonly string[] FieldsToFetch;

        protected bool IsMapReduce;
        /// <summary>
        /// The session for this query
        /// </summary>
        protected readonly InMemoryDocumentSessionOperations TheSession;

        /// <summary>
        /// The fields of dynamic map-reduce query
        /// </summary>
        protected DynamicMapReduceField[] DynamicMapReduceFields = new DynamicMapReduceField[0];

        /// <summary>
        ///   The fields to highlight
        /// </summary>
        protected List<HighlightedField> HighlightedFields = new List<HighlightedField>();

        /// <summary>
        ///   Highlighter pre tags
        /// </summary>
        protected string[] HighlighterPreTags = new string[0];

        /// <summary>
        ///   Highlighter post tags
        /// </summary>
        protected string[] HighlighterPostTags = new string[0];

        /// <summary>
        ///   Highlighter key
        /// </summary>
        protected string HighlighterKeyName;

        /// <summary>
        ///   The page size to use when querying the index
        /// </summary>
        protected int? PageSize;

        protected QueryOperation QueryOperation;

        protected readonly LinkedList<QueryToken> SelectTokens = new LinkedList<QueryToken>();

        protected readonly FromToken FromToken;

        protected LinkedList<QueryToken> WhereTokens = new LinkedList<QueryToken>();

        protected LinkedList<QueryToken> OrderByTokens = new LinkedList<QueryToken>();

        /// <summary>
        ///   which record to start reading from
        /// </summary>
        protected int Start;

        private readonly DocumentConventions _conventions;
        /// <summary>
        /// Timeout for this query
        /// </summary>
        protected TimeSpan? Timeout;
        /// <summary>
        /// Should we wait for non stale results
        /// </summary>
        protected bool TheWaitForNonStaleResults;
        /// <summary>
        /// Should we wait for non stale results as of now?
        /// </summary>
        protected bool TheWaitForNonStaleResultsAsOfNow;
        /// <summary>
        /// The paths to include when loading the query
        /// </summary>
        protected HashSet<string> Includes = new HashSet<string>();

        /// <summary>
        /// Holds the query stats
        /// </summary>
        protected QueryStatistics QueryStats = new QueryStatistics();

        /// <summary>
        /// Holds the query highlights
        /// </summary>
        protected QueryHighlightings Highlightings = new QueryHighlightings();

        /// <summary>
        /// The name of the results transformer to use after executing this query
        /// </summary>
        protected string Transformer;

        /// <summary>
        /// Determines if entities should be tracked and kept in memory
        /// </summary>
        protected bool DisableEntitiesTracking;

        /// <summary>
        /// Determine if query results should be cached.
        /// </summary>
        protected bool DisableCaching;

        /// <summary>
        /// Indicates if detailed timings should be calculated for various query parts (Lucene search, loading documents, transforming results). Default: false
        /// </summary>
        protected bool ShowQueryTimings;

        /// <summary>
        /// Determine if scores of query results should be explained
        /// </summary>
        protected bool ShouldExplainScores;

        public bool IsDistinct => SelectTokens.First?.Value is DistinctToken;

        /// <summary>
        /// Gets the document convention from the query session
        /// </summary>
        public DocumentConventions Conventions => _conventions;

        /// <summary>
        ///   Gets the session associated with this document query
        /// </summary>
        public IDocumentSession Session => (IDocumentSession)TheSession;
        public IAsyncDocumentSession AsyncSession => (IAsyncDocumentSession)TheSession;

        public bool IsDynamicMapReduce => DynamicMapReduceFields.Length > 0;

        protected Action<QueryResult> AfterQueryExecutedCallback;
        protected AfterStreamExecutedDelegate AfterStreamExecutedCallback;
        protected long? CutoffEtag;

        private static TimeSpan DefaultTimeout
        {
            get
            {
                if (Debugger.IsAttached) // increase timeout if we are debugging
                    return TimeSpan.FromMinutes(15);

                return TimeSpan.FromSeconds(15);
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AbstractDocumentQuery{T, TSelf}"/> class.
        /// </summary>
        protected AbstractDocumentQuery(InMemoryDocumentSessionOperations session,
                                     string indexName,
                                     string[] fieldsToFetch,
                                     string[] projectionFields,
                                     bool isMapReduce)
        {
            ProjectionFields = projectionFields;
            FieldsToFetch = fieldsToFetch;
            IsMapReduce = isMapReduce;
            IndexName = indexName;

            if (fieldsToFetch != null && fieldsToFetch.Length > 0)
                SelectTokens.AddLast(FieldsToFetchToken.Create(fieldsToFetch, projectionFields));

            FromToken = FromToken.Create(indexName);

            TheSession = session;
            AfterQueryExecuted(UpdateStatsAndHighlightings);

            _conventions = session == null ? new DocumentConventions() : session.Conventions;
            _linqPathProvider = new LinqPathProvider(_conventions);
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
            OrderBy(Constants.Documents.Indexing.Fields.DistanceFieldName);
            return this;
        }

        /// <summary>
        /// When using spatial queries, instruct the query to sort by the distance from the origin point
        /// </summary>
        IDocumentQueryCustomization IDocumentQueryCustomization.SortByDistance(double lat, double lng)
        {
            OrderBy(string.Format("{0};{1};{2}", Constants.Documents.Indexing.Fields.DistanceFieldName, lat.ToInvariantString(), lng.ToInvariantString()));
            return this;
        }

        /// <summary>
        /// When using spatial queries, instruct the query to sort by the distance from the origin point
        /// </summary>
        IDocumentQueryCustomization IDocumentQueryCustomization.SortByDistance(double lat, double lng, string sortedFieldName)
        {
            OrderBy(string.Format("{0};{1};{2};{3}", Constants.Documents.Indexing.Fields.DistanceFieldName, lat.ToInvariantString(), lng.ToInvariantString(), sortedFieldName));
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
            GenerateQueryWithinRadiusOf(Constants.Documents.Indexing.Fields.DefaultSpatialFieldName, radius, latitude, longitude, distErrorPercent);
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
            GenerateQueryWithinRadiusOf(Constants.Documents.Indexing.Fields.DefaultSpatialFieldName, radius, latitude, longitude, distErrorPercent, radiusUnits);
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


        IDocumentQueryCustomization IDocumentQueryCustomization.RelatesToShape(string fieldName, string shapeWkt, SpatialRelation rel, double distErrorPercent)
        {
            GenerateSpatialQueryData(fieldName, shapeWkt, rel, distErrorPercent);
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

        protected TSelf GenerateSpatialQueryData(string fieldName, string shapeWkt, SpatialRelation relation, double distanceErrorPct = 0.025, SpatialUnits? radiusUnits = null)
        {
            throw new NotImplementedException("This feature is not yet implemented");

            //IsSpatialQuery = true;
            //SpatialFieldName = fieldName;
            //QueryShape = new WktSanitizer().Sanitize(shapeWkt);
            //SpatialRelation = relation;
            //DistanceErrorPct = distanceErrorPct;
            //SpatialUnits = radiusUnits;
            //return (TSelf)this;
        }

        protected TSelf GenerateSpatialQueryData(string fieldName, SpatialCriteria criteria)
        {
            throw new NotImplementedException("This feature is not yet implemented");
            //var wkt = criteria.Shape as string;
            //if (wkt == null && criteria.Shape != null)
            //{
            //    var jsonSerializer = Conventions.CreateSerializer();

            //    /*using (var jsonWriter = new RavenJTokenWriter())
            //    {
            //        var converter = new ShapeConverter();
            //        jsonSerializer.Serialize(jsonWriter, criteria.Shape);
            //        if (!converter.TryConvert(jsonWriter.Token, out wkt))
            //            throw new ArgumentException("Shape");
            //    }*/
            //}

            //if (wkt == null)
            //    throw new ArgumentException("Shape");

            //IsSpatialQuery = true;
            //SpatialFieldName = fieldName;
            //QueryShape = new WktSanitizer().Sanitize(wkt);
            //SpatialRelation = criteria.Relation;
            //DistanceErrorPct = criteria.DistanceErrorPct;
            //return (TSelf)this;
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

        public void UsingDefaultOperator(QueryOperator @operator)
        {
            DefaultOperator = @operator;
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
            var idPrefix = Conventions.GetCollectionName(typeof(TInclude));
            if (idPrefix != null)
            {
                idPrefix = Conventions.TransformTypeCollectionNameToDocumentIdPrefix(idPrefix);
                idPrefix += Conventions.IdentityPartsSeparator;
            }

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
            TheWaitForNonStaleResults = true;
            CutoffEtag = null;
            Timeout = waitTimeout;
        }

        protected internal QueryOperation InitializeQueryOperation()
        {
            var indexQuery = GetIndexQuery();

            return new QueryOperation(TheSession,
                IndexName,
                indexQuery,
                ProjectionFields,
                TheWaitForNonStaleResults,
                Timeout,
                TransformResultsFunc,
                Includes,
                DisableEntitiesTracking);
        }

        public IndexQuery GetIndexQuery()
        {
            var query = ToString();
            var indexQuery = GenerateIndexQuery(query);
            BeforeQueryExecutionAction?.Invoke(indexQuery);

            return indexQuery;
        }

        /// <summary>
        ///   Gets the fields for projection
        /// </summary>
        /// <returns></returns>
        public IEnumerable<string> GetProjectionFields()
        {
            return ProjectionFields ?? Enumerable.Empty<string>();
        }

        /// <summary>
        /// Order the search results in alphanumeric order
        /// </summary>
        public void AlphaNumericOrdering(string fieldName, bool descending)
        {
            fieldName = EnsureValidFieldName(fieldName, isNestedPath: false);
            AddOrder(Constants.Documents.Indexing.Fields.AlphaNumericFieldName + ";" + fieldName, descending);
        }

        /// <summary>
        /// Order the search results randomly
        /// </summary>
        public void RandomOrdering()
        {
            AddOrder(Constants.Documents.Indexing.Fields.RandomFieldName + ";" + Guid.NewGuid(), false);
        }

        /// <summary>
        /// Order the search results randomly using the specified seed
        /// this is useful if you want to have repeatable random queries
        /// </summary>
        public void RandomOrdering(string seed)
        {
            AddOrder(Constants.Documents.Indexing.Fields.RandomFieldName + ";" + seed, false);
        }

        public void CustomSortUsing(string typeName, bool descending)
        {
            AddOrder(Constants.Documents.Indexing.Fields.CustomSortFieldName + ";" + typeName, descending);
        }

        public IDocumentQueryCustomization BeforeQueryExecution(Action<IndexQuery> action)
        {
            BeforeQueryExecutionAction += action;
            return this;
        }

        public IDocumentQueryCustomization TransformResults(Func<IndexQuery, IEnumerable<object>, IEnumerable<object>> resultsTransformer)
        {
            TransformResultsFunc = resultsTransformer;
            return this;
        }

        IDocumentQueryCustomization IDocumentQueryCustomization.Highlight(
            string fieldName, int fragmentLength, int fragmentCount, string fragmentsField)
        {
            Highlight(fieldName, fragmentLength, fragmentCount, fragmentsField);
            return this;
        }

        IDocumentQueryCustomization IDocumentQueryCustomization.Highlight(
            string fieldName, int fragmentLength, int fragmentCount, out FieldHighlightings fieldHighlightings)
        {
            Highlight(fieldName, fragmentLength, fragmentCount, out fieldHighlightings);
            return this;
        }

        IDocumentQueryCustomization IDocumentQueryCustomization.Highlight(
            string fieldName, string fieldKeyName, int fragmentLength, int fragmentCount, out FieldHighlightings fieldHighlightings)
        {
            Highlight(fieldName, fieldKeyName, fragmentLength, fragmentCount, out fieldHighlightings);
            return this;
        }

        IDocumentQueryCustomization IDocumentQueryCustomization.SetAllowMultipleIndexEntriesForSameDocumentToResultTransformer(bool val)
        {
            SetAllowMultipleIndexEntriesForSameDocumentToResultTransformer(val);
            return this;
        }

        public void AddMapReduceField(DynamicMapReduceField field)
        {
            IsMapReduce = true;

            DynamicMapReduceFields = DynamicMapReduceFields.Concat(new[] { field }).ToArray();
        }

        public DynamicMapReduceField[] GetGroupByFields()
        {
            return DynamicMapReduceFields.Where(x => x.IsGroupBy).ToArray();
        }

        IDocumentQueryCustomization IDocumentQueryCustomization.SetHighlighterTags(string preTag, string postTag)
        {
            SetHighlighterTags(preTag, postTag);
            return this;
        }

        IDocumentQueryCustomization IDocumentQueryCustomization.SetHighlighterTags(string[] preTags, string[] postTags)
        {
            SetHighlighterTags(preTags, postTags);
            return this;
        }

        public IDocumentQueryCustomization NoTracking()
        {
            DisableEntitiesTracking = true;
            return this;
        }

        public IDocumentQueryCustomization NoCaching()
        {
            DisableCaching = true;
            return this;
        }

        public IDocumentQueryCustomization ShowTimings()
        {
            ShowQueryTimings = true;
            return this;
        }

        public void SetHighlighterTags(string preTag, string postTag)
        {
            SetHighlighterTags(new[] { preTag }, new[] { postTag });
        }

        /// <summary>
        ///   Adds an ordering for a specific field to the query
        /// </summary>
        /// <param name = "fieldName">Name of the field.</param>
        /// <param name = "descending">if set to <c>true</c> [descending].</param>
        public void AddOrder(string fieldName, bool descending)
        {
            fieldName = EnsureValidFieldName(fieldName, isNestedPath: false);
            OrderByTokens.AddLast(descending ? OrderByToken.CreateDescending(fieldName) : OrderByToken.CreateAscending(fieldName));
        }

        public void Highlight(string fieldName, int fragmentLength, int fragmentCount, string fragmentsField)
        {
            throw new NotImplementedException("This feature is not yet implemented");
            //HighlightedFields.Add(new HighlightedField(fieldName, fragmentLength, fragmentCount, fragmentsField));
        }

        public void Highlight(string fieldName, int fragmentLength, int fragmentCount, out FieldHighlightings fieldHighlightings)
        {
            throw new NotImplementedException("This feature is not yet implemented");
            //HighlightedFields.Add(new HighlightedField(fieldName, fragmentLength, fragmentCount, null));
            //fieldHighlightings = Highlightings.AddField(fieldName);
        }

        public void Highlight(string fieldName, string fieldKeyName, int fragmentLength, int fragmentCount, out FieldHighlightings fieldHighlightings)
        {
            throw new NotImplementedException("This feature is not yet implemented");
            //HighlighterKeyName = fieldKeyName;
            //HighlightedFields.Add(new HighlightedField(fieldName, fragmentLength, fragmentCount, null));
            //fieldHighlightings = Highlightings.AddField(fieldName);
        }

        public void SetHighlighterTags(string[] preTags, string[] postTags)
        {
            throw new NotImplementedException("This feature is not yet implemented");
            //HighlighterPreTags = preTags;
            //HighlighterPostTags = postTags;
        }

        /// <summary>
        ///   Includes the specified path in the query, loading the document specified in that path
        /// </summary>
        /// <param name = "path">The path.</param>
        public void Include(string path)
        {
            Includes.Add(path);
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
            PageSize = count;
        }

        /// <summary>
        ///   Skips the specified count.
        /// </summary>
        /// <param name = "count">The count.</param>
        /// <returns></returns>
        public void Skip(int count)
        {
            Start = count;
        }

        /// <summary>
        ///   Filter the results from the index using the specified where clause.
        /// </summary>
        public void Where(string fieldName, string whereClause)
        {
            fieldName = EnsureValidFieldName(fieldName, isNestedPath: false);

            AppendOperatorIfNeeded(WhereTokens);

            WhereTokens.AddLast(WhereToken.Lucene(fieldName, whereClause));
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
            _currentClauseDepth++;

            AppendOperatorIfNeeded(WhereTokens);
            NegateIfNeeded();

            WhereTokens.AddLast(OpenSubclauseToken.Instance);
        }

        /// <summary>
        ///   Simplified method for closing a clause within the query
        /// </summary>
        /// <returns></returns>
        public void CloseSubclause()
        {
            _currentClauseDepth--;

            WhereTokens.AddLast(CloseSubclauseToken.Instance);
        }

        /// <summary>
        ///   Matches exact value
        /// </summary>
        public void WhereEquals(WhereParams whereParams)
        {
            whereParams.FieldName = EnsureValidFieldName(whereParams.FieldName, whereParams.IsNestedPath);

            var transformToEqualValue = TransformToEqualValue(whereParams);
            LastEquality = new KeyValuePair<string, object>(whereParams.FieldName, transformToEqualValue);

            AppendOperatorIfNeeded(WhereTokens);
            NegateIfNeeded();

            WhereTokens.AddLast(WhereToken.Equals(whereParams.FieldName, transformToEqualValue));
        }

        ///<summary>
        /// Negate the next operation
        ///</summary>
        public void NegateNext()
        {
            Negate = !Negate;
        }

        /// <summary>
        /// Check that the field has one of the specified value
        /// </summary>
        public void WhereIn(string fieldName, IEnumerable<object> values)
        {
            AppendOperatorIfNeeded(WhereTokens);
            NegateIfNeeded();

            fieldName = EnsureValidFieldName(fieldName, isNestedPath: false);

            WhereTokens.AddLast(WhereToken.In(fieldName, TransformEnumerable(fieldName, UnpackEnumerable(values))));
        }

        /// <summary>
        ///   Matches fields which starts with the specified value.
        /// </summary>
        /// <param name = "fieldName">Name of the field.</param>
        /// <param name = "value">The value.</param>
        public void WhereStartsWith(string fieldName, object value)
        {
            var whereParams = new WhereParams
            {
                FieldName = fieldName,
                Value = value,
                IsAnalyzed = true,
                AllowWildcards = true
            };

            whereParams.FieldName = EnsureValidFieldName(whereParams.FieldName, whereParams.IsNestedPath);

            var transformToEqualValue = TransformToEqualValue(whereParams);
            LastEquality = new KeyValuePair<string, object>(whereParams.FieldName, transformToEqualValue);

            AppendOperatorIfNeeded(WhereTokens);
            NegateIfNeeded();

            WhereTokens.AddLast(WhereToken.StartsWith(whereParams.FieldName, transformToEqualValue));
        }

        /// <summary>
        ///   Matches fields which ends with the specified value.
        /// </summary>
        /// <param name = "fieldName">Name of the field.</param>
        /// <param name = "value">The value.</param>
        public void WhereEndsWith(string fieldName, object value)
        {
            var whereParams = new WhereParams
            {
                FieldName = fieldName,
                Value = value,
                IsAnalyzed = true,
                AllowWildcards = true
            };

            whereParams.FieldName = EnsureValidFieldName(whereParams.FieldName, whereParams.IsNestedPath);

            var transformToEqualValue = TransformToEqualValue(whereParams);
            LastEquality = new KeyValuePair<string, object>(whereParams.FieldName, transformToEqualValue);

            AppendOperatorIfNeeded(WhereTokens);
            NegateIfNeeded();

            WhereTokens.AddLast(WhereToken.EndsWith(whereParams.FieldName, transformToEqualValue));
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
            AppendOperatorIfNeeded(WhereTokens);
            NegateIfNeeded();

            fieldName = GetFieldNameForRangeQueries(fieldName, start, end);

            WhereTokens.AddLast(WhereToken.Between(fieldName, start == null ? "*" : TransformToRangeValue(new WhereParams { Value = start, FieldName = fieldName }), end == null ? "NULL" : TransformToRangeValue(new WhereParams { Value = end, FieldName = fieldName })));
        }

        /// <summary>
        ///   Matches fields where the value is greater than the specified value
        /// </summary>
        /// <param name = "fieldName">Name of the field.</param>
        /// <param name = "value">The value.</param>
        public void WhereGreaterThan(string fieldName, object value)
        {
            AppendOperatorIfNeeded(WhereTokens);
            NegateIfNeeded();

            fieldName = GetFieldNameForRangeQueries(fieldName, value, null);

            WhereTokens.AddLast(WhereToken.GreaterThan(fieldName, value == null ? "*" : TransformToRangeValue(new WhereParams { Value = value, FieldName = fieldName })));
        }

        /// <summary>
        ///   Matches fields where the value is greater than or equal to the specified value
        /// </summary>
        /// <param name = "fieldName">Name of the field.</param>
        /// <param name = "value">The value.</param>
        public void WhereGreaterThanOrEqual(string fieldName, object value)
        {
            AppendOperatorIfNeeded(WhereTokens);
            NegateIfNeeded();

            fieldName = GetFieldNameForRangeQueries(fieldName, value, null);

            WhereTokens.AddLast(WhereToken.GreaterThanOrEqual(fieldName, value == null ? "*" : TransformToRangeValue(new WhereParams { Value = value, FieldName = fieldName })));
        }

        /// <summary>
        ///   Matches fields where the value is less than the specified value
        /// </summary>
        /// <param name = "fieldName">Name of the field.</param>
        /// <param name = "value">The value.</param>
        public void WhereLessThan(string fieldName, object value)
        {
            AppendOperatorIfNeeded(WhereTokens);
            NegateIfNeeded();

            fieldName = GetFieldNameForRangeQueries(fieldName, value, null);

            WhereTokens.AddLast(WhereToken.LessThan(fieldName, value == null ? "NULL" : TransformToRangeValue(new WhereParams { Value = value, FieldName = fieldName })));
        }

        /// <summary>
        ///   Matches fields where the value is less than or equal to the specified value
        /// </summary>
        /// <param name = "fieldName">Name of the field.</param>
        /// <param name = "value">The value.</param>
        public void WhereLessThanOrEqual(string fieldName, object value)
        {
            AppendOperatorIfNeeded(WhereTokens);
            NegateIfNeeded();

            fieldName = GetFieldNameForRangeQueries(fieldName, value, null);

            WhereTokens.AddLast(WhereToken.LessThanOrEqual(fieldName, value == null ? "NULL" : TransformToRangeValue(new WhereParams { Value = value, FieldName = fieldName })));
        }

        /// <summary>
        ///   Add an AND to the query
        /// </summary>
        public void AndAlso()
        {
            if (WhereTokens.Last == null)
                return;

            if (WhereTokens.Last.Value is QueryOperatorToken)
                throw new InvalidOperationException("TODO");

            WhereTokens.AddLast(QueryOperatorToken.And);
        }

        /// <summary>
        ///   Add an OR to the query
        /// </summary>
        public void OrElse()
        {
            if (WhereTokens.Last == null)
                return;

            if (WhereTokens.Last.Value is QueryOperatorToken)
                throw new InvalidOperationException("TODO");

            WhereTokens.AddLast(QueryOperatorToken.Or);
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
            var whereToken = WhereTokens.Last?.Value as WhereToken;
            if (whereToken == null)
            {
                throw new InvalidOperationException("Missing where clause");
            }

            if (boost <= 0m)
            {
                throw new ArgumentOutOfRangeException(nameof(boost), "Boost factor must be a positive number");
            }

            if (boost != 1m)
            {
                // 1.0 is the default
                whereToken.Boost = boost;
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
            var whereToken = WhereTokens.Last?.Value as WhereToken;
            if (whereToken == null)
            {
                throw new InvalidOperationException("Missing where clause");
            }

            if (fuzzy < 0m || fuzzy > 1m)
            {
                throw new ArgumentOutOfRangeException(nameof(fuzzy), "Fuzzy distance must be between 0.0 and 1.0");
            }

            //var ch = QueryText[QueryText.Length - 1]; // TODO [ppekrol]
            //if (ch == '"' || ch == ']')
            //{
            //    // this check is overly simplistic
            //    throw new InvalidOperationException("Fuzzy factor can only modify single word terms");
            //}

            whereToken.Fuzzy = fuzzy;
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
            var whereToken = WhereTokens.Last?.Value as WhereToken;
            if (whereToken == null)
            {
                throw new InvalidOperationException("Missing where clause");
            }

            if (proximity < 1)
            {
                throw new ArgumentOutOfRangeException(nameof(proximity), "Proximity distance must be a positive number");
            }

            //if (QueryText[QueryText.Length - 1] != '"') // TODO [ppekrol]
            //{
            //    // this check is overly simplistic
            //    throw new InvalidOperationException("Proximity distance can only modify a phrase");
            //}

            whereToken.Proximity = proximity;
        }

        /// <summary>
        ///   Order the results by the specified fields
        ///   The fields are the names of the fields to sort, defaulting to sorting by ascending.
        ///   You can prefix a field name with '-' to indicate sorting by descending or '+' to sort by ascending
        /// </summary>
        /// <param name = "fields">The fields.</param>
        public void OrderBy(params string[] fields)
        {
            foreach (var field in fields)
            {
                var f = EnsureValidFieldName(field, isNestedPath: false);
                OrderByTokens.AddLast(OrderByToken.CreateAscending(f));
            }
        }

        /// <summary>
        ///   Order the results by the specified fields
        ///   The fields are the names of the fields to sort, defaulting to sorting by descending.
        ///   You can prefix a field name with '-' to indicate sorting by descending or '+' to sort by ascending
        /// </summary>
        /// <param name = "fields">The fields.</param>
        public void OrderByDescending(params string[] fields)
        {
            foreach (var field in fields)
            {
                var f = EnsureValidFieldName(field, isNestedPath: false);
                OrderByTokens.AddLast(OrderByToken.CreateDescending(f));
            }
        }

        /// <summary>
        ///   Instructs the query to wait for non stale results as of now.
        /// </summary>
        /// <returns></returns>
        public void WaitForNonStaleResultsAsOfNow()
        {
            TheWaitForNonStaleResults = true;
            TheWaitForNonStaleResultsAsOfNow = true;
            Timeout = DefaultTimeout;
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
        IDocumentQueryCustomization IDocumentQueryCustomization.WaitForNonStaleResultsAsOf(long cutOffEtag)
        {
            WaitForNonStaleResultsAsOf(cutOffEtag);
            return this;
        }

        /// <summary>
        /// Instructs the query to wait for non stale results as of the cutoff etag for the specified timeout.
        /// </summary>
        /// <param name="cutOffEtag">The cut off etag.</param>
        /// <param name="waitTimeout">The wait timeout.</param>
        IDocumentQueryCustomization IDocumentQueryCustomization.WaitForNonStaleResultsAsOf(long cutOffEtag, TimeSpan waitTimeout)
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
            TheWaitForNonStaleResults = true;
            TheWaitForNonStaleResultsAsOfNow = true;
            Timeout = waitTimeout;
        }

        /// <summary>
        /// Instructs the query to wait for non stale results as of the cutoff etag.
        /// </summary>
        public void WaitForNonStaleResultsAsOf(long cutOffEtag)
        {
            WaitForNonStaleResultsAsOf(cutOffEtag, DefaultTimeout);
        }

        /// <summary>
        /// Instructs the query to wait for non stale results as of the cutoff etag.
        /// </summary>
        public void WaitForNonStaleResultsAsOf(long cutOffEtag, TimeSpan waitTimeout)
        {
            TheWaitForNonStaleResults = true;
            Timeout = waitTimeout;
            CutoffEtag = cutOffEtag;
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
        public void Statistics(out QueryStatistics stats)
        {
            stats = QueryStats;
        }

        /// <summary>
        /// Callback to get the results of the query
        /// </summary>
        public void AfterQueryExecuted(Action<QueryResult> afterQueryExecutedCallback)
        {
            AfterQueryExecutedCallback += afterQueryExecutedCallback;
        }

        /// <summary>
        /// Callback to get the results of the stream
        /// </summary>
        public void AfterStreamExecuted(AfterStreamExecutedDelegate afterStreamExecutedCallback)
        {
            AfterStreamExecutedCallback += afterStreamExecutedCallback;
        }

        /// <summary>
        /// Called externally to raise the after query executed callback
        /// </summary>
        public void InvokeAfterQueryExecuted(QueryResult result)
        {
            AfterQueryExecutedCallback?.Invoke(result);
        }

        /// <summary>
        /// Called externally to raise the after stream executed callback
        /// </summary>
        public void InvokeAfterStreamExecuted(BlittableJsonReaderObject result)
        {
            AfterStreamExecutedCallback?.Invoke(result);
        }

        #endregion

        /// <summary>
        ///   Generates the index query.
        /// </summary>
        /// <param name = "query">The query.</param>
        /// <returns></returns>
        protected virtual IndexQuery GenerateIndexQuery(string query)
        {
            if (IsSpatialQuery)
            {
                if (IndexName == "dynamic" || FromToken.IsDynamic)
                    throw new NotSupportedException("Dynamic indexes do not support spatial queries. A static index, with spatial field(s), must be defined.");

                var spatialQuery = new SpatialIndexQuery
                {
                    Query = query,
                    Start = Start,
                    WaitForNonStaleResultsAsOfNow = TheWaitForNonStaleResultsAsOfNow,
                    WaitForNonStaleResults = TheWaitForNonStaleResults,
                    WaitForNonStaleResultsTimeout = Timeout,
                    CutoffEtag = CutoffEtag,
                    //SortedFields = OrderByFields.Select(x => new SortedField(x)).ToArray(),
                    DynamicMapReduceFields = DynamicMapReduceFields,
                    //FieldsToFetch = FieldsToFetch,
                    SpatialFieldName = SpatialFieldName,
                    QueryShape = QueryShape,
                    RadiusUnitOverride = SpatialUnits,
                    SpatialRelation = SpatialRelation,
                    DistanceErrorPercentage = DistanceErrorPct,
                    //DefaultOperator = DefaultOperator,
                    HighlightedFields = HighlightedFields.Select(x => x.Clone()).ToArray(),
                    HighlighterPreTags = HighlighterPreTags.ToArray(),
                    HighlighterPostTags = HighlighterPostTags.ToArray(),
                    HighlighterKeyName = HighlighterKeyName,
                    Transformer = Transformer,
                    AllowMultipleIndexEntriesForSameDocumentToResultTransformer = AllowMultipleIndexEntriesForSameDocumentToResultTransformer,
                    TransformerParameters = TransformerParameters,
                    DisableCaching = DisableCaching,
                    ShowTimings = ShowQueryTimings,
                    ExplainScores = ShouldExplainScores,
                    Includes = Includes.ToArray()
                };

                if (PageSize.HasValue)
                    spatialQuery.PageSize = PageSize.Value;

                return spatialQuery;
            }

            var indexQuery = new IndexQuery
            {
                Query = query,
                Start = Start,
                CutoffEtag = CutoffEtag,
                WaitForNonStaleResultsAsOfNow = TheWaitForNonStaleResultsAsOfNow,
                WaitForNonStaleResults = TheWaitForNonStaleResults,
                WaitForNonStaleResultsTimeout = Timeout,
                //SortedFields = OrderByFields.Select(x => new SortedField(x)).ToArray(),
                DynamicMapReduceFields = DynamicMapReduceFields,
                //FieldsToFetch = FieldsToFetch,
                //DefaultOperator = DefaultOperator,
                HighlightedFields = HighlightedFields.Select(x => x.Clone()).ToArray(),
                HighlighterPreTags = HighlighterPreTags.ToArray(),
                HighlighterPostTags = HighlighterPostTags.ToArray(),
                HighlighterKeyName = HighlighterKeyName,
                Transformer = Transformer,
                TransformerParameters = TransformerParameters,
                AllowMultipleIndexEntriesForSameDocumentToResultTransformer = AllowMultipleIndexEntriesForSameDocumentToResultTransformer,
                DisableCaching = DisableCaching,
                ShowTimings = ShowQueryTimings,
                ExplainScores = ShouldExplainScores,
                Includes = Includes.ToArray()
            };

            if (PageSize != null)
                indexQuery.PageSize = PageSize.Value;

            return indexQuery;
        }

        /// <summary>
        /// Perform a search for documents which fields that match the searchTerms.
        /// If there is more than a single term, each of them will be checked independently.
        /// </summary>
        public void Search(string fieldName, string searchTerms, EscapeQueryOptions escapeQueryOptions = EscapeQueryOptions.RawQuery)
        {
            AppendOperatorIfNeeded(WhereTokens);
            NegateIfNeeded();

            fieldName = EnsureValidFieldName(fieldName, isNestedPath: false);

            switch (escapeQueryOptions)
            {
                case EscapeQueryOptions.EscapeAll:
                    searchTerms = RavenQuery.Escape(searchTerms, false, false);
                    break;
                case EscapeQueryOptions.AllowPostfixWildcard:
                    searchTerms = RavenQuery.Escape(searchTerms, false, false);
                    searchTerms = EscapePostfixWildcard.Replace(searchTerms, "*${1}");
                    break;
                case EscapeQueryOptions.AllowAllWildcards:
                    searchTerms = RavenQuery.Escape(searchTerms, false, false);
                    searchTerms = searchTerms.Replace("\\*", "*");
                    break;
                case EscapeQueryOptions.RawQuery:
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(escapeQueryOptions), "Value: " + escapeQueryOptions);
            }
            var hasWhiteSpace = searchTerms.Any(char.IsWhiteSpace);
            LastEquality = new KeyValuePair<string, object>(fieldName,
                hasWhiteSpace ? "(" + searchTerms + ")" : searchTerms
                );

            WhereTokens.AddLast(WhereToken.Search(fieldName, searchTerms));
        }

        /// <summary>
        ///   Returns a <see cref = "System.String" /> that represents the query for this instance.
        /// </summary>
        /// <returns>
        ///   A <see cref = "System.String" /> that represents the query for this instance.
        /// </returns>
        public override string ToString()
        {
            if (_currentClauseDepth != 0)
                throw new InvalidOperationException(string.Format("A clause was not closed correctly within this query, current clause depth = {0}", _currentClauseDepth));

            var queryText = new StringBuilder();

            BuildSelect(queryText);
            BuildFrom(queryText);
            BuildWhere(queryText);
            BuildOrderBy(queryText);

            return queryText.ToString();
        }

        /// <summary>
        /// The last term that we asked the query to use equals on
        /// </summary>
        public KeyValuePair<string, object> GetLastEqualityTerm(bool isAsync = false)
        {
            return LastEquality;
        }

        public void Intersect()
        {
            var last = WhereTokens.Last?.Value;
            if (last is WhereToken || last is CloseSubclauseToken)
                WhereTokens.AddLast(IntersectToken.Instance);

            throw new InvalidOperationException("Cannot add INTERSECT at this point.");
        }

        public void ContainsAny(string fieldName, IEnumerable<object> values)
        {
            AppendOperatorIfNeeded(WhereTokens);
            NegateIfNeeded();

            fieldName = EnsureValidFieldName(fieldName, isNestedPath: false);

            WhereTokens.AddLast(WhereToken.ContainsAny(fieldName, TransformEnumerable(fieldName, UnpackEnumerable(values))));
        }

        public void ContainsAll(string fieldName, IEnumerable<object> values)
        {
            AppendOperatorIfNeeded(WhereTokens);
            NegateIfNeeded();

            fieldName = EnsureValidFieldName(fieldName, isNestedPath: false);

            WhereTokens.AddLast(WhereToken.ContainsAll(fieldName, TransformEnumerable(fieldName, UnpackEnumerable(values))));
        }

        public void AddRootType(Type type)
        {
            RootTypes.Add(type);
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
            var memberExpression = _linqPathProvider.GetMemberExpression(expression);
            return FieldUtil.ApplyRangeSuffixIfNecessary(memberQueryPath, memberExpression.Type);
        }

        public string GetMemberQueryPath(Expression expression)
        {
            var result = _linqPathProvider.GetPath(expression);
            result.Path = result.Path.Substring(result.Path.IndexOf('.') + 1);

            if (expression.NodeType == ExpressionType.ArrayLength)
                result.Path += ".Length";

            var propertyName = IndexName == null || FromToken.IsDynamic
                ? _conventions.FindPropertyNameForDynamicIndex(typeof(T), IndexName, "", result.Path)
                : _conventions.FindPropertyNameForIndex(typeof(T), IndexName, "", result.Path);
            return propertyName;
        }

        public void SetAllowMultipleIndexEntriesForSameDocumentToResultTransformer(bool val)
        {
            AllowMultipleIndexEntriesForSameDocumentToResultTransformer = val;
        }

        public void SetTransformer(string transformer)
        {
            Transformer = transformer;
        }

        public void Distinct()
        {
            if (IsDistinct)
                throw new InvalidOperationException("This is already a distinct query.");

            SelectTokens.AddFirst(DistinctToken.Instance);
        }

        private void UpdateStatsAndHighlightings(QueryResult queryResult)
        {
            QueryStats.UpdateQueryStats(queryResult);
            Highlightings.Update(queryResult);
        }

        private void BuildSelect(StringBuilder writer)
        {
            if (SelectTokens.Count == 0)
                return;

            writer
                .Append("SELECT ");

            var token = SelectTokens.First;
            while (token != null)
            {
                AddSpaceIfNeeded(token.Previous?.Value, token.Value, writer);
                token.Value.WriteTo(writer);

                token = token.Next;
            }
        }

        private void BuildFrom(StringBuilder writer)
        {
            AddSpaceIfNeeded(SelectTokens.Last?.Value, FromToken, writer);
            FromToken.WriteTo(writer);
        }

        private void BuildWhere(StringBuilder writer)
        {
            if (WhereTokens.Count == 0)
                return;

            writer
                .Append(" WHERE ");

            var token = WhereTokens.First;
            while (token != null)
            {
                AddSpaceIfNeeded(token.Previous?.Value, token.Value, writer);
                token.Value.WriteTo(writer);

                token = token.Next;
            }
        }

        private void BuildOrderBy(StringBuilder writer)
        {
            if (OrderByTokens.Count == 0)
                return;

            writer
                .Append(" ORDER BY ");

            var token = OrderByTokens.First;
            while (token != null)
            {
                if (token.Previous != null)
                    writer.Append(", ");

                token.Value.WriteTo(writer);

                token = token.Next;
            }
        }

        private static void AddSpaceIfNeeded(QueryToken previousToken, QueryToken currentToken, StringBuilder writer)
        {
            if (previousToken == null)
                return;

            if (previousToken is OpenSubclauseToken || currentToken is CloseSubclauseToken)
                return;

            writer.Append(" ");
        }

        private void AppendOperatorIfNeeded(LinkedList<QueryToken> tokens)
        {
            if (tokens.Count == 0)
                return;

            if (tokens.Last.Value is WhereToken || tokens.Last.Value is CloseSubclauseToken)
                tokens.AddLast(DefaultOperator == QueryOperator.And ? QueryOperatorToken.And : QueryOperatorToken.Or);
        }

        private IEnumerable<object> TransformEnumerable(string fieldName, IEnumerable<object> values)
        {
            foreach (var value in values)
            {
                var enumerable = value as IEnumerable;
                if (enumerable != null && value is string == false)
                {
                    foreach (var transformedValue in TransformEnumerable(fieldName, enumerable.Cast<object>()))
                        yield return transformedValue;

                    continue;
                }

                var nestedWhereParams = new WhereParams
                {
                    AllowWildcards = true,
                    IsAnalyzed = true,
                    FieldName = fieldName,
                    Value = value
                };

                yield return TransformToEqualValue(nestedWhereParams);
            }
        }

        private void NegateIfNeeded()
        {
            if (Negate == false)
                return;

            Negate = false;
            WhereTokens.AddLast(NegateToken.Instance);
        }

        private static IEnumerable<object> UnpackEnumerable(IEnumerable items)
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

        private string EnsureValidFieldName(string fieldName, bool isNestedPath)
        {
            if (TheSession?.Conventions == null || isNestedPath)
                return fieldName;

            if (IsMapReduce)
            {
                if (IsDynamicMapReduce)
                {
                    string name;
                    var rangeType = FieldUtil.GetRangeTypeFromFieldName(fieldName, out name);

                    var renamedField = DynamicMapReduceFields.FirstOrDefault(x => x.ClientSideName == name);

                    if (renamedField != null)
                        return FieldUtil.ApplyRangeSuffixIfNecessary(renamedField.Name, rangeType);
                }

                return fieldName;
            }

            foreach (var rootType in RootTypes)
            {
                var identityProperty = TheSession.Conventions.GetIdentityProperty(rootType);
                if (identityProperty != null && identityProperty.Name == fieldName)
                {
                    return Constants.Documents.Indexing.Fields.DocumentIdFieldName;
                }
            }

            return fieldName;
        }

        private string GetFieldNameForRangeQueries(string fieldName, object start, object end)
        {
            fieldName = EnsureValidFieldName(fieldName, isNestedPath: false);

            if (fieldName == Constants.Documents.Indexing.Fields.DocumentIdFieldName)
                return fieldName;

            var val = start ?? end;
            return FieldUtil.ApplyRangeSuffixIfNecessary(fieldName, val);
        }

        private object TransformToEqualValue(WhereParams whereParams)
        {
            if (whereParams.Value == null)
            {
                return null;
            }
            if (Equals(whereParams.Value, string.Empty))
            {
                return string.Empty;
            }

            var type = whereParams.Value.GetType().GetNonNullableType();

            if (_conventions.SaveEnumsAsIntegers && type.GetTypeInfo().IsEnum)
            {
                return (int)whereParams.Value;
            }

            if (type == typeof(bool))
            {
                return (bool)whereParams.Value;
            }
            if (type == typeof(DateTime))
            {
                var val = (DateTime)whereParams.Value;
                var s = val.GetDefaultRavenFormat(isUtc: val.Kind == DateTimeKind.Utc);
                return s;
            }
            if (type == typeof(DateTimeOffset))
            {
                var val = (DateTimeOffset)whereParams.Value;
                return val.UtcDateTime.GetDefaultRavenFormat(true);
            }

            if (type == typeof(decimal))
            {
                return (double)(decimal)whereParams.Value;
            }

            if (type == typeof(double))
            {
                return (double)whereParams.Value;
            }

            var strValue = whereParams.Value as string;
            if (strValue != null)
            {
                strValue = RavenQuery.Escape(strValue,
                    whereParams.AllowWildcards && whereParams.IsAnalyzed, whereParams.IsAnalyzed);

                return whereParams.IsAnalyzed ? strValue : string.Concat("[[", strValue, "]]");
            }

            if (_conventions.TryConvertValueForQuery(whereParams.FieldName, whereParams.Value, QueryValueConvertionType.Equality, out strValue))
                return strValue;

            if (whereParams.Value is ValueType)
            {
                var escaped = RavenQuery.Escape(Convert.ToString(whereParams.Value, CultureInfo.InvariantCulture), whereParams.AllowWildcards && whereParams.IsAnalyzed, true);

                return escaped;
            }

            var result = GetImplicitStringConversion(whereParams.Value.GetType());
            if (result != null)
            {
                return RavenQuery.Escape(result(whereParams.Value), whereParams.AllowWildcards && whereParams.IsAnalyzed, true);
            }

            throw new NotImplementedException("This feature is not yet implemented");
            /*
            var jsonSerializer = _conventions.CreateSerializer();
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
            */
        }

        private static Func<object, string> GetImplicitStringConversion(Type type)
        {
            if (type == null)
                return null;

            Func<object, string> value;
            var localStringsCache = _implicitStringsCache;
            if (localStringsCache.TryGetValue(type, out value))
                return value;

            var methodInfo = type.GetMethod("op_Implicit", new[] { type });

            if (methodInfo == null || methodInfo.ReturnType != typeof(string))
            {
                _implicitStringsCache = new Dictionary<Type, Func<object, string>>(localStringsCache)
                {
                    {type, null}
                };
                return null;
            }

            var arg = Expression.Parameter(typeof(object), "self");

            var func = (Func<object, string>)Expression.Lambda(Expression.Call(methodInfo, Expression.Convert(arg, type)), arg).Compile();

            _implicitStringsCache = new Dictionary<Type, Func<object, string>>(localStringsCache)
            {
                {type, func}
            };
            return func;
        }

        private string TransformToRangeValue(WhereParams whereParams)
        {
            if (whereParams.Value == null)
                return Constants.Documents.Indexing.Fields.NullValueNotAnalyzed;
            if (Equals(whereParams.Value, string.Empty))
                return Constants.Documents.Indexing.Fields.EmptyStringNotAnalyzed;

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
            if (_conventions.TryConvertValueForQuery(whereParams.FieldName, whereParams.Value, QueryValueConvertionType.Range,
                out strVal))
                return strVal;

            if (whereParams.Value is ValueType)
                return RavenQuery.Escape(Convert.ToString(whereParams.Value, CultureInfo.InvariantCulture),
                    false, true);

            var stringWriter = new StringWriter();
            _conventions.CreateSerializer().Serialize(stringWriter, whereParams.Value);

            var sb = stringWriter.GetStringBuilder();
            if (sb.Length > 1 && sb[0] == '"' && sb[sb.Length - 1] == '"')
            {
                sb.Remove(sb.Length - 1, 1);
                sb.Remove(0, 1);
            }

            return RavenQuery.Escape(sb.ToString(), false, true);
        }
    }
}
