//-----------------------------------------------------------------------
// <copyright file="DocumentQuery.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading;
#if !NET_3_5
using Raven.Client.Connection.Async;
using System.Threading.Tasks;
#endif
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Client.Exceptions;
using Raven.Client.Listeners;
using Raven.Json.Linq;
using Newtonsoft.Json.Linq;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Linq;

namespace Raven.Client.Document
{
	/// <summary>
    ///   A query against a Raven index
    /// </summary>
    public abstract class AbstractDocumentQuery<T, TSelf> : IDocumentQueryCustomization, IRavenQueryInspector, IAbstractDocumentQuery<T> where TSelf : AbstractDocumentQuery<T, TSelf>
    {
        /// <summary>
        /// Whatever to negate the next operation
        /// </summary>
        protected bool negate;
#if !SILVERLIGHT
        /// <summary>
        /// The database commands to use
        /// </summary>
        protected readonly IDatabaseCommands theDatabaseCommands;
#endif
#if !NET_3_5
        /// <summary>
        /// Async database commands to use
        /// </summary>
        protected readonly IAsyncDatabaseCommands theAsyncDatabaseCommands;
#endif
        /// <summary>
        /// The index to query
        /// </summary>
        protected readonly string indexName;
        private int currentClauseDepth;

        private KeyValuePair<string, string> lastEquality;

        /// <summary>
        ///   The list of fields to project directly from the index
        /// </summary>
        protected readonly string[] projectionFields;

        /// <summary>
        /// The query listeners for this query
        /// </summary>
        protected readonly IDocumentQueryListener[] queryListeners;
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
        ///   The types to sort the fields by (NULL if not specified)
        /// </summary>
        protected HashSet<KeyValuePair<string, Type>> sortByHints = new HashSet<KeyValuePair<string, Type>>();

        /// <summary>
        ///   The page size to use when querying the index
        /// </summary>
        protected int? pageSize;

        private QueryResult queryResult;

        /// <summary>
        /// The query to use
        /// </summary>
        protected StringBuilder theQueryText = new StringBuilder();

        /// <summary>
        ///   which record to start reading from
        /// </summary>
        protected int start;

        /// <summary>
        /// Timeout for this query
        /// </summary>
        protected TimeSpan timeout;
        /// <summary>
        /// Should we wait for non stale results
        /// </summary>
        protected bool theWaitForNonStaleResults;
        /// <summary>
        /// The paths to include when loading the query
        /// </summary>
        protected HashSet<string> includes = new HashSet<string>();
        /// <summary>
        /// What aggregated operation to execute
        /// </summary>
        protected AggregationOperation aggregationOp;
        /// <summary>
        /// Fields to group on
        /// </summary>
        protected string[] groupByFields;
#if !NET_3_5
        private Task<QueryResult> queryResultTask;
#endif

        /// <summary>
        ///   Get the name of the index being queried
        /// </summary>
        public string IndexQueried
        {
            get { return indexName; }
        }

#if !SILVERLIGHT
        /// <summary>
        ///   Grant access to the database commands
        /// </summary>
        public IDatabaseCommands DatabaseCommands
        {
            get { return theDatabaseCommands; }
        }
#endif

#if !NET_3_5
        /// <summary>
        ///   Grant access to the async database commands
        /// </summary>
        public IAsyncDatabaseCommands AsyncDatabaseCommands
        {
            get { return theAsyncDatabaseCommands; }
        }
#endif

		/// <summary>
		/// Gets the document convention from the query session
		/// </summary>
		public DocumentConvention DocumentConvention
		{
			get { return this.theSession.Conventions; }
		}

#if !SILVERLIGHT
        /// <summary>
        ///   Gets the session associated with this document query
        /// </summary>
        public IDocumentSession Session
        {
            get { return (IDocumentSession)theSession; }
        }
#endif

        /// <summary>
        ///   Gets the query text built so far
        /// </summary>
        protected StringBuilder QueryText
        {
            get { return theQueryText; }
        }


#if !SILVERLIGHT && !NET_3_5
        /// <summary>
        ///   Initializes a new instance of the <see cref = "DocumentQuery&lt;T&gt;" /> class.
        /// </summary>
        /// <param name = "theSession">The session.</param>
        /// <param name = "databaseCommands">The database commands.</param>
        /// <param name = "indexName">Name of the index.</param>
        /// <param name = "projectionFields">The projection fields.</param>
        public AbstractDocumentQuery(InMemoryDocumentSessionOperations theSession,
                                     IDatabaseCommands databaseCommands,
                                     string indexName,
                                     string[] projectionFields,
                                     IDocumentQueryListener[] queryListeners)
            : this(theSession, databaseCommands, null, indexName, projectionFields, queryListeners)
        {
        }
#endif

        /// <summary>
        /// Initializes a new instance of the <see cref="DocumentQuery&lt;T&gt;"/> class.
        /// </summary>
        /// <param name="databaseCommands">The database commands.</param>
#if !NET_3_5
        /// <param name="asyncDatabaseCommands">The async database commands</param>
#endif

        /// <param name = "indexName">Name of the index.</param>
        /// <param name = "projectionFields">The projection fields.</param>
        /// <param name = "theSession">The session.</param>
        public AbstractDocumentQuery(InMemoryDocumentSessionOperations theSession,
#if !SILVERLIGHT
 IDatabaseCommands databaseCommands,
#endif
#if !NET_3_5
 IAsyncDatabaseCommands asyncDatabaseCommands,
#endif
 string indexName,
                                     string[] projectionFields,
                                     IDocumentQueryListener[] queryListeners)
        {
#if !SILVERLIGHT
            this.theDatabaseCommands = databaseCommands;
#endif
            this.projectionFields = projectionFields;
            this.queryListeners = queryListeners;
            this.indexName = indexName;
            this.theSession = theSession;
#if !NET_3_5
            this.theAsyncDatabaseCommands = asyncDatabaseCommands;
#endif
        }

        /// <summary>
        ///   Initializes a new instance of the <see cref = "DocumentQuery&lt;T&gt;" /> class.
        /// </summary>
        /// <param name = "other">The other.</param>
        protected AbstractDocumentQuery(AbstractDocumentQuery<T, TSelf> other)
        {
#if !SILVERLIGHT
            theDatabaseCommands = other.theDatabaseCommands;
#endif
#if !NET_3_5
            theAsyncDatabaseCommands = other.theAsyncDatabaseCommands;
#endif
            indexName = other.indexName;
            projectionFields = other.projectionFields;
            theSession = other.theSession;
            cutoff = other.cutoff;
            orderByFields = other.orderByFields;
            sortByHints = other.sortByHints;
            pageSize = other.pageSize;
            theQueryText = other.theQueryText;
            start = other.start;
            timeout = other.timeout;
            theWaitForNonStaleResults = other.theWaitForNonStaleResults;
            includes = other.includes;
            queryListeners = other.queryListeners;
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
        /// Selects the specified fields directly from the index
        /// </summary>
        /// <typeparam name="TProjection">The type of the projection.</typeparam>
        /// <param name="fields">The fields.</param>
        IDocumentQueryCustomization IDocumentQueryCustomization.CreateQueryForSelectedFields<TProjection>(params string[] fields)
        {
            return CreateQueryForSelectedFields<TProjection>(fields);
        }


        /// <summary>
        /// Selects the specified fields directly from the index
        /// </summary>
        protected abstract IDocumentQueryCustomization CreateQueryForSelectedFields<TProjection>(string[] fields);

        /// <summary>
        ///   Filter matches to be inside the specified radius
        /// </summary>
        /// <param name = "radius">The radius.</param>
        /// <param name = "latitude">The latitude.</param>
        /// <param name = "longitude">The longitude.</param>
        IDocumentQueryCustomization IDocumentQueryCustomization.WithinRadiusOf(double radius, double latitude,
                                                                               double longitude)
        {
            GenerateQueryWithinRadiusOf(radius, latitude, longitude);
            return this;
        }

        /// <summary>
        ///   Filter matches to be inside the specified radius
        /// </summary>
        /// <param name = "radius">The radius.</param>
        /// <param name = "latitude">The latitude.</param>
        /// <param name = "longitude">The longitude.</param>
        protected abstract object GenerateQueryWithinRadiusOf(double radius, double latitude, double longitude);

        /// <summary>
        ///   EXPERT ONLY: Instructs the query to wait for non stale results.
        ///   This shouldn't be used outside of unit tests unless you are well aware of the implications
        /// </summary>
        IDocumentQueryCustomization IDocumentQueryCustomization.WaitForNonStaleResults()
        {
            WaitForNonStaleResults();
            return this;
        }

        /// <summary>
        ///   Includes the specified path in the query, loading the document specified in that path
        /// </summary>
        /// <param name = "path">The path.</param>
        IDocumentQueryCustomization IDocumentQueryCustomization.Include<TResult>(Expression<Func<TResult, object>> path)
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
            timeout = waitTimeout;
        }

#if !SILVERLIGHT
        /// <summary>
        ///   Gets the query result
        ///   Execute the query the first time that this is called.
        /// </summary>
        /// <value>The query result.</value>
        public QueryResult QueryResult
        {
            get
            {
            	var currentQueryResults = queryResult ?? (queryResult = GetQueryResult());
            	return currentQueryResults.CreateSnapshot();
            }
        }
#endif

#if !NET_3_5
        /// <summary>
        ///   Gets the query result
        ///   Execute the query the first time that this is called.
        /// </summary>
        /// <value>The query result.</value>
        public Task<QueryResult> QueryResultAsync
        {
            get
            {
            	var currentQueryResultTask = queryResultTask ?? (queryResultTask = GetQueryResultAsync());
            	return currentQueryResultTask
            		.ContinueWith(x => x.Result.CreateSnapshot());
            }
        }
#endif

        /// <summary>
        ///   Gets the fields for projection
        /// </summary>
        /// <returns></returns>
        public IEnumerable<string> GetProjectionFields()
        {
            return projectionFields ?? Enumerable.Empty<string>();
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
            fieldName = EnsureValidFieldName(new WhereEqualsParams
            {
                FieldName = fieldName
            });
            fieldName = descending ? "-" + fieldName : fieldName;
            orderByFields = orderByFields.Concat(new[] { fieldName }).ToArray();
            sortByHints.Add(new KeyValuePair<string, Type>(fieldName, fieldType));
        }

#if !SILVERLIGHT
        /// <summary>
        ///   Gets the enumerator.
        /// </summary>
        public IEnumerator<T> GetEnumerator()
        {
			var sp = Stopwatch.StartNew();
            do
            {
                try
                {
                    var currentQueryResults = QueryResult;
                    foreach (var include in currentQueryResults.Includes)
                    {
                        var metadata = include.Value<RavenJObject>("@metadata");

                        theSession.TrackEntity<object>(metadata.Value<string>("@id"),
                                                    include,
                                                    metadata);
                    }
                    var list = currentQueryResults.Results
                        .Select(Deserialize)
                        .ToList();
                    return list.GetEnumerator();
                }
                catch (NonAuthoritiveInformationException)
                {
                    if (sp.Elapsed > theSession.NonAuthoritiveInformationTimeout)
                        throw;
                    queryResult = null;
                    // we explicitly do NOT want to consider retries for non authoritive information as 
                    // additional request counted against the session quota
                    theSession.DecrementRequestCount();
                }
            } while (true);
        }
#else
        /// <summary>
        ///   Gets the enumerator.
        /// </summary>
        public Task<IEnumerator<T>> GetEnumeratorAsync()
        {
			var startTime = DateTime.Now;
        	return QueryResultAsync
				.ContinueWith(t => ProcessEnumerator(t, startTime))
				.Unwrap();
        }

		private Task<IEnumerator<T>> ProcessEnumerator(Task<QueryResult> t, DateTime startTime)
		{
			try
			{
				queryResult = t.Result;
                foreach (var include in queryResult.Includes)
                {
                    var metadata = include.Value<RavenJObject>("@metadata");

                    theSession.TrackEntity<object>(metadata.Value<string>("@id"),
                                                include,
                                                metadata);
                }
                var list = queryResult.Results
                    .Select(Deserialize)
                    .ToList();
				return TaskEx.Run(() => (IEnumerator<T>)list.GetEnumerator());
			}
            catch (NonAuthoritiveInformationException)
            {
                if ((DateTime.Now - startTime) > theSession.NonAuthoritiveInformationTimeout)
                    throw;
                queryResult = null;
                // we explicitly do NOT want to consider retries for non authoritive information as 
                // additional request counted against the session quota
                theSession.DecrementRequestCount();

            	return QueryResultAsync
					.ContinueWith(t2 => ProcessEnumerator(t2, startTime))
					.Unwrap();
            }
		}

#endif
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
Consider using session.Query<T>() instead of session.LuceneQuery<T>. The session.Query<T>() method fully supports Linq queries, while session.LuceneQuery<T>() is intended for lower level API access.
If you really want to do in memory filtering on the data returned from the query, you can use: session.LuceneQuery<T>().ToList().Where(x=>x.Name == ""Ayende"")
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
Consider using session.Query<T>() instead of session.LuceneQuery<T>. The session.Query<T>() method fully supports Linq queries, while session.LuceneQuery<T>() is intended for lower level API access.
If you really want to do in memory filtering on the data returned from the query, you can use: session.LuceneQuery<T>().ToList().Count(x=>x.Name == ""Ayende"")
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
Consider using session.Query<T>() instead of session.LuceneQuery<T>. The session.Query<T>() method fully supports Linq queries, while session.LuceneQuery<T>() is intended for lower level API access.
If you really want to do in memory filtering on the data returned from the query, you can use: session.LuceneQuery<T>().ToList().Count()
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

        /// <summary>
        ///   Filter the results from the index using the specified where clause.
        /// </summary>
        /// <param name = "whereClause">The where clause.</param>
        public void Where(string whereClause)
        {
            if (theQueryText.Length > 0)
            {
                theQueryText.Append(" ");
            }

            theQueryText.Append(whereClause);
        }

        /// <summary>
        ///   Matches exact value
        /// </summary>
        /// <remarks>
        ///   Defaults to NotAnalyzed
        /// </remarks>
        public void WhereEquals(string fieldName, object value)
        {
            WhereEquals(new WhereEqualsParams
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
            WhereEquals(new WhereEqualsParams
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
            if (theQueryText.Length > 0 && theQueryText[theQueryText.Length - 1] != '(')
            {
                theQueryText.Append(" ");
            }
            NegateIfNeeded();
            theQueryText.Append("(");
        }

        ///<summary>
        ///  Instruct the index to group by the specified fields using the specified aggregation operation
        ///</summary>
        ///<remarks>
        ///  This is only valid on dynamic indexes queries
        ///</remarks>
        public void GroupBy(AggregationOperation aggregationOperation, params string[] fieldsToGroupBy)
        {
            groupByFields = fieldsToGroupBy;
            aggregationOp = aggregationOperation;
        }

        /// <summary>
        ///   Simplified method for closing a clause within the query
        /// </summary>
        /// <returns></returns>
        public void CloseSubclause()
        {
            currentClauseDepth--;
            theQueryText.Append(")");
        }

        /// <summary>
        ///   Matches exact value
        /// </summary>
        public void WhereEquals(WhereEqualsParams whereEqualsParams)
        {
            EnsureValidFieldName(whereEqualsParams);
            var transformToEqualValue = TransformToEqualValue(whereEqualsParams);
			lastEquality = new KeyValuePair<string, string>(whereEqualsParams.FieldName, transformToEqualValue);
            if (theQueryText.Length > 0 && theQueryText[theQueryText.Length - 1] != '(')
            {
                theQueryText.Append(" ");
            }

            NegateIfNeeded();

            theQueryText.Append(whereEqualsParams.FieldName);
            theQueryText.Append(":");
            theQueryText.Append(transformToEqualValue);
        }

        private string EnsureValidFieldName(WhereEqualsParams whereEqualsParams)
        {
			if (theSession == null || theSession.Conventions == null || whereEqualsParams.IsNestedPath)
				return whereEqualsParams.FieldName;

            var identityProperty = theSession.Conventions.GetIdentityProperty(typeof(T));
			if (identityProperty == null || identityProperty.Name != whereEqualsParams.FieldName)
				return whereEqualsParams.FieldName;
        	
			return whereEqualsParams.FieldName = Constants.DocumentIdFieldName;
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
            theQueryText.Append("-");
        }

        /// <summary>
        ///   Matches substrings of the field
        /// </summary>
        public void WhereContains(string fieldName, object value)
        {
            WhereEquals(new WhereEqualsParams
            {
                AllowWildcards = true,
                IsAnalyzed = true,
                FieldName = fieldName,
                Value = value
            });
        }

		/// <summary>
		///   Matches substrings of the field
		/// </summary>
		public void WhereContains(string fieldName, params object [] values)
		{
			if (values == null || values.Length == 0)
				return;

			OpenSubclause();

			WhereContains(fieldName, values[0]);


			for (var i = 1; i < values.Length; i++)
			{
				OrElse();
				WhereContains(fieldName, values[i]);
			}

			CloseSubclause();
		}

		/// <summary>
		///   Matches substrings of the field
		/// </summary>
		public void WhereContains(string fieldName, IEnumerable<object> values)
		{
			WhereContains(fieldName, values.ToArray());
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
                new WhereEqualsParams
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
                new WhereEqualsParams
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
            if (theQueryText.Length > 0)
            {
                theQueryText.Append(" ");
            }

            if ((start ?? end) != null)
                sortByHints.Add(new KeyValuePair<string, Type>(fieldName, (start ?? end).GetType()));

            NegateIfNeeded();

            fieldName = EnsureValidFieldName(new WhereEqualsParams { FieldName = fieldName });

            theQueryText.Append(fieldName).Append(":{");
            theQueryText.Append(start == null ? "*" : TransformToRangeValue(start));
            theQueryText.Append(" TO ");
            theQueryText.Append(end == null ? "NULL" : TransformToRangeValue(end));
            theQueryText.Append("}");
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
            if (theQueryText.Length > 0)
            {
                theQueryText.Append(" ");
            }
            if ((start ?? end) != null)
                sortByHints.Add(new KeyValuePair<string, Type>(fieldName, (start ?? end).GetType()));

            NegateIfNeeded();

            fieldName = EnsureValidFieldName(new WhereEqualsParams { FieldName = fieldName });
            theQueryText.Append(fieldName).Append(":[");
            theQueryText.Append(start == null ? "*" : TransformToRangeValue(start));
            theQueryText.Append(" TO ");
            theQueryText.Append(end == null ? "NULL" : TransformToRangeValue(end));
            theQueryText.Append("]");
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
            if (theQueryText.Length < 1)
            {
                throw new InvalidOperationException("Missing where clause");
            }

            theQueryText.Append(" AND");
        }

        /// <summary>
        ///   Add an OR to the query
        /// </summary>
        public void OrElse()
        {
            if (theQueryText.Length < 1)
            {
                throw new InvalidOperationException("Missing where clause");
            }

            theQueryText.Append(" OR");
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
            if (theQueryText.Length < 1)
            {
                throw new InvalidOperationException("Missing where clause");
            }

            if (boost <= 0m)
            {
                throw new ArgumentOutOfRangeException("Boost factor must be a positive number");
            }

            if (boost != 1m)
            {
                // 1.0 is the default
                theQueryText.Append("^").Append(boost);
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
            if (theQueryText.Length < 1)
            {
                throw new InvalidOperationException("Missing where clause");
            }

            if (fuzzy < 0m || fuzzy > 1m)
            {
                throw new ArgumentOutOfRangeException("Fuzzy distance must be between 0.0 and 1.0");
            }

            var ch = theQueryText[theQueryText.Length - 1];
            if (ch == '"' || ch == ']')
            {
                // this check is overly simplistic
                throw new InvalidOperationException("Fuzzy factor can only modify single word terms");
            }

            theQueryText.Append("~");
            if (fuzzy != 0.5m)
            {
                // 0.5 is the default
                theQueryText.Append(fuzzy);
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
            if (theQueryText.Length < 1)
            {
                throw new InvalidOperationException("Missing where clause");
            }

            if (proximity < 1)
            {
                throw new ArgumentOutOfRangeException("proximity", "Proximity distance must be positive number");
            }

            if (theQueryText[theQueryText.Length - 1] != '"')
            {
                // this check is overly simplistic
                throw new InvalidOperationException("Proximity distance can only modify a phrase");
            }

            theQueryText.Append("~").Append(proximity);
        }

        /// <summary>
        ///   Order the results by the specified fields
        /// </summary>
        /// <remarks>
        ///   The fields are the names of the fields to sort, defaulting to sorting by ascending.
        ///   You can prefix a field name with '-' to indicate sorting by descending or '+' to sort by ascending
        /// </remarks>
        /// <param name = "fields">The fields.</param>
        public void OrderBy(params string[] fields)
        {
            orderByFields = orderByFields.Concat(fields).ToArray();
        }

        /// <summary>
        ///   Instructs the query to wait for non stale results as of now.
        /// </summary>
        /// <returns></returns>
        public void WaitForNonStaleResultsAsOfNow()
        {
            theWaitForNonStaleResults = true;
            cutoff = DateTime.UtcNow;
            timeout = TimeSpan.FromSeconds(15);
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
            cutoff = DateTime.UtcNow;
            timeout = waitTimeout;
        }

        /// <summary>
        ///   Instructs the query to wait for non stale results as of the cutoff date.
        /// </summary>
        /// <param name = "cutOff">The cut off.</param>
        /// <returns></returns>
        public void WaitForNonStaleResultsAsOf(DateTime cutOff)
        {
            WaitForNonStaleResultsAsOf(cutOff, TimeSpan.FromSeconds(15));
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
        ///   EXPERT ONLY: Instructs the query to wait for non stale results.
        ///   This shouldn't be used outside of unit tests unless you are well aware of the implications
        /// </summary>
        public void WaitForNonStaleResults()
        {
            WaitForNonStaleResults(TimeSpan.FromSeconds(15));
        }

        #endregion

#if !NET_3_5
        private Task<QueryResult> GetQueryResultAsync()
        {
            theSession.IncrementRequestCount();
            var startTime = DateTime.Now;

            var query = theQueryText.ToString();

            Debug.WriteLine(string.Format("Executing query '{0}' on index '{1}' in '{2}'",
                                          query, indexName, theSession.StoreIdentifier));

            var indexQuery = GenerateIndexQuery(query);

            AddOperationHeaders(theAsyncDatabaseCommands.OperationsHeaders.Add);

            return GetQueryResultTaskResult(query, indexQuery, startTime);
        }

        private Task<QueryResult> GetQueryResultTaskResult(string query, IndexQuery indexQuery, DateTime startTime)
        {
            return theAsyncDatabaseCommands.QueryAsync(indexName, indexQuery, includes.ToArray())
                .ContinueWith(task =>
                {
                    if (theWaitForNonStaleResults && task.Result.IsStale)
                    {
                        var elapsed1 = DateTime.Now - startTime;
                        if (elapsed1 > timeout)
                        {
                            throw new TimeoutException(
                                string.Format("Waited for {0:#,#}ms for the query to return non stale result.",
                                              elapsed1.TotalMilliseconds));
                        }
                        Debug.WriteLine(
                            string.Format(
                                "Stale query results on non stable query '{0}' on index '{1}' in '{2}', query will be retried",
                                query, indexName, theSession.StoreIdentifier));


                        return TaskEx.Delay(100)
                            .ContinueWith(_ => GetQueryResultTaskResult(query, indexQuery, startTime))
                            .Unwrap();
                    }

                    Debug.WriteLine(string.Format("Query returned {0}/{1} results", task.Result.Results.Count,
                                                  task.Result.TotalResults));
					task.Result.EnsureSnapshot();
                    return task;
                }).Unwrap();
        }
#endif

#if !SILVERLIGHT
        /// <summary>
        ///   Gets the query result.
        /// </summary>
        /// <returns></returns>
        protected virtual QueryResult GetQueryResult()
        {
            foreach (var documentQueryListener in queryListeners)
            {
                documentQueryListener.BeforeQueryExecuted(this);
            }
            theSession.IncrementRequestCount();
            var sp = Stopwatch.StartNew();
            while (true)
            {
                var query = theQueryText.ToString();

                Debug.WriteLine(string.Format("Executing query '{0}' on index '{1}' in '{2}'",
                                              query, indexName, theSession.StoreIdentifier));

                var indexQuery = GenerateIndexQuery(query);

                AddOperationHeaders(theDatabaseCommands.OperationsHeaders.Add);

                var result = theDatabaseCommands.Query(indexName, indexQuery, includes.ToArray());
                if (theWaitForNonStaleResults && result.IsStale)
                {
                    if (sp.Elapsed > timeout)
                    {
                        sp.Stop();
                        throw new TimeoutException(
                            string.Format("Waited for {0:#,#}ms for the query to return non stale result.",
                                          sp.ElapsedMilliseconds));
                    }
                    Debug.WriteLine(
                        string.Format(
                            "Stale query results on non stable query '{0}' on index '{1}' in '{2}', query will be retried",
                            query, indexName, theSession.StoreIdentifier));
                    Thread.Sleep(100);
                    continue;
                }
                Debug.WriteLine(string.Format("Query returned {0}/{1} results", result.Results.Count,
                                              result.TotalResults));
            	result.EnsureSnapshot();
                return result;
            }
        }
#endif

        private SortOptions FromPrimitiveTypestring(string type)
        {
            switch (type)
            {
                case "Int16":
                    return SortOptions.Short;
                case "Int32":
                    return SortOptions.Int;
                case "Int64":
                    return SortOptions.Long;
                case "Single":
                    return SortOptions.Float;
                case "String":
                    return SortOptions.String;
                default:
                    return SortOptions.String;
            }
        }


        /// <summary>
        ///   Generates the index query.
        /// </summary>
        /// <param name = "query">The query.</param>
        /// <returns></returns>
        protected virtual IndexQuery GenerateIndexQuery(string query)
        {
            return new IndexQuery
            {
                GroupBy = groupByFields,
                AggregationOperation = aggregationOp,
                Query = query,
                PageSize = pageSize ?? 128,
                Start = start,
                Cutoff = cutoff,
                SortedFields = orderByFields.Select(x => new SortedField(x)).ToArray(),
                FieldsToFetch = projectionFields
            };
        }

        private void AddOperationHeaders(Action<string, string> addOperationHeader)
        {
            foreach (var sortByHint in sortByHints)
            {
                if (sortByHint.Value == null)
                    continue;

                addOperationHeader(
                    string.Format("SortHint-{0}", Uri.EscapeDataString(sortByHint.Key.Trim('-'))),
                    FromPrimitiveTypestring(sortByHint.Value.Name).ToString());
            }
        }

        private T Deserialize(RavenJObject result)
        {
            var metadata = result.Value<RavenJObject>("@metadata");
            if (
				// we asked for a projection directly from the index
				projectionFields != null && projectionFields.Length > 0 
				// we got a document without an @id
                // we aren't querying a document, we are probably querying a map reduce index result or a projection
			   || metadata == null || string.IsNullOrEmpty(metadata.Value<string>("@id")))
			{  
                if (typeof(T) == typeof(RavenJObject))
                    return (T)(object)result;

#if !NET_3_5
                if (typeof(T) == typeof(object))
                {
                    return (T)(object)new DynamicJsonObject(result);
                }
#endif
                HandleInternalMetadata(result);

                var deserializedResult =
                    (T)theSession.Conventions.CreateSerializer().Deserialize(new RavenJTokenReader(result), typeof(T));

                var documentId = result.Value<string>(Constants.DocumentIdFieldName); //check if the result contain the reserved name
                if (string.IsNullOrEmpty(documentId) == false)
                {
                    // we need to make an addtional check, since it is possible that a value was explicitly stated
                    // for the identity property, in which case we don't want to override it.
                    var identityProperty = theSession.Conventions.GetIdentityProperty(typeof(T));
					if (identityProperty == null ||
						(result[identityProperty.Name] == null ||
							result[identityProperty.Name].Type == JTokenType.Null))
                    {
                        theSession.TrySetIdentity(deserializedResult, documentId);
                    }
                }

                return deserializedResult;
            }
            return theSession.TrackEntity<T>(metadata.Value<string>("@id"),
                                          result,
                                          metadata);
        }

        private void HandleInternalMetadata(RavenJObject result)
        {
			// Implant a property with "id" value ... if not exists
        	var metadata = result.Value<RavenJObject>("@metadata");
			if (metadata == null || string.IsNullOrEmpty(metadata.Value<string>("@id"))) 
        	{
				// if the item has metadata, then nested items will not have it, so we can skip recursing down
				foreach (var nested in result.Select(property => property.Value))
				{
					var jObject = nested as RavenJObject;
					if(jObject != null)
						HandleInternalMetadata(jObject);
					var jArray = nested as RavenJArray;
					if (jArray == null) 
						continue;
					foreach (var item in jArray.OfType<RavenJObject>())
					{
						HandleInternalMetadata(item);
					}
				}
				return;
        	}

			var entityName = metadata.Value<string>(Constants.RavenEntityName);

			var idPropName = theSession.Conventions.FindIdentityPropertyNameFromEntityName(entityName);
			if (result.ContainsKey(idPropName))
				return;

			result[idPropName] = new RavenJValue(metadata.Value<string>("@id"));
        }

    	private string TransformToEqualValue(WhereEqualsParams whereEqualsParams)
        {
            if (whereEqualsParams.Value == null)
            {
				return Constants.NullValueNotAnalyzed;
            }

            if (whereEqualsParams.Value is bool)
            {
                return (bool)whereEqualsParams.Value ? "true" : "false";
            }

            if (whereEqualsParams.Value is DateTime)
            {
                return DateTools.DateToString((DateTime)whereEqualsParams.Value, DateTools.Resolution.MILLISECOND);
            }
			
			if (whereEqualsParams.Value is DateTimeOffset)
			{
				return DateTools.DateToString(((DateTimeOffset)whereEqualsParams.Value).DateTime, DateTools.Resolution.MILLISECOND);
			}

			if(whereEqualsParams.FieldName == Constants.DocumentIdFieldName && whereEqualsParams.Value is string == false)
			{
				return theSession.Conventions.FindFullDocumentKeyFromNonStringIdentifier(whereEqualsParams.Value, typeof(T));
			}

    		var escaped = RavenQuery.Escape(Convert.ToString(whereEqualsParams.Value, CultureInfo.InvariantCulture),
                                            whereEqualsParams.AllowWildcards && whereEqualsParams.IsAnalyzed);

            if (whereEqualsParams.Value is string == false)
                return escaped;

            return whereEqualsParams.IsAnalyzed ? escaped : String.Concat("[[", escaped, "]]");
        }

        private static string TransformToRangeValue(object value)
        {
            if (value == null)
				return Constants.NullValueNotAnalyzed;

            if (value is int)
                return NumberUtil.NumberToString((int)value);
            if (value is long)
                return NumberUtil.NumberToString((long)value);
            if (value is decimal)
                return NumberUtil.NumberToString((double)(decimal)value);
            if (value is double)
                return NumberUtil.NumberToString((double)value);
            if (value is float)
                return NumberUtil.NumberToString((float)value);
            if (value is DateTime)
                return DateTools.DateToString((DateTime)value, DateTools.Resolution.MILLISECOND);
			if (value is DateTimeOffset)
				return DateTools.DateToString(((DateTimeOffset)value).DateTime, DateTools.Resolution.MILLISECOND);

            return RavenQuery.Escape(value.ToString(), false);
        }

        /// <summary>
        ///   Returns a <see cref = "System.String" /> that represents this instance.
        /// </summary>
        /// <returns>
        ///   A <see cref = "System.String" /> that represents this instance.
        /// </returns>
        public override string ToString()
        {
            if (currentClauseDepth != 0)
            {
                throw new InvalidOperationException(
                    string.Format("A clause was not closed correctly within this query, current clause depth = {0}",
                                  currentClauseDepth));
            }
            return theQueryText.ToString();
        }

        /// <summary>
        ///   The last term that we asked the query to use equals on
        /// </summary>
        public KeyValuePair<string, string> GetLastEqualityTerm()
        {
            return lastEquality;
        }
#if !NET_3_5
        /// <summary>
        /// Returns a list of results for a query asynchronously. 
        /// </summary>
        public Task<Tuple<QueryResult,IList<T>>> ToListAsync()
        {
            return QueryResultAsync
                .ContinueWith(r =>
                {
                    var result = r.Result;

                    foreach (var include in result.Includes)
                    {
                        var metadata = include.Value<RavenJObject>("@metadata");
                        theSession.TrackEntity<object>(metadata.Value<string>("@id"), include, metadata);
                    }

                	return Tuple.Create(r.Result, (IList<T>) result.Results.Select(Deserialize).ToList());
                });
        }

		/// <summary>
		/// Gets the total count of records for this query
		/// </summary>
		public Task<int> CountAsync()
		{
			Take(0);
			return QueryResultAsync
				.ContinueWith(r => r.Result.TotalResults);
		}
#endif
    }
}