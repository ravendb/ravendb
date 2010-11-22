using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading;
using Newtonsoft.Json.Linq;
using Raven.Client.Client;
using Raven.Client.Exceptions;
using Raven.Client.Linq;
using Raven.Database.Data;
using Raven.Database.Indexing;
using Raven.Database.Extensions;

namespace Raven.Client.Document
{
    /// <summary>
    /// A query against a Raven index
    /// </summary>
    public class DocumentQuery<T> : IDocumentQuery<T>, IDocumentQueryCustomization, IRavenQueryInspector
    {
        private bool negate;
        private readonly IDatabaseCommands databaseCommands;
        private readonly string indexName;
        private int currentClauseDepth;

        private KeyValuePair<string, string> lastEquality;

        /// <summary>
        /// The list of fields to project directly from the index
        /// </summary>
        protected readonly string[] projectionFields;
        private readonly DocumentSession session;
        /// <summary>
        /// The cutoff date to use for detecting staleness in the index
        /// </summary>
        protected DateTime? cutoff;
        /// <summary>
        /// The fields to order the results by
        /// </summary>
        protected string[] orderByFields = new string[0];
        /// <summary>
        /// The types to sort the fields by (NULL if not specified)
        /// </summary>
        protected Type[] orderByTypes = new Type[0];
        /// <summary>
        /// The page size to use when querying the index
        /// </summary>
        protected int pageSize = 128;
        private QueryResult queryResult;
        private StringBuilder queryText = new StringBuilder();
        /// <summary>
        /// which record to start reading from 
        /// </summary>
        protected int start;
        private TimeSpan timeout;
        private bool waitForNonStaleResults;
        private readonly HashSet<string> includes = new HashSet<string>();

        /// <summary>
        /// Gets the current includes on this query
        /// </summary>
        public IEnumerable<String> Includes
        {
            get { return includes; }
        }

        /// <summary>
        /// Gets the database commands associated with this document query
        /// </summary>
        public IDatabaseCommands Commands
        {
            get { return databaseCommands; }
        }

        /// <summary>
        /// Get the name of the index being queried
        /// </summary>
        public string IndexQueried
        {
            get { return indexName; }
        }

        /// <summary>
        /// Gets the session associated with this document query
        /// </summary>
        public IDocumentSession Session
        {
            get { return this.session; }
        }

        /// <summary>
        /// Gets the query text built so far
        /// </summary>
        protected StringBuilder QueryText
        {
            get { return this.queryText; }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DocumentQuery&lt;T&gt;"/> class.
        /// </summary>
        /// <param name="session">The session.</param>
        /// <param name="databaseCommands">The database commands.</param>
        /// <param name="indexName">Name of the index.</param>
        /// <param name="projectionFields">The projection fields.</param>
        public DocumentQuery(DocumentSession session, IDatabaseCommands databaseCommands, string indexName,
                             string[] projectionFields)
        {
            this.databaseCommands = databaseCommands;
            this.projectionFields = projectionFields;
            this.indexName = indexName;
            this.session = session;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DocumentQuery&lt;T&gt;"/> class.
        /// </summary>
        /// <param name="other">The other.</param>
        protected DocumentQuery(DocumentQuery<T> other)
        {
            databaseCommands = other.databaseCommands;
            indexName = other.indexName;
            projectionFields = other.projectionFields;
            session = other.session;
            cutoff = other.cutoff;
            orderByFields = other.orderByFields;
            orderByTypes = other.orderByTypes;
            pageSize = other.pageSize;
            queryText = other.queryText;
            start = other.start;
            timeout = other.timeout;
            waitForNonStaleResults = other.waitForNonStaleResults;
            includes = other.includes;
        }

        #region IDocumentQuery<T> Members

        /// <summary>
        /// EXPERT ONLY: Instructs the query to wait for non stale results for the specified wait timeout.
        /// This shouldn't be used outside of unit tests unless you are well aware of the implications
        /// </summary>
        /// <param name="waitTimeout">The wait timeout.</param>
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
        public IDocumentQuery<TProjection> SelectFields<TProjection>(string[] fields)
        {
            return new DocumentQuery<TProjection>(session, databaseCommands, indexName, fields)
            {
                pageSize = pageSize,
                queryText = new StringBuilder(queryText.ToString()),
                start = start,
                timeout = timeout,
                cutoff = cutoff,
                waitForNonStaleResults = waitForNonStaleResults,
                orderByTypes = orderByTypes,
                orderByFields = orderByFields,
            };
        }

        /// <summary>
        /// Filter matches to be inside the specified radius
        /// </summary>
        /// <param name="radius">The radius.</param>
        /// <param name="latitude">The latitude.</param>
        /// <param name="longitude">The longitude.</param>
        IDocumentQueryCustomization IDocumentQueryCustomization.WithinRadiusOf(double radius, double latitude, double longitude)
        {
            WithinRadiusOf(radius, latitude, longitude);
            return this;
        }

        /// <summary>
        /// EXPERT ONLY: Instructs the query to wait for non stale results.
        /// This shouldn't be used outside of unit tests unless you are well aware of the implications
        /// </summary>
        IDocumentQueryCustomization IDocumentQueryCustomization.WaitForNonStaleResults()
        {
            WaitForNonStaleResults();
            return this;
        }

        /// <summary>
        /// Instruct the query to wait for non stale result for the specified wait timeout.
        /// </summary>
        /// <param name="waitTimeout">The wait timeout.</param>
        /// <returns></returns>
        public IDocumentQuery<T> WaitForNonStaleResults(TimeSpan waitTimeout)
        {
            waitForNonStaleResults = true;
            timeout = waitTimeout;
            return this;
        }

        /// <summary>
        /// Gets the query result
        /// Execute the query the first time that this is called.
        /// </summary>
        /// <value>The query result.</value>
        public QueryResult QueryResult
        {
            get { return queryResult ?? (queryResult = GetQueryResult()); }
        }

        /// <summary>
        /// Gets the fields for projection 
        /// </summary>
        /// <returns></returns>
        public IEnumerable<string> GetProjectionFields()
        {
            return projectionFields ?? Enumerable.Empty<string>();
        }

        /// <summary>
        /// Adds an ordering for a specific field to the query
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="descending">if set to <c>true</c> [descending].</param>
        public IDocumentQuery<T> AddOrder(string fieldName, bool descending)
        {
            return this.AddOrder(fieldName, descending, null);
        }

        /// <summary>
        /// Adds an ordering for a specific field to the query and specifies the type of field for sorting purposes
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="descending">if set to <c>true</c> [descending].</param>
        /// <param name="fieldType">the type of the field to be sorted.</param>
        public IDocumentQuery<T> AddOrder(string fieldName, bool descending, Type fieldType)
        {
            fieldName = descending ? "-" + fieldName : fieldName;
            orderByFields = orderByFields.Concat(new[] { fieldName }).ToArray();
            orderByTypes = orderByTypes.Concat(new[] { fieldType }).ToArray();
            return this;
        }

        /// <summary>
        /// Gets the enumerator.
        /// </summary>
        public IEnumerator<T> GetEnumerator()
        {
            var sp = Stopwatch.StartNew();
            do
            {
                try
                {
                    foreach (var include in QueryResult.Includes)
                    {
                        var metadata = include.Value<JObject>("@metadata");

                        session.TrackEntity<object>(metadata.Value<string>("@id"),
                                                    include,
                                                    metadata);
                    }
                    var list = QueryResult.Results
                        .Select(Deserialize)
                        .ToList();
                    return list.GetEnumerator();
                }
                catch (NonAuthoritiveInformationException)
                {
                    if (sp.Elapsed > session.NonAuthoritiveInformationTimeout)
                        throw;
                    queryResult = null;
                    // we explicitly do NOT want to consider retries for non authoritive information as 
                    // additional request counted against the session quota
                    session.DecrementRequestCount();
                }
            } while (true);
        }


        /// <summary>
        /// Returns an enumerator that iterates through a collection.
        /// </summary>
        /// <returns>
        /// An <see cref="T:System.Collections.IEnumerator"/> object that can be used to iterate through the collection.
        /// </returns>
        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        /// <summary>
        /// Includes the specified path in the query, loading the document specified in that path
        /// </summary>
        /// <param name="path">The path.</param>
        public IDocumentQuery<T> Include(string path)
        {
            includes.Add(path);
            return this;
        }

        /// <summary>
        /// This function exists solely to forbid in memory where clause on IDocumentQuery, because
        /// that is nearly always a mistake.
        /// </summary>
        [Obsolete(@"
You cannot issue an in memory filter - such as Where(x=>x.Name == ""Ayende"") - on IDocumentQuery. 
This is likely a bug, because this will execute the filter in memory, rather than in RavenDB.
Consider using session.Query<T>() instead of session.LuceneQuery<T>. The session.Query<T>() method fully supports Linq queries, while session.LuceneQuery<T>() is intended for lower level API access.
If you really want to do in memory filtering on the data returned from the query, you can use: session.LuceneQuery<T>().ToList().Where(x=>x.Name == ""Ayende"")
", true)]
        public IEnumerable<T> Where(Func<T, bool> predicate)
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// Includes the specified path in the query, loading the document specified in that path
        /// </summary>
        /// <param name="path">The path.</param>
        public IDocumentQuery<T> Include(Expression<Func<T, object>> path)
        {
            return Include(path.ToPropertyPath());
        }

        /// <summary>
        /// Negates the next operation
        /// </summary>
        public IDocumentQuery<T> Not
        {
            get
            {
                negate = true;
                return this;
            }
        }

        /// <summary>
        /// Takes the specified count.
        /// </summary>
        /// <param name="count">The count.</param>
        /// <returns></returns>
        public IDocumentQuery<T> Take(int count)
        {
            pageSize = count;
            return this;
        }

        /// <summary>
        /// Skips the specified count.
        /// </summary>
        /// <param name="count">The count.</param>
        /// <returns></returns>
        public IDocumentQuery<T> Skip(int count)
        {
            start = count;
            return this;
        }

        /// <summary>
        /// Filter the results from the index using the specified where clause.
        /// </summary>
        /// <param name="whereClause">The where clause.</param>
        public IDocumentQuery<T> Where(string whereClause)
        {
            if (queryText.Length > 0)
            {
                queryText.Append(" ");
            }

            queryText.Append(whereClause);
            return this;
        }

        /// <summary>
        /// 	Matches exact value
        /// </summary>
        /// <remarks>
        /// 	Defaults to NotAnalyzed
        /// </remarks>
        public IDocumentQuery<T> WhereEquals(string fieldName, object value)
        {
            return this.WhereEquals(fieldName, value, false, false);
        }

        /// <summary>
        /// 	Matches exact value
        /// </summary>
        /// <remarks>
        /// 	Defaults to allow wildcards only if analyzed
        /// </remarks>
        public IDocumentQuery<T> WhereEquals(string fieldName, object value, bool isAnalyzed)
        {
            return this.WhereEquals(fieldName, value, isAnalyzed, isAnalyzed);
        }


        /// <summary>
        /// Simplified method for opening a new clause within the query
        /// </summary>
        /// <returns></returns>
        public IDocumentQuery<T> OpenSubclause()
        {
            currentClauseDepth++;
            if (queryText.Length > 0 && queryText[queryText.Length - 1] != '(')
            {
                queryText.Append(" ");
            }
            this.queryText.Append("(");
            return this;
        }

        /// <summary>
        /// Simplified method for closing a clause within the query
        /// </summary>
        /// <returns></returns>
        public IDocumentQuery<T> CloseSubclause()
        {
            currentClauseDepth--;
            this.queryText.Append(")");
            return this;
        }

        /// <summary>
        /// 	Matches exact value
        /// </summary>
        public IDocumentQuery<T> WhereEquals(string fieldName, object value, bool isAnalyzed, bool allowWildcards)
        {
            var transformToEqualValue = TransformToEqualValue(value, isAnalyzed, allowWildcards);
            lastEquality = new KeyValuePair<string, string>(fieldName, transformToEqualValue);
            if (queryText.Length > 0 && queryText[queryText.Length - 1] != '(')
            {
                queryText.Append(" ");
            }

            NegateIfNeeded();

            queryText.Append(fieldName);
            queryText.Append(":");
            queryText.Append(transformToEqualValue);

            return this;
        }

        private void NegateIfNeeded()
        {
            if (negate == false)
                return;
            negate = false;
            queryText.Append("-");
        }

        /// <summary>
        /// 	Matches substrings of the field
        /// </summary>
        public IDocumentQuery<T> WhereContains(string fieldName, object value)
        {
            return this.WhereEquals(fieldName, value, true, true);
        }

        /// <summary>
        /// Matches fields which starts with the specified value.
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="value">The value.</param>
        public IDocumentQuery<T> WhereStartsWith(string fieldName, object value)
        {
            // NOTE: doesn't fully match StartsWith semantics
            return this.WhereEquals(fieldName, String.Concat(value, "*"), true, true);
        }

        /// <summary>
        /// Matches fields which ends with the specified value.
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="value">The value.</param>
        public IDocumentQuery<T> WhereEndsWith(string fieldName, object value)
        {
            // http://lucene.apache.org/java/2_4_0/queryparsersyntax.html#Wildcard%20Searches
            // You cannot use a * or ? symbol as the first character of a search

            // NOTE: doesn't fully match EndsWith semantics
            return this.WhereEquals(fieldName, String.Concat("*", value), true, true);
        }

        /// <summary>
        /// Matches fields where the value is between the specified start and end, exclusive
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="start">The start.</param>
        /// <param name="end">The end.</param>
        /// <returns></returns>
        public IDocumentQuery<T> WhereBetween(string fieldName, object start, object end)
        {
            if (queryText.Length > 0)
            {
                queryText.Append(" ");
            }

            NegateIfNeeded();

            queryText.Append(fieldName).Append(":{");
            queryText.Append(start == null ? "*" : TransformToRangeValue(start));
            queryText.Append(" TO ");
            queryText.Append(end == null ? "NULL" : TransformToRangeValue(end));
            queryText.Append("}");

            return this;
        }

        /// <summary>
        /// Matches fields where the value is between the specified start and end, inclusive
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="start">The start.</param>
        /// <param name="end">The end.</param>
        /// <returns></returns>
        public IDocumentQuery<T> WhereBetweenOrEqual(string fieldName, object start, object end)
        {
            if (queryText.Length > 0)
            {
                queryText.Append(" ");
            }

            NegateIfNeeded();

            queryText.Append(fieldName).Append(":[");
            queryText.Append(start == null ? "*" : TransformToRangeValue(start));
            queryText.Append(" TO ");
            queryText.Append(end == null ? "NULL" : TransformToRangeValue(end));
            queryText.Append("]");

            return this;
        }

        /// <summary>
        /// Matches fields where the value is greater than the specified value
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="value">The value.</param>
        public IDocumentQuery<T> WhereGreaterThan(string fieldName, object value)
        {
            return this.WhereBetween(fieldName, value, null);
        }

        /// <summary>
        /// Matches fields where the value is greater than or equal to the specified value
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="value">The value.</param>
        public IDocumentQuery<T> WhereGreaterThanOrEqual(string fieldName, object value)
        {
            return this.WhereBetweenOrEqual(fieldName, value, null);
        }

        /// <summary>
        /// Matches fields where the value is less than the specified value
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="value">The value.</param>
        public IDocumentQuery<T> WhereLessThan(string fieldName, object value)
        {
            return this.WhereBetween(fieldName, null, value);
        }

        /// <summary>
        /// Matches fields where the value is less than or equal to the specified value
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="value">The value.</param>
        public IDocumentQuery<T> WhereLessThanOrEqual(string fieldName, object value)
        {
            return this.WhereBetweenOrEqual(fieldName, null, value);
        }

        /// <summary>
        /// Add an AND to the query
        /// </summary>
        public IDocumentQuery<T> AndAlso()
        {
            if (this.queryText.Length < 1)
            {
                throw new InvalidOperationException("Missing where clause");
            }

            queryText.Append(" AND");
            return this;
        }

        /// <summary>
        /// Add an OR to the query
        /// </summary>
        public IDocumentQuery<T> OrElse()
        {
            if (this.queryText.Length < 1)
            {
                throw new InvalidOperationException("Missing where clause");
            }

            queryText.Append(" OR");
            return this;
        }

        /// <summary>
        /// 	Specifies a boost weight to the last where clause.
        /// 	The higher the boost factor, the more relevant the term will be.
        /// </summary>
        /// <param name = "boost">boosting factor where 1.0 is default, less than 1.0 is lower weight, greater than 1.0 is higher weight</param>
        /// <returns></returns>
        /// <remarks>
        /// 	http://lucene.apache.org/java/2_4_0/queryparsersyntax.html#Boosting%20a%20Term
        /// </remarks>
        public IDocumentQuery<T> Boost(decimal boost)
        {
            if (this.queryText.Length < 1)
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
                this.queryText.Append("^").Append(boost);
            }

            return this;
        }

        /// <summary>
        /// 	Specifies a fuzziness factor to the single word term in the last where clause
        /// </summary>
        /// <param name = "fuzzy">0.0 to 1.0 where 1.0 means closer match</param>
        /// <returns></returns>
        /// <remarks>
        /// 	http://lucene.apache.org/java/2_4_0/queryparsersyntax.html#Fuzzy%20Searches
        /// </remarks>
        public IDocumentQuery<T> Fuzzy(decimal fuzzy)
        {
            if (this.queryText.Length < 1)
            {
                throw new InvalidOperationException("Missing where clause");
            }

            if (fuzzy < 0m || fuzzy > 1m)
            {
                throw new ArgumentOutOfRangeException("Fuzzy distance must be between 0.0 and 1.0");
            }

            var ch = this.queryText[this.queryText.Length - 1];
            if (ch == '"' || ch == ']')
            {
                // this check is overly simplistic
                throw new InvalidOperationException("Fuzzy factor can only modify single word terms");
            }

            this.queryText.Append("~");
            if (fuzzy != 0.5m)
            {
                // 0.5 is the default
                this.queryText.Append(fuzzy);
            }

            return this;
        }

        /// <summary>
        /// 	Specifies a proximity distance for the phrase in the last where clause
        /// </summary>
        /// <param name = "proximity">number of words within</param>
        /// <returns></returns>
        /// <remarks>
        /// 	http://lucene.apache.org/java/2_4_0/queryparsersyntax.html#Proximity%20Searches
        /// </remarks>
        public IDocumentQuery<T> Proximity(int proximity)
        {
            if (queryText.Length < 1)
            {
                throw new InvalidOperationException("Missing where clause");
            }

            if (proximity < 1)
            {
                throw new ArgumentOutOfRangeException("proximity", "Proximity distance must be positive number");
            }

            if (queryText[queryText.Length - 1] != '"')
            {
                // this check is overly simplistic
                throw new InvalidOperationException("Proximity distance can only modify a phrase");
            }

            queryText.Append("~").Append(proximity);

            return this;
        }

        /// <summary>
        /// Filter matches to be inside the specified radius
        /// </summary>
        /// <param name="radius">The radius.</param>
        /// <param name="latitude">The latitude.</param>
        /// <param name="longitude">The longitude.</param>
        public IDocumentQuery<T> WithinRadiusOf(double radius, double latitude, double longitude)
        {
            IDocumentQuery<T> spatialDocumentQuery = new SpatialDocumentQuery<T>(this, radius, latitude, longitude);
            if (negate)
            {
                negate = false;
                spatialDocumentQuery = spatialDocumentQuery.Not;
            }
            return spatialDocumentQuery.Not;
        }

        /// <summary>
        /// Sorts the query results by distance.
        /// </summary>
        public IDocumentQuery<T> SortByDistance()
        {
            return new SpatialDocumentQuery<T>(this, true);
        }

        /// <summary>
        /// Order the results by the specified fields
        /// </summary>
        /// <remarks>
        /// The fields are the names of the fields to sort, defaulting to sorting by ascending.
        /// You can prefix a field name with '-' to indicate sorting by descending or '+' to sort by ascending
        /// </remarks>
        /// <param name="fields">The fields.</param>
        public IDocumentQuery<T> OrderBy(params string[] fields)
        {
            orderByFields = orderByFields.Concat(fields).ToArray();
            orderByTypes = new Type[orderByFields.Length];
            return this;
        }

        /// <summary>
        /// Instructs the query to wait for non stale results as of now.
        /// </summary>
        /// <returns></returns>
        public IDocumentQuery<T> WaitForNonStaleResultsAsOfNow()
        {
            waitForNonStaleResults = true;
            cutoff = DateTime.UtcNow;
            timeout = TimeSpan.FromSeconds(15);
            return this;
        }

        /// <summary>
        /// Instructs the query to wait for non stale results as of now for the specified timeout.
        /// </summary>
        /// <param name="waitTimeout">The wait timeout.</param>
        /// <returns></returns>
        IDocumentQueryCustomization IDocumentQueryCustomization.WaitForNonStaleResultsAsOfNow(TimeSpan waitTimeout)
        {
            WaitForNonStaleResultsAsOfNow(waitTimeout);
            return this;
        }

        /// <summary>
        /// Instructs the query to wait for non stale results as of the cutoff date.
        /// </summary>
        /// <param name="cutOff">The cut off.</param>
        /// <returns></returns>
        IDocumentQueryCustomization IDocumentQueryCustomization.WaitForNonStaleResultsAsOf(DateTime cutOff)
        {
            WaitForNonStaleResultsAsOf(cutOff);
            return this;
        }

        /// <summary>
        /// Instructs the query to wait for non stale results as of the cutoff date for the specified timeout
        /// </summary>
        /// <param name="cutOff">The cut off.</param>
        /// <param name="waitTimeout">The wait timeout.</param>
        IDocumentQueryCustomization IDocumentQueryCustomization.WaitForNonStaleResultsAsOf(DateTime cutOff, TimeSpan waitTimeout)
        {
            WaitForNonStaleResultsAsOf(cutOff, waitTimeout);
            return this;
        }

        /// <summary>
        /// Instructs the query to wait for non stale results as of now.
        /// </summary>
        /// <returns></returns>
        IDocumentQueryCustomization IDocumentQueryCustomization.WaitForNonStaleResultsAsOfNow()
        {
            WaitForNonStaleResultsAsOfNow();
            return this;
        }

        /// <summary>
        /// Instructs the query to wait for non stale results as of now for the specified timeout.
        /// </summary>
        /// <param name="waitTimeout">The wait timeout.</param>
        /// <returns></returns>
        public IDocumentQuery<T> WaitForNonStaleResultsAsOfNow(TimeSpan waitTimeout)
        {
            waitForNonStaleResults = true;
            cutoff = DateTime.UtcNow;
            timeout = waitTimeout;
            return this;
        }

        /// <summary>
        /// Instructs the query to wait for non stale results as of the cutoff date.
        /// </summary>
        /// <param name="cutOff">The cut off.</param>
        /// <returns></returns>
        public IDocumentQuery<T> WaitForNonStaleResultsAsOf(DateTime cutOff)
        {
            waitForNonStaleResults = true;
            this.cutoff = cutOff.ToUniversalTime();
            return this;
        }

        /// <summary>
        /// Instructs the query to wait for non stale results as of the cutoff date for the specified timeout
        /// </summary>
        /// <param name="cutOff">The cut off.</param>
        /// <param name="waitTimeout">The wait timeout.</param>
        public IDocumentQuery<T> WaitForNonStaleResultsAsOf(DateTime cutOff, TimeSpan waitTimeout)
        {
            waitForNonStaleResults = true;
            this.cutoff = cutOff.ToUniversalTime();
            timeout = waitTimeout;
            return this;
        }

        /// <summary>
        /// EXPERT ONLY: Instructs the query to wait for non stale results.
        /// This shouldn't be used outside of unit tests unless you are well aware of the implications
        /// </summary>
        public IDocumentQuery<T> WaitForNonStaleResults()
        {
            waitForNonStaleResults = true;
            timeout = TimeSpan.FromSeconds(15);
            return this;
        }

        #endregion

        /// <summary>
        /// Gets the query result.
        /// </summary>
        /// <returns></returns>
        protected virtual QueryResult GetQueryResult()
        {
            session.IncrementRequestCount();
            var sp = Stopwatch.StartNew();
            while (true)
            {
                var query = queryText.ToString();

                Trace.WriteLine(string.Format("Executing query '{0}' on index '{1}' in '{2}'",
                                              query, indexName, session.StoreIdentifier));

                IndexQuery indexQuery = GenerateIndexQuery(query);

                for (int x = 0; x < this.orderByFields.Length; x++)
                {
                    String field = this.orderByFields[x];
                    Type fieldType = this.orderByTypes[x];
                    if (fieldType == null) { continue; }

                    databaseCommands.OperationsHeaders.Add(
                        string.Format("SortHint_{0}", field.Trim('-')), FromPrimitiveTypestring(fieldType.Name).ToString());
                }


                var result = databaseCommands.Query(indexName, indexQuery, includes.ToArray());
                if (waitForNonStaleResults && result.IsStale)
                {
                    if (sp.Elapsed > timeout)
                    {
                        sp.Stop();
                        throw new TimeoutException(string.Format("Waited for {0:#,#}ms for the query to return non stale result.",
                                                                 sp.ElapsedMilliseconds));
                    }
                    Trace.WriteLine(
                        string.Format("Stale query results on non stable query '{0}' on index '{1}' in '{2}', query will be retried",
                                      query, indexName, session.StoreIdentifier));
                    Thread.Sleep(100);
                    continue;
                }
                Trace.WriteLine(string.Format("Query returned {0}/{1} results", result.Results.Count, result.TotalResults));
                return result;
            }
        }

        private SortOptions FromPrimitiveTypestring(string type)
        {
            switch (type)
            {
                case "Int16": return SortOptions.Short;
                case "Int32": return SortOptions.Int;
                case "Int64": return SortOptions.Long;
                case "Single": return SortOptions.Float;
                case "String": return SortOptions.String;
                default: return SortOptions.String;
            }
        }


        /// <summary>
        /// Generates the index query.
        /// </summary>
        /// <param name="query">The query.</param>
        /// <returns></returns>
        protected virtual IndexQuery GenerateIndexQuery(string query)
        {
            return new IndexQuery
            {
                Query = query,
                PageSize = pageSize,
                Start = start,
                Cutoff = cutoff,
                SortedFields = orderByFields.Select(x => new SortedField(x)).ToArray(),
                FieldsToFetch = projectionFields
            };
        }

        private T Deserialize(JObject result)
        {
            var metadata = result.Value<JObject>("@metadata");
            if (projectionFields != null && projectionFields.Length > 0 // we asked for a projection directly from the index
                || metadata == null) // we aren't querying a document, we are probably querying a map reduce index result
            {
                var deserializedResult = (T)session.Conventions.CreateSerializer().Deserialize(new JTokenReader(result), typeof(T));
                var documentId = result.Value<string>("__document_id");//check if the result contain the reserved name
                if (string.IsNullOrEmpty(documentId) == false)
                {
                    // we need to make an addtional check, since it is possible that a value was explicitly stated
                    // for the identity property, in which case we don't want to override it.
                    var identityProperty = session.Conventions.GetIdentityProperty(typeof(T));
                    if (identityProperty == null || 
                        (result.Property(identityProperty.Name) == null ||
                        result.Property(identityProperty.Name).Value.Type == JTokenType.Null))
                    {
                        session.TrySetIdentity(deserializedResult, documentId);
                    }
                }

                return deserializedResult;
            }
            return session.TrackEntity<T>(metadata.Value<string>("@id"),
                                          result,
                                          metadata);
        }

        private static string TransformToEqualValue(object value, bool isAnalyzed, bool allowWildcards)
        {
            if (value == null)
            {
                return "[[NULL_VALUE]]";
            }

            if (value is bool)
            {
                return (bool)value ? "true" : "false";
            }

            if (value is DateTime)
            {
                return DateTools.DateToString((DateTime)value, DateTools.Resolution.MILLISECOND);
            }

            var escaped = RavenQuery.Escape(Convert.ToString(value, CultureInfo.InvariantCulture), allowWildcards && isAnalyzed);

            if (value is string == false)
                return escaped;

            return isAnalyzed ? escaped : String.Concat("[[", escaped, "]]");
        }

        private static string TransformToRangeValue(object value)
        {
            if (value == null)
                return "[[NULL_VALUE]]";

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

            return RavenQuery.Escape(value.ToString(), false);
        }

        /// <summary>
        /// Returns a <see cref="System.String"/> that represents this instance.
        /// </summary>
        /// <returns>
        /// A <see cref="System.String"/> that represents this instance.
        /// </returns>
        public override string ToString()
        {
            if (currentClauseDepth != 0)
            {
                throw new InvalidOperationException(string.Format("A clause was not closed correctly within this query, current clause depth = {0}", currentClauseDepth));
            }
            return this.queryText.ToString();
        }

        /// <summary>
        /// The last term that we asked the query to use equals on
        /// </summary>
        public KeyValuePair<string, string> GetLastEqualityTerm()
        {
            return lastEquality;
        }
    }
}
