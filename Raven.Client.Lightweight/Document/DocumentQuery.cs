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
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using Raven.Client.Client;
#if !NET_3_5
using System.Threading.Tasks;
using Raven.Client.Client.Async;
#endif
using Newtonsoft.Json.Linq;
using Raven.Abstractions.Data;
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
#if !SILVERLIGHT
		private readonly IDatabaseCommands databaseCommands;
#endif
#if !NET_3_5
		private readonly IAsyncDatabaseCommands asyncDatabaseCommands;
#endif
		private readonly string indexName;
		private int currentClauseDepth;

		private KeyValuePair<string, string> lastEquality;

		/// <summary>
		/// The list of fields to project directly from the index
		/// </summary>
		protected readonly string[] projectionFields;

		private readonly IDocumentQueryListener[] queryListeners;
		private readonly InMemoryDocumentSessionOperations session;
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
		protected HashSet<KeyValuePair<string, Type>> sortByHints = new HashSet<KeyValuePair<string, Type>>();

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
		private AggregationOperation aggregationOp;
		private string[] groupByFields;
#if !NET_3_5
		private Task<QueryResult> queryResultTask;
#endif

		/// <summary>
		/// Gets the current includes on this query
		/// </summary>
		public IEnumerable<String> Includes
		{
			get { return includes; }
		}

		/// <summary>
		/// Get the name of the index being queried
		/// </summary>
		public string IndexQueried
		{
			get { return indexName; }
		}

#if !SILVERLIGHT
		/// <summary>
		/// Grant access to the database commands
		/// </summary>
		public IDatabaseCommands DatabaseCommands
		{
			get { return databaseCommands; }
		}
#endif

#if !NET_3_5
		/// <summary>
		/// Grant access to the async database commands
		/// </summary>
		public IAsyncDatabaseCommands AsyncDatabaseCommands
		{
			get { return asyncDatabaseCommands; }
		}
#endif

#if !SILVERLIGHT
		/// <summary>
		/// Gets the session associated with this document query
		/// </summary>
		public IDocumentSession Session
		{
			get { return (IDocumentSession)this.session; }
		}
#endif

		/// <summary>
		/// Gets the query text built so far
		/// </summary>
		protected StringBuilder QueryText
		{
			get { return this.queryText; }
		}


#if !SILVERLIGHT && !NET_3_5
		/// <summary>
		/// Initializes a new instance of the <see cref="DocumentQuery&lt;T&gt;"/> class.
		/// </summary>
		/// <param name="session">The session.</param>
		/// <param name="databaseCommands">The database commands.</param>
		/// <param name="indexName">Name of the index.</param>
		/// <param name="projectionFields">The projection fields.</param>
		public DocumentQuery(InMemoryDocumentSessionOperations session, 
			IDatabaseCommands databaseCommands, 
			string indexName,
			string[] projectionFields,
			IDocumentQueryListener[] queryListeners)
			:this(session,databaseCommands, null, indexName, projectionFields, queryListeners)
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

		/// <param name="indexName">Name of the index.</param>
		/// <param name="projectionFields">The projection fields.</param>
		/// <param name="session">The session.</param>
		public DocumentQuery(InMemoryDocumentSessionOperations session, 
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
			this.databaseCommands = databaseCommands;
#endif
			this.projectionFields = projectionFields;
			this.queryListeners = queryListeners;
			this.indexName = indexName;
			this.session = session;
#if !NET_3_5
			this.asyncDatabaseCommands = asyncDatabaseCommands;
#endif
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="DocumentQuery&lt;T&gt;"/> class.
		/// </summary>
		/// <param name="other">The other.</param>
		protected DocumentQuery(DocumentQuery<T> other)
		{
#if !SILVERLIGHT
			databaseCommands = other.databaseCommands;
#endif
#if !NET_3_5
			asyncDatabaseCommands = other.asyncDatabaseCommands;
#endif
			indexName = other.indexName;
			projectionFields = other.projectionFields;
			session = other.session;
			cutoff = other.cutoff;
			orderByFields = other.orderByFields;
			sortByHints = other.sortByHints;
			pageSize = other.pageSize;
			queryText = other.queryText;
			start = other.start;
			timeout = other.timeout;
			waitForNonStaleResults = other.waitForNonStaleResults;
			includes = other.includes;
			queryListeners = other.queryListeners;
		}

		#region IDocumentQuery<T> Members

		/// <summary>
		/// Includes the specified path in the query, loading the document specified in that path
		/// </summary>
		/// <param name="path">The path.</param>
		IDocumentQueryCustomization IDocumentQueryCustomization.Include(string path)
		{
			Include(path);
			return this;
		}

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
			return new DocumentQuery<TProjection>(session, 
#if !SILVERLIGHT
				databaseCommands,
#endif
#if !NET_3_5
				asyncDatabaseCommands,
#endif
				indexName, fields,
				queryListeners)
			{
				pageSize = pageSize,
				queryText = new StringBuilder(queryText.ToString()),
				start = start,
				timeout = timeout,
				cutoff = cutoff,
				waitForNonStaleResults = waitForNonStaleResults,
				sortByHints = sortByHints,
				orderByFields = orderByFields,
				groupByFields = groupByFields,
				aggregationOp = aggregationOp
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
		/// Includes the specified path in the query, loading the document specified in that path
		/// </summary>
		/// <param name="path">The path.</param>
		IDocumentQueryCustomization IDocumentQueryCustomization.Include<TResult>(Expression<Func<TResult, object>> path)
		{
			Include(path.ToPropertyPath());
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

#if !SILVERLIGHT
		/// <summary>
		/// Gets the query result
		/// Execute the query the first time that this is called.
		/// </summary>
		/// <value>The query result.</value>
		public QueryResult QueryResult
		{
			get { return queryResult ?? (queryResult = GetQueryResult()); }
		}
#endif

#if !NET_3_5
		/// <summary>
		/// Gets the query result
		/// Execute the query the first time that this is called.
		/// </summary>
		/// <value>The query result.</value>
		public Task<QueryResult> QueryResultAsync
		{
			get { return queryResultTask ?? (queryResultTask = GetQueryResultAsync()); }
		}
#endif

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
			fieldName = EnsureValidFieldName(fieldName);
			fieldName = descending ? "-" + fieldName : fieldName;
			orderByFields = orderByFields.Concat(new[] { fieldName }).ToArray();
			sortByHints.Add(new KeyValuePair<string, Type>(fieldName, fieldType));
			return this;
		}

		/// <summary>
		/// Gets the enumerator.
		/// </summary>
		public IEnumerator<T> GetEnumerator()
		{
#if !SILVERLIGHT
			var sp = Stopwatch.StartNew();
#else
			var startTime = DateTime.Now;
#endif
			do
			{
				try
				{
#if !SILVERLIGHT
					queryResult = QueryResult;
#else
                    queryResult = QueryResultAsync.Result;
#endif
                    foreach (var include in queryResult.Includes)
					{
						var metadata = include.Value<JObject>("@metadata");

						session.TrackEntity<object>(metadata.Value<string>("@id"),
													include,
													metadata);
					}
                    var list = queryResult.Results
						.Select(Deserialize)
						.ToList();
					return list.GetEnumerator();
				}
				catch (NonAuthoritiveInformationException)
				{
#if !SILVERLIGHT
					if (sp.Elapsed > session.NonAuthoritiveInformationTimeout)
						throw;
#else
					if ((DateTime.Now - startTime) > session.NonAuthoritiveInformationTimeout)
						throw;

#endif
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
			return this.WhereEquals(fieldName, value, true, false);
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
		    NegateIfNeeded();
			this.queryText.Append("(");
			return this;
		}

		///<summary>
		/// Instruct the index to group by the specified fields using the specified aggregation operation
		///</summary>
		/// <remarks>
		/// This is only valid on dynamic indexes queries
		/// </remarks>
		public IDocumentQuery<T> GroupBy(AggregationOperation aggregationOperation, params string[] fieldsToGroupBy)
		{
			this.groupByFields = fieldsToGroupBy;
			this.aggregationOp = aggregationOperation;
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
			fieldName = EnsureValidFieldName(fieldName);
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

		private string EnsureValidFieldName(string fieldName)
		{
			if (session == null)
				return fieldName;
			if (session.Conventions == null)
				return fieldName;
			var identityProperty = session.Conventions.GetIdentityProperty(typeof(T));
			if(identityProperty != null && identityProperty.Name == fieldName)
			{
				fieldName = "__document_id";
			}
			return fieldName;
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

			if ((start ?? end) != null)
				sortByHints.Add(new KeyValuePair<string, Type>(fieldName, (start ?? end).GetType()));

			NegateIfNeeded();
			
			fieldName = EnsureValidFieldName(fieldName);
			
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
			if ((start ?? end) != null)
				sortByHints.Add(new KeyValuePair<string, Type>(fieldName, (start ?? end).GetType()));

			NegateIfNeeded();

			fieldName = EnsureValidFieldName(fieldName);
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

#if !NET_3_5
		private Task<QueryResult> GetQueryResultAsync()
		{
			session.IncrementRequestCount();
			var startTime = DateTime.Now;

			var query = queryText.ToString();

			Debug.WriteLine(string.Format("Executing query '{0}' on index '{1}' in '{2}'",
			                              query, indexName, session.StoreIdentifier));

			IndexQuery indexQuery = GenerateIndexQuery(query);

			AddOperationHeaders(asyncDatabaseCommands.OperationsHeaders.Add);

			return GetQueryResultTaskResult(query, indexQuery, startTime);
		}

		private Task<QueryResult> GetQueryResultTaskResult(string query, IndexQuery indexQuery, DateTime startTime)
		{
			return asyncDatabaseCommands.QueryAsync(indexName, indexQuery, includes.ToArray())
				.ContinueWith(task =>
				{
					if (waitForNonStaleResults && task.Result.IsStale)
					{
						TimeSpan elapsed1 = DateTime.Now - startTime;
						if (elapsed1 > timeout)
						{
							throw new TimeoutException(string.Format("Waited for {0:#,#}ms for the query to return non stale result.",
							                                         elapsed1.TotalMilliseconds));
						}
						Debug.WriteLine(
							string.Format("Stale query results on non stable query '{0}' on index '{1}' in '{2}', query will be retried",
							              query, indexName, session.StoreIdentifier));

						
						return TaskEx.Delay(100)
							.ContinueWith(_ => GetQueryResultTaskResult(query, indexQuery, startTime))
							.Unwrap();
					}

					Debug.WriteLine(string.Format("Query returned {0}/{1} results", task.Result.Results.Count, task.Result.TotalResults));
					return task;
				}).Unwrap();
		}
#endif

#if !SILVERLIGHT
		/// <summary>
		/// Gets the query result.
		/// </summary>
		/// <returns></returns>
		protected virtual QueryResult GetQueryResult()
		{
			foreach (var documentQueryListener in queryListeners)
			{
				documentQueryListener.BeforeQueryExecuted(this);
			}
			session.IncrementRequestCount();
			var sp = Stopwatch.StartNew();
			while (true)
			{
				var query = queryText.ToString();

				Debug.WriteLine(string.Format("Executing query '{0}' on index '{1}' in '{2}'",
											  query, indexName, session.StoreIdentifier));

				IndexQuery indexQuery = GenerateIndexQuery(query);

				AddOperationHeaders(databaseCommands.OperationsHeaders.Add);

				var result = databaseCommands.Query(indexName, indexQuery, includes.ToArray());
				if (waitForNonStaleResults && result.IsStale)
				{
					if (sp.Elapsed > timeout)
					{
						sp.Stop();
						throw new TimeoutException(string.Format("Waited for {0:#,#}ms for the query to return non stale result.",
																 sp.ElapsedMilliseconds));
					}
					Debug.WriteLine(
						string.Format("Stale query results on non stable query '{0}' on index '{1}' in '{2}', query will be retried",
									  query, indexName, session.StoreIdentifier));
					System.Threading.Thread.Sleep(100);
					continue;
				}
				Debug.WriteLine(string.Format("Query returned {0}/{1} results", result.Results.Count, result.TotalResults));
				return result;
			}
		}
#endif

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
				GroupBy = groupByFields,
				AggregationOperation = aggregationOp,
				Query = query,
				PageSize = pageSize,
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
					string.Format("SortHint_{0}", sortByHint.Key.Trim('-')), FromPrimitiveTypestring(sortByHint.Value.Name).ToString());
			}
		}

		private T Deserialize(JObject result)
		{
			var metadata = result.Value<JObject>("@metadata");
			if (projectionFields != null && projectionFields.Length > 0 // we asked for a projection directly from the index
				|| metadata == null) // we aren't querying a document, we are probably querying a map reduce index result
			{

				if (typeof(T) == typeof(JObject))
					return (T)(object)result;

#if !NET_3_5
				if (typeof(T) == typeof(object))
				{
					return (T)(object)new Database.Linq.DynamicJsonObject(result);
				}
#endif
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
