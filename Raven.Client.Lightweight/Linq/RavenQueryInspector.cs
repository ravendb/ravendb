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
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Client.Connection.Async;

namespace Raven.Client.Linq
{
	/// <summary>
	/// Implements <see cref="IRavenQueryable{T}"/>
	/// </summary>
	public class RavenQueryInspector<T> : IRavenQueryable<T>, IRavenQueryInspector
	{
		private readonly Expression expression;
		private readonly IRavenQueryProvider provider;
		private readonly RavenQueryStatistics queryStats;
		private readonly string indexName;
#if !SILVERLIGHT
		private readonly IDatabaseCommands databaseCommands;
#endif
		private readonly IAsyncDatabaseCommands asyncDatabaseCommands;
		private InMemoryDocumentSessionOperations session;

		/// <summary>
		/// Initializes a new instance of the <see cref="RavenQueryInspector{T}"/> class.
		/// </summary>
		public RavenQueryInspector(
			IRavenQueryProvider provider, 
			RavenQueryStatistics queryStats,
			string indexName,
			Expression expression,
			InMemoryDocumentSessionOperations session
#if !SILVERLIGHT
			, IDatabaseCommands databaseCommands
#endif
			, IAsyncDatabaseCommands asyncDatabaseCommands
			)
		{
			if (provider == null)
			{
				throw new ArgumentNullException("provider");
			}
			this.provider = provider.For<T>();
			this.queryStats = queryStats;
			this.indexName = indexName;
			this.session = session;
#if !SILVERLIGHT
			this.databaseCommands = databaseCommands;
#endif
			this.asyncDatabaseCommands = asyncDatabaseCommands;
			this.provider.AfterQueryExecuted(queryStats.UpdateQueryStats);
			this.expression = expression ?? Expression.Constant(this);
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

		/// <summary>
		/// Returns a <see cref="System.String"/> that represents this instance.
		/// </summary>
		/// <returns>
		/// A <see cref="System.String"/> that represents this instance.
		/// </returns>
		public override string ToString()
		{
			RavenQueryProviderProcessor<T> ravenQueryProvider = GetRavenQueryProvider();
			var luceneQuery = ravenQueryProvider.GetLuceneQueryFor(expression);
			string fields = "";
			if (ravenQueryProvider.FieldsToFetch.Count > 0)
				fields = "<" + string.Join(", ", ravenQueryProvider.FieldsToFetch.ToArray()) + ">: ";
			return fields + luceneQuery;
		}

		/// <summary>
		/// Returns a <see cref="System.String"/> that represents this instance.
		/// </summary>
		/// <returns>
		/// A <see cref="System.String"/> that represents this instance.
		/// </returns>
		public string ToAsyncString()
		{
			RavenQueryProviderProcessor<T> ravenQueryProvider = GetRavenQueryProvider();
			var luceneQuery = ravenQueryProvider.GetAsyncLuceneQueryFor(expression);
			string fields = "";
			if(ravenQueryProvider.FieldsToFetch.Count > 0)
				fields = "<" + string.Join(", ", ravenQueryProvider.FieldsToFetch.ToArray()) + ">: ";
			return fields + luceneQuery;
		}

		private RavenQueryProviderProcessor<T> GetRavenQueryProvider()
		{
			return new RavenQueryProviderProcessor<T>(provider.QueryGenerator, provider.CustomizeQuery, null, indexName, new HashSet<string>(), new Dictionary<string, string>());
		}

		/// <summary>
		/// Get the name of the index being queried
		/// </summary>
		public string IndexQueried
		{
			get
			{
				var ravenQueryProvider = new RavenQueryProviderProcessor<T>(provider.QueryGenerator, null, null, indexName, new HashSet<string>(), new Dictionary<string, string>());
				var luceneQuery = ravenQueryProvider.GetLuceneQueryFor(expression);
				return ((IRavenQueryInspector)luceneQuery).IndexQueried;
			}
		}

		/// <summary>
		/// Get the name of the index being queried asynchronously
		/// </summary>
		public string AsyncIndexQueried
		{
			get
			{
				var ravenQueryProvider = new RavenQueryProviderProcessor<T>(provider.QueryGenerator, null, null, indexName, new HashSet<string>(), new Dictionary<string, string>());
				var luceneQuery = ravenQueryProvider.GetAsyncLuceneQueryFor(expression);
				return ((IRavenQueryInspector)luceneQuery).IndexQueried;
			}
		}

#if !SILVERLIGHT
		/// <summary>
		/// Grant access to the database commands
		/// </summary>
		public IDatabaseCommands DatabaseCommands
		{
			get
			{
				if(databaseCommands == null)
					throw new NotSupportedException("You cannot get database commands for this query");
				return databaseCommands;
			}
		}

		
#endif

		/// <summary>
		/// Grant access to the async database commands
		/// </summary>
		public IAsyncDatabaseCommands AsyncDatabaseCommands
		{
			get
			{
				if (asyncDatabaseCommands == null)
					throw new NotSupportedException("You cannot get database commands for this query");
				return asyncDatabaseCommands;
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
		public KeyValuePair<string, string> GetLastEqualityTerm()
		{
			var ravenQueryProvider = new RavenQueryProviderProcessor<T>(provider.QueryGenerator, null, null, indexName, new HashSet<string>(), new Dictionary<string, string>());
			var luceneQuery = ravenQueryProvider.GetLuceneQueryFor(expression);
			return ((IRavenQueryInspector)luceneQuery).GetLastEqualityTerm();
		}

#if SILVERLIGHT
		/// <summary>
		///   This function exists solely to forbid calling ToList() on a queryable in Silverlight.
		/// </summary>
		[Obsolete("You cannot execute a query synchronously from the Silverlight client. Instead, use queryable.ToListAsync().", true)]
		public static IList<TOther> ToList<TOther>()
		{
			throw new NotSupportedException();
		}

		/// <summary>
		///   This function exists solely to forbid calling ToArray() on a queryable in Silverlight.
		/// </summary>
		[Obsolete("You cannot execute a query synchronously from the Silverlight client. Instead, use queryable.ToListAsync().", true)]
		public static TOther[] ToArray<TOther>()
		{
			throw new NotSupportedException();
		}
#endif

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
