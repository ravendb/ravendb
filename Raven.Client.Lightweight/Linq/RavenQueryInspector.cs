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
using Raven.Client.Client;
#if !NET_3_5
using Raven.Client.Client.Async;
#endif
using Raven.Client.Document;
using Raven.Database.Data;

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
#if !SILVERLIGHT
		private readonly IDatabaseCommands databaseCommands;
#endif
#if !NET_3_5
		private readonly IAsyncDatabaseCommands asyncDatabaseCommands;
#endif

		/// <summary>
		/// Initializes a new instance of the <see cref="RavenQueryInspector{T}"/> class.
		/// </summary>
		public RavenQueryInspector(
			IRavenQueryProvider provider, 
			RavenQueryStatistics queryStats,
			Expression expression
#if !SILVERLIGHT
				,IDatabaseCommands databaseCommands
#endif
#if !NET_3_5
				,IAsyncDatabaseCommands asyncDatabaseCommands
#endif
			)
		{
			if (provider == null)
			{
				throw new ArgumentNullException("provider");
			}
			this.provider = provider;
			this.queryStats = queryStats;
#if !SILVERLIGHT
			this.databaseCommands = databaseCommands;
#endif
#if !NET_3_5
			this.asyncDatabaseCommands = asyncDatabaseCommands;
#endif
			this.provider.AfterQueryExecuted(UpdateQueryStats);
			this.expression = expression ?? Expression.Constant(this);
		}

		private void UpdateQueryStats(QueryResult obj)
		{
			queryStats.IsStale = obj.IsStale;
			queryStats.TotalResults = obj.TotalResults;
			queryStats.SkippedResults = obj.SkippedResults;
			queryStats.Timestamp = obj.IndexTimestamp;
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
			return ((IEnumerable<T>)provider.Execute(expression)).GetEnumerator();
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
			var ravenQueryProvider = new RavenQueryProviderProcessor<T>(provider.QueryGenerator, null, null);
			ravenQueryProvider.ProcessExpression(expression);
			string fields = "";
			if (ravenQueryProvider.FieldsToFetch.Count > 0)
				fields = "<" + string.Join(", ", ravenQueryProvider.FieldsToFetch.ToArray()) + ">: ";
			return
				fields +
				ravenQueryProvider.LuceneQuery;
		}

		/// <summary>
		/// Get the name of the index being queried
		/// </summary>
		public string IndexQueried
		{
			get
			{
				var ravenQueryProvider = new RavenQueryProviderProcessor<T>(provider.QueryGenerator, null, null);
				ravenQueryProvider.ProcessExpression(expression);
				return ((IRavenQueryInspector)ravenQueryProvider.LuceneQuery).IndexQueried;
			}
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

		///<summary>
		///</summary>
		public KeyValuePair<string, string> GetLastEqualityTerm()
		{
			var ravenQueryProvider = new RavenQueryProviderProcessor<T>(provider.QueryGenerator, null, null);
			ravenQueryProvider.ProcessExpression(expression);
			return ((IRavenQueryInspector)ravenQueryProvider.LuceneQuery).GetLastEqualityTerm();
		}
	}
}
