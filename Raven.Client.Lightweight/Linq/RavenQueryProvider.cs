//-----------------------------------------------------------------------
// <copyright file="RavenQueryProvider.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Raven.Abstractions.Data;
#if !NET35
using Raven.Client.Connection.Async;
#endif
#if !Silverlight
using Raven.Client.Connection;
using Raven.Client.Document;

#endif

namespace Raven.Client.Linq
{
	/// <summary>
	/// An implementation of <see cref="IRavenQueryProvider"/>
	/// </summary>
	public class RavenQueryProvider<T> : IRavenQueryProvider
	{
		private Action<QueryResult> afterQueryExecuted;
		private Action<IDocumentQueryCustomization> customizeQuery;
		private readonly string indexName;
		private readonly IDocumentQueryGenerator queryGenerator;
		private readonly RavenQueryStatistics ravenQueryStatistics;
#if !SILVERLIGHT
		private readonly IDatabaseCommands databaseCommands;
#endif
#if !NET35
		private readonly IAsyncDatabaseCommands asyncDatabaseCommands;
#endif

		/// <summary>
		/// Initializes a new instance of the <see cref="RavenQueryProvider{T}"/> class.
		/// </summary>
		public RavenQueryProvider(
			IDocumentQueryGenerator queryGenerator,
			string indexName,
			RavenQueryStatistics ravenQueryStatistics
#if !SILVERLIGHT
, IDatabaseCommands databaseCommands
#endif
#if !NET35
, IAsyncDatabaseCommands asyncDatabaseCommands
#endif
)
		{
			FieldsToFetch = new HashSet<string>();
			FieldsToRename = new Dictionary<string, string>();

			this.queryGenerator = queryGenerator;
			this.indexName = indexName;
			this.ravenQueryStatistics = ravenQueryStatistics;
#if !SILVERLIGHT
			this.databaseCommands = databaseCommands;
#endif
#if !NET35
			this.asyncDatabaseCommands = asyncDatabaseCommands;
#endif
		}

		/// <summary>
		/// Gets the actions for customizing the generated lucene query
		/// </summary>
		public Action<IDocumentQueryCustomization> CustomizedQuery
		{
			get { return customizeQuery; }
		}

		/// <summary>
		/// Gets the name of the index.
		/// </summary>
		/// <value>The name of the index.</value>
		public string IndexName
		{
			get { return indexName; }
		}

		/// <summary>
		/// Get the query generator
		/// </summary>
		public IDocumentQueryGenerator QueryGenerator
		{
			get { return queryGenerator; }
		}

		public Action<IDocumentQueryCustomization> CustomizeQuery
		{
			get { return customizeQuery; }
		}

		/// <summary>
		/// Set the fields to fetch
		/// </summary>
		public HashSet<string> FieldsToFetch { get; private set; }

		/// <summary>
		/// Set the fields to rename
		/// </summary>
		public Dictionary<string, string> FieldsToRename { get; private set; }

		/// <summary>
		/// Change the result type for the query provider
		/// </summary>
		public IRavenQueryProvider For<S>()
		{
			if (typeof(T) == typeof(S))
				return this;

			var ravenQueryProvider = new RavenQueryProvider<S>(queryGenerator, indexName, ravenQueryStatistics
#if !SILVERLIGHT
				, databaseCommands
#endif
#if !NET35
				, asyncDatabaseCommands
#endif
			);
			ravenQueryProvider.Customize(customizeQuery);
			return ravenQueryProvider;
		}

		/// <summary>
		/// Executes the query represented by a specified expression tree.
		/// </summary>
		/// <param name="expression">An expression tree that represents a LINQ query.</param>
		/// <returns>
		/// The value that results from executing the specified query.
		/// </returns>
		public virtual object Execute(Expression expression)
		{
			return GetQueryProviderProcessor<T>().Execute(expression);
		}

		IQueryable<S> IQueryProvider.CreateQuery<S>(Expression expression)
		{
			return new RavenQueryInspector<S>(this, ravenQueryStatistics, indexName, expression, (InMemoryDocumentSessionOperations)queryGenerator
#if !SILVERLIGHT
				, databaseCommands
#endif
#if !NET35
				, asyncDatabaseCommands
#endif
			);
		}

		IQueryable IQueryProvider.CreateQuery(Expression expression)
		{
			Type elementType = TypeSystem.GetElementType(expression.Type);
			try
			{
				var makeGenericType = typeof(RavenQueryInspector<>).MakeGenericType(elementType);
				var args = new object[] { this, ravenQueryStatistics, indexName, expression, queryGenerator
#if !SILVERLIGHT
				                                      ,databaseCommands
#endif
#if !NET35
				                                      ,asyncDatabaseCommands
#endif
				                                    };
				return (IQueryable) Activator.CreateInstance(makeGenericType, args);
			}
			catch (TargetInvocationException tie)
			{
				throw tie.InnerException;
			}
		}

		/// <summary>
		/// Executes the specified expression.
		/// </summary>
		/// <typeparam name="S"></typeparam>
		/// <param name="expression">The expression.</param>
		/// <returns></returns>
		S IQueryProvider.Execute<S>(Expression expression)
		{
			return (S)Execute(expression);
		}

		/// <summary>
		/// Executes the query represented by a specified expression tree.
		/// </summary>
		/// <param name="expression">An expression tree that represents a LINQ query.</param>
		/// <returns>
		/// The value that results from executing the specified query.
		/// </returns>
		object IQueryProvider.Execute(Expression expression)
		{
			return Execute(expression);
		}

		/// <summary>
		/// Callback to get the results of the query
		/// </summary>
		public void AfterQueryExecuted(Action<QueryResult> afterQueryExecutedCallback)
		{
			this.afterQueryExecuted = afterQueryExecutedCallback;
		}

		/// <summary>
		/// Customizes the query using the specified action
		/// </summary>
		/// <param name="action">The action.</param>
		public virtual void Customize(Action<IDocumentQueryCustomization> action)
		{
			if (action == null)
				return;
			customizeQuery += action;
		}

#if !NET35

		/// <summary>
		/// Move the registered after query actions
		/// </summary>
		public void MoveAfterQueryExecuted<K>(IAsyncDocumentQuery<K> documentQuery)
		{
			if (afterQueryExecuted != null)
				documentQuery.AfterQueryExecuted(afterQueryExecuted);
		}

		/// <summary>
		/// Convert the expression to a Lucene query
		/// </summary>
		public IAsyncDocumentQuery<TResult> ToAsyncLuceneQuery<TResult>(Expression expression)
		{
			var processor = GetQueryProviderProcessor<T>();
			return (IAsyncDocumentQuery<TResult>)processor.GetAsyncLuceneQueryFor(expression);
		}

		/// <summary>
		/// Register the query as a lazy query in the session and return a lazy
		/// instance that will evaluate the query only when needed
		/// </summary>
		public Lazy<IEnumerable<S>> Lazily<S>(Expression expression, Action<IEnumerable<S>> onEval )
		{
			var processor = GetQueryProviderProcessor<S>();
			var query = processor.GetLuceneQueryFor(expression);
			if (afterQueryExecuted != null)
				query.AfterQueryExecuted(afterQueryExecuted);
		
			if (FieldsToFetch.Count > 0)
				query = query.SelectFields<S>(FieldsToFetch.ToArray());
			return query.Lazily(onEval);
		}
#endif

		protected virtual RavenQueryProviderProcessor<S> GetQueryProviderProcessor<S>()
		{
			return new RavenQueryProviderProcessor<S>(queryGenerator, customizeQuery, afterQueryExecuted, indexName,
				FieldsToFetch, 
				FieldsToRename);
		}

		/// <summary>
		/// Convert the expression to a Lucene query
		/// </summary>
		public IDocumentQuery<TResult> ToLuceneQuery<TResult>(Expression expression)
		{
			var processor = GetQueryProviderProcessor<T>();
			return (IDocumentQuery<TResult>)processor.GetLuceneQueryFor(expression);
		}
	}
}