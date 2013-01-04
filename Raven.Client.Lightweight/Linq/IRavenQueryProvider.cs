//-----------------------------------------------------------------------
// <copyright file="IRavenQueryProvider.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Client.Document;

namespace Raven.Client.Linq
{
	using System.Linq.Expressions;

	/// <summary>
	/// Extension for the built-in <see cref="IQueryProvider"/> allowing for Raven specific operations
	/// </summary>
	public interface IRavenQueryProvider : IQueryProvider
	{
		/// <summary>
		/// Callback to get the results of the query
		/// </summary>
		void AfterQueryExecuted(Action<QueryResult> afterQueryExecuted);

		/// <summary>
		/// Customizes the query using the specified action
		/// </summary>
		void Customize(Action<IDocumentQueryCustomization> action);

		/// <summary>
		/// Gets the name of the index.
		/// </summary>
		/// <value>The name of the index.</value>
		string IndexName { get; }

		/// <summary>
		/// Get the query generator
		/// </summary>
		IDocumentQueryGenerator QueryGenerator { get; }

		/// <summary>
		/// The action to execute on the customize query
		/// </summary>
		Action<IDocumentQueryCustomization> CustomizeQuery { get; }

		/// <summary>
		/// Change the result type for the query provider
		/// </summary>
		IRavenQueryProvider For<S>();

		/// <summary>
		/// Convert the Linq query to a Lucene query
		/// </summary>
		/// <returns></returns>
		IAsyncDocumentQuery<T> ToAsyncLuceneQuery<T>(Expression expression);

		/// <summary>
		/// Convert the Linq query to a lazy Lucene query and provide a function to execute when it is being evaluate
		/// </summary>
		Lazy<IEnumerable<T>> Lazily<T>(Expression expression, Action<IEnumerable<T>> onEval);


		/// <summary>
		/// Move the registered after query actions
		/// </summary>
		void MoveAfterQueryExecuted<T>(IAsyncDocumentQuery<T> documentQuery);

		/// <summary>
		/// Set the fields to fetch
		/// </summary>
		HashSet<string> FieldsToFetch { get; }

	}
}
