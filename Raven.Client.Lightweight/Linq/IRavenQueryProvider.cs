//-----------------------------------------------------------------------
// <copyright file="IRavenQueryProvider.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Json.Linq;

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
        /// The name of the transformer to use with this query
        /// </summary>
        /// <param name="transformerName"></param>
	    void TransformWith(string transformerName);

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
		[Obsolete("Use ToAsyncDocumentQuery instead.")]
		IAsyncDocumentQuery<T> ToAsyncLuceneQuery<T>(Expression expression);

        /// <summary>
        /// Convert the Linq query to a Lucene query
        /// </summary>
        IAsyncDocumentQuery<T> ToAsyncDocumentQuery<T>(Expression expression);

	    /// <summary>
	    /// Convert the linq query to a Lucene query
	    /// </summary>
        [Obsolete("Use ToDocumentQuery instead.")]
	    IDocumentQuery<TResult> ToLuceneQuery<TResult>(Expression expression);

        /// <summary>
        /// Convert the linq query to a Lucene query
        /// </summary>
	    IDocumentQuery<TResult> ToDocumentQuery<TResult>(Expression expression);

		/// <summary>
		/// Convert the Linq query to a lazy Lucene query and provide a function to execute when it is being evaluated
		/// </summary>
		Lazy<IEnumerable<T>> Lazily<T>(Expression expression, Action<IEnumerable<T>> onEval);
        
        Lazy<Task<IEnumerable<T>>> LazilyAsync<T>(Expression expression, Action<IEnumerable<T>> onEval);

		/// <summary>
		/// Convert the Linq query to a lazy-count Lucene query and provide a function to execute when it is being evaluated
		/// </summary>
		Lazy<int> CountLazily<T>(Expression expression);

		/// <summary>
		/// Move the registered after query actions
		/// </summary>
		void MoveAfterQueryExecuted<T>(IAsyncDocumentQuery<T> documentQuery);

		/// <summary>
		/// Set the fields to fetch
		/// </summary>
		HashSet<string> FieldsToFetch { get; }

        /// <summary>
        /// The result transformer to use
        /// </summary>
	    string ResultTransformer { get; }

        /// <summary>
        /// Gets the query inputs being supplied to
        /// </summary>
        Dictionary<string, RavenJToken> QueryInputs { get; } 
	    
        /// <summary>
        /// Adds input to this query via a key/value pair
        /// </summary>
        /// <param name="input"></param>
        /// <param name="foo"></param>
        void AddQueryInput(string input, RavenJToken foo);

	}
}
