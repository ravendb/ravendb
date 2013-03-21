//-----------------------------------------------------------------------
// <copyright file="IRavenQueryable.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Linq;
using System.Linq.Expressions;
using Raven.Client.Indexes;
using Raven.Client.Spatial;
using Raven.Json.Linq;

namespace Raven.Client.Linq
{
	/// <summary>
	/// An implementation of <see cref="IOrderedQueryable{T}"/> with Raven specific operation
	/// </summary>
	public interface IRavenQueryable<T> : IOrderedQueryable<T>
	{
		/// <summary>
		/// Provide statistics about the query, such as total count of matching records
		/// </summary>
		IRavenQueryable<T> Statistics(out RavenQueryStatistics stats);

		/// <summary>
		/// Customizes the query using the specified action
		/// </summary>
		/// <param name="action">The action.</param>
		/// <returns></returns>
		IRavenQueryable<T> Customize(Action<IDocumentQueryCustomization> action);

        /// <summary>
        /// Specifies a result transformer to use on the results
        /// </summary>
        /// <typeparam name="TTransformer"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <returns></returns>
	    IRavenQueryable<TResult> TransformWith<TTransformer, TResult>() where TTransformer : AbstractTransformerCreationTask, new();

        /// <summary>
        /// Inputs a key and value to the query (accessible by the transformer)
        /// </summary>
	    IRavenQueryable<T> AddQueryInput(string name, RavenJToken value);

		IRavenQueryable<T> Spatial(Expression<Func<T, object>> path, Func<SpatialCriteriaFactory, SpatialCriteria> clause);
	}
}