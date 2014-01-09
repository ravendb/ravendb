//-----------------------------------------------------------------------
// <copyright file="IDocumentQuery.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Raven.Abstractions.Data;
using Raven.Client.Linq;
using Raven.Client.Spatial;
using Raven.Json.Linq;

namespace Raven.Client
{
	/// <summary>
	/// A query against a Raven index
	/// </summary>
	public interface IDocumentQuery<T> : IEnumerable<T>, IDocumentQueryBase<T, IDocumentQuery<T>>
	{
		/// <summary>
		/// Selects the specified fields directly from the index
		/// </summary>
		/// <typeparam name="TProjection">The type of the projection.</typeparam>
		/// <param name="fields">The fields.</param>
		IDocumentQuery<TProjection> SelectFields<TProjection>(params string[] fields);

		/// <summary>
		/// Selects the specified fields directly from the index
		/// </summary>
		/// <typeparam name="TProjection">The type of the projection.</typeparam>
		IDocumentQuery<TProjection> SelectFields<TProjection>(string[] fields, string[] projections);

		/// <summary>
		/// Selects the projection fields directly from the index
		/// </summary>
		/// <typeparam name="TProjection">The type of the projection.</typeparam>
		IDocumentQuery<TProjection> SelectFields<TProjection>();

		/// <summary>
		/// Sets user defined inputs to the query
		/// </summary>
		/// <param name="queryInputs"></param>
		void SetQueryInputs(Dictionary<string, RavenJToken> queryInputs);

#if !SILVERLIGHT
		/// <summary>
		/// Gets the query result
		/// Execute the query the first time that this is called.
		/// </summary>
		/// <value>The query result.</value>
		QueryResult QueryResult { get; }
		bool IsDistinct { get; }
#endif

		/// <summary>
		/// Register the query as a lazy query in the session and return a lazy
		/// instance that will evaluate the query only when needed
		/// </summary>
		Lazy<IEnumerable<T>> Lazily();

		/// <summary>
		/// Register the query as a lazy query in the session and return a lazy
		/// instance that will evaluate the query only when needed.
		/// Also provide a function to execute when the value is evaluated
		/// </summary>
		Lazy<IEnumerable<T>> Lazily(Action<IEnumerable<T>> onEval);

		/// <summary>
		/// Register the query as a lazy-count query in the session and return a lazy
		/// instance that will evaluate the query only when needed.
		/// </summary>
		Lazy<int> CountLazily();

		/// <summary>
		/// Create the index query object for this query
		/// </summary>
		IndexQuery GetIndexQuery(bool isAsync);

		IDocumentQuery<T> Spatial(Expression<Func<T, object>> path, Func<SpatialCriteriaFactory, SpatialCriteria> clause);

		IDocumentQuery<T> Spatial(string name, Func<SpatialCriteriaFactory, SpatialCriteria> clause);

		/// <summary>
		/// Get the facets as per the specified doc with the given start and pageSize
		/// </summary>
		FacetResults GetFacets(string facetSetupDoc, int facetStart, int? facetPageSize);

		/// <summary>
		/// Get the facets as per the specified facets with the given start and pageSize
		/// </summary>
		FacetResults GetFacets(List<Facet> facets, int facetStart, int? facetPageSize);
	}
}