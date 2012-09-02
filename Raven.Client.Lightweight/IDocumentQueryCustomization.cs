//-----------------------------------------------------------------------
// <copyright file="IDocumentQueryCustomization.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;

namespace Raven.Client
{
	/// <summary>
	/// Customize the document query
	/// </summary>
	public interface IDocumentQueryCustomization
	{
		/// <summary>
		/// Instructs the query to wait for non stale results as of the last write made by any session belonging to the 
		/// current document store.
		/// This ensures that you'll always get the most relevant results for your scenarios using simple indexes (map only or dynamic queries).
		/// However, when used to query map/reduce indexes, it does NOT guarantee that the document that this etag belong to is actually considered for the results. 
		/// </summary>
		IDocumentQueryCustomization WaitForNonStaleResultsAsOfLastWrite();

		/// <summary>
		/// Instructs the query to wait for non stale results as of the last write made by any session belonging to the 
		/// current document store.
		/// This ensures that you'll always get the most relevant results for your scenarios using simple indexes (map only or dynamic queries).
		/// However, when used to query map/reduce indexes, it does NOT guarantee that the document that this etag belong to is actually considered for the results. 
		/// </summary>
		IDocumentQueryCustomization WaitForNonStaleResultsAsOfLastWrite(TimeSpan waitTimeout);

		/// <summary>
		/// Instructs the query to wait for non stale results as of now.
		/// </summary>
		/// <returns></returns>
		IDocumentQueryCustomization WaitForNonStaleResultsAsOfNow();
		/// <summary>
		/// Instructs the query to wait for non stale results as of now for the specified timeout.
		/// </summary>
		/// <param name="waitTimeout">The wait timeout.</param>
		/// <returns></returns>
		IDocumentQueryCustomization WaitForNonStaleResultsAsOfNow(TimeSpan waitTimeout);

		/// <summary>
		/// Instructs the query to wait for non stale results as of the cutoff date.
		/// </summary>
		/// <param name="cutOff">The cut off.</param>
		/// <returns></returns>
		IDocumentQueryCustomization WaitForNonStaleResultsAsOf(DateTime cutOff);
		/// <summary>
		/// Instructs the query to wait for non stale results as of the cutoff date for the specified timeout
		/// </summary>
		/// <param name="cutOff">The cut off.</param>
		/// <param name="waitTimeout">The wait timeout.</param>
		IDocumentQueryCustomization WaitForNonStaleResultsAsOf(DateTime cutOff, TimeSpan waitTimeout);

		/// <summary>
		/// EXPERT ONLY: Instructs the query to wait for non stale results.
		/// This shouldn't be used outside of unit tests unless you are well aware of the implications
		/// </summary>
		IDocumentQueryCustomization WaitForNonStaleResults();

		/// <summary>
		/// Includes the specified path in the query, loading the document specified in that path
		/// </summary>
		/// <typeparam name="TResult">The type of the object that holds the id that you want to include.</typeparam>
		/// <param name="path">The path, which is name of the property that holds the id of the object to include.</param>
		/// <returns></returns>
		IDocumentQueryCustomization Include<TResult>(Expression<Func<TResult, object>> path);

		/// <summary>
		/// Includes the specified path in the query, loading the document specified in that path
		/// </summary>
		/// <typeparam name="TResult">The type of the object that holds the id that you want to include.</typeparam>
		/// <typeparam name="TInclude">The type of the object that you want to include.</typeparam>
		/// <param name="path">The path, which is name of the property that holds the id of the object to include.</param>
		/// <returns></returns>
		IDocumentQueryCustomization Include<TResult, TInclude>(Expression<Func<TResult, object>> path);

		/// <summary>
		/// Includes the specified path in the query, loading the document specified in that path
		/// </summary>
		/// <param name="path">The path.</param>
		IDocumentQueryCustomization Include(string path);

		/// <summary>
		/// EXPERT ONLY: Instructs the query to wait for non stale results for the specified wait timeout.
		/// This shouldn't be used outside of unit tests unless you are well aware of the implications
		/// </summary>
		/// <param name="waitTimeout">The wait timeout.</param>
		IDocumentQueryCustomization WaitForNonStaleResults(TimeSpan waitTimeout);
		
		/// <summary>
		/// Filter matches to be inside the specified radius
		/// </summary>
		/// <param name="radius">The radius.</param>
		/// <param name="latitude">The latitude.</param>
		/// <param name="longitude">The longitude.</param>
		IDocumentQueryCustomization WithinRadiusOf(double radius, double latitude, double longitude);

		/// <summary>
		/// Filter matches to be inside the specified radius
		/// </summary>
		/// <param name="radius">The radius.</param>
		/// <param name="latitude">The latitude.</param>
		/// <param name="longitude">The longitude.</param>
		IDocumentQueryCustomization WithinRadiusOf(string fieldName, double radius, double latitude, double longitude);

		/// <summary>
		/// Filter matches based on a given shape - only documents with the shape defined in fieldName that
		/// have a relation rel with the given shapeWKT will be returned
		/// </summary>
		/// <param name="fieldName">The name of the field containing the shape to use for filtering</param>
		/// <param name="shapeWKT">The query shape</param>
		/// <param name="rel">Spatial relation to check</param>
		/// <returns></returns>
		IDocumentQueryCustomization RelatesToShape(string fieldName, string shapeWKT, SpatialRelation rel);

		/// <summary>
		/// When using spatial queries, instruct the query to sort by the distance from the origin point
		/// </summary>
		IDocumentQueryCustomization SortByDistance();

		/// <summary>
		/// Order the search results randomly
		/// </summary>
		IDocumentQueryCustomization RandomOrdering();

		/// <summary>
		/// Order the search results randomly using the specified seed
		/// this is useful if you want to have repeatable random queries
		/// </summary>
		IDocumentQueryCustomization RandomOrdering(string seed);

		/// <summary>
		/// Execute the transfromation function on the results of this query.
		/// </summary>
		IDocumentQueryCustomization TransformResults(Func<IndexQuery,IEnumerable<object>, IEnumerable<object>> resultsTransformer);
	}
}