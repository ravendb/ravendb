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
using Raven.Client.Spatial;

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
		/// Instructs the query to wait for non stale results as of the cutoff etag.
		/// </summary>
		/// <param name="cutOffEtag">The cut off etag.</param>
		IDocumentQueryCustomization WaitForNonStaleResultsAsOf(Etag cutOffEtag);

		/// <summary>
		/// Instructs the query to wait for non stale results as of the cutoff etag for the specified timeout.
		/// </summary>
		/// <param name="cutOffEtag">The cut off etag.</param>
		/// <param name="waitTimeout">The wait timeout.</param>
		IDocumentQueryCustomization WaitForNonStaleResultsAsOf(Etag cutOffEtag, TimeSpan waitTimeout);

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
		IDocumentQueryCustomization WithinRadiusOf(double radius, double latitude, double longitude);

		/// <summary>
		/// Filter matches to be inside the specified radius
		/// </summary>
		IDocumentQueryCustomization WithinRadiusOf(string fieldName, double radius, double latitude, double longitude);
		
		/// <summary>
		/// Filter matches to be inside the specified radius
		/// </summary>
		IDocumentQueryCustomization WithinRadiusOf(double radius, double latitude, double longitude, SpatialUnits radiusUnits);

		/// <summary>
		/// Filter matches to be inside the specified radius
		/// </summary>
		IDocumentQueryCustomization WithinRadiusOf(string fieldName, double radius, double latitude, double longitude, SpatialUnits radiusUnits);

		/// <summary>
		/// Filter matches based on a given shape - only documents with the shape defined in fieldName that
		/// have a relation rel with the given shapeWKT will be returned
		/// </summary>
		/// <param name="fieldName">The name of the field containing the shape to use for filtering</param>
		/// <param name="shapeWKT">The query shape</param>
		/// <param name="rel">Spatial relation to check</param>
		/// <returns></returns>
		IDocumentQueryCustomization RelatesToShape(string fieldName, string shapeWKT, SpatialRelation rel);

		IDocumentQueryCustomization Spatial(string fieldName, Func<SpatialCriteriaFactory, SpatialCriteria> clause);

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
		/// Allow you to modify the index query before it is executed
		/// </summary>
		IDocumentQueryCustomization BeforeQueryExecution(Action<IndexQuery> action);

		/// <summary>
		/// Execute the transformation function on the results of this query.
		/// </summary>
		[Obsolete("Use Result Transformers instead.")]
		IDocumentQueryCustomization TransformResults(Func<IndexQuery,IEnumerable<object>, IEnumerable<object>> resultsTransformer);

		/// <summary>
		///   Adds matches highlighting for the specified field.
		/// </summary>
		/// <remarks>
		///   The specified field should be analysed and stored for highlighter to work.
		///   For each match it creates a fragment that contains matched text surrounded by highlighter tags.
		/// </remarks>
		/// <param name="fieldName">The field name to highlight.</param>
		/// <param name="fragmentLength">The fragment length.</param>
		/// <param name="fragmentCount">The maximum number of fragments for the field.</param>
		/// <param name="fragmentsField">The field in query results item to put highlightings into.</param>
		IDocumentQueryCustomization Highlight(string fieldName, int fragmentLength, int fragmentCount, string fragmentsField);

		/// <summary>
		///   Adds matches highlighting for the specified field.
		/// </summary>
		/// <remarks>
		///   The specified field should be analysed and stored for highlighter to work.
		///   For each match it creates a fragment that contains matched text surrounded by highlighter tags.
		/// </remarks>
		/// <param name="fieldName">The field name to highlight.</param>
		/// <param name="fragmentLength">The fragment length.</param>
		/// <param name="fragmentCount">The maximum number of fragments for the field.</param>
		/// <param name="highlightings">Field highlightings for all results.</param>
		IDocumentQueryCustomization Highlight(string fieldName, int fragmentLength, int fragmentCount, out FieldHighlightings highlightings);

		/// <summary>
		///   Sets the tags to highlight matches with.
		/// </summary>
		/// <param name="preTag">Prefix tag.</param>
		/// <param name="postTag">Postfix tag.</param>
		IDocumentQueryCustomization SetHighlighterTags(string preTag, string postTag);

		/// <summary>
		///   Sets the tags to highlight matches with.
		/// </summary>
		/// <param name="preTags">Prefix tags.</param>
		/// <param name="postTags">Postfix tags.</param>
		IDocumentQueryCustomization SetHighlighterTags(string[] preTags, string[] postTags);

		/// <summary>
		/// Disables tracking for queried entities by Raven's Unit of Work.
		/// Usage of this option will prevent holding query results in memory.
		/// </summary>
		IDocumentQueryCustomization NoTracking();

		/// <summary>
		/// Disables caching for query results.
		/// </summary>
		IDocumentQueryCustomization NoCaching();
	}
}