//-----------------------------------------------------------------------
// <copyright file="IDocumentQueryCustomization.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq.Expressions;

using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Abstractions.Indexing;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Spatial;

namespace Raven.NewClient.Client
{
    /// <summary>
    ///     Customize the document query
    /// </summary>
    public interface IDocumentQueryCustomization
    {
        /// <summary>
        ///     Allow you to modify the index query before it is executed
        /// </summary>
        IDocumentQueryCustomization BeforeQueryExecution(Action<IndexQuery> action);

        /// <summary>
        ///     Adds matches highlighting for the specified field.
        /// </summary>
        /// <remarks>
        ///     The specified field should be analysed and stored for highlighter to work.
        ///     For each match it creates a fragment that contains matched text surrounded by highlighter tags.
        /// </remarks>
        /// <param name="fieldName">The field name to highlight.</param>
        /// <param name="fragmentLength">The fragment length.</param>
        /// <param name="fragmentCount">The maximum number of fragments for the field.</param>
        /// <param name="fragmentsField">The field in query results item to put highlightings into.</param>
        IDocumentQueryCustomization Highlight(string fieldName, int fragmentLength, int fragmentCount, string fragmentsField);

        /// <summary>
        ///     Adds matches highlighting for the specified field.
        /// </summary>
        /// <remarks>
        ///     The specified field should be analysed and stored for highlighter to work.
        ///     For each match it creates a fragment that contains matched text surrounded by highlighter tags.
        /// </remarks>
        /// <param name="fieldName">The field name to highlight.</param>
        /// <param name="fragmentLength">The fragment length.</param>
        /// <param name="fragmentCount">The maximum number of fragments for the field.</param>
        /// <param name="highlightings">Field highlightings for all results.</param>
        IDocumentQueryCustomization Highlight(string fieldName, int fragmentLength, int fragmentCount, out FieldHighlightings highlightings);

        /// <summary>
        ///     Adds matches highlighting for the specified field on a Map/Reduce Index.
        /// </summary>
        /// <remarks>
        ///     This is only valid for Map/Reduce Index querys.
        ///     The specified field and key should be analysed and stored for highlighter to work.
        ///     For each match it creates a fragment that contains matched text surrounded by highlighter tags.
        /// </remarks>
        /// <param name="fieldName">The field name to highlight.</param>
        /// <param name="fieldKeyName">The field key to associate highlights with.</param>
        /// <param name="fragmentLength">The fragment length.</param>
        /// <param name="fragmentCount">The maximum number of fragments for the field.</param>
        /// <param name="highlightings">Field highlightings for all results.</param>
        IDocumentQueryCustomization Highlight(string fieldName, string fieldKeyName, int fragmentLength, int fragmentCount, out FieldHighlightings highlightings);

        /// <summary>
        ///     Includes the specified path in the query, loading the document specified in that path
        /// </summary>
        /// <typeparam name="TResult">The type of the object that holds the id that you want to include.</typeparam>
        /// <param name="path">The path, which is name of the property that holds the id of the object to include.</param>
        /// <returns></returns>
        IDocumentQueryCustomization Include<TResult>(Expression<Func<TResult, object>> path);

        /// <summary>
        ///     Includes the specified path in the query, loading the document specified in that path
        /// </summary>
        /// <typeparam name="TResult">The type of the object that holds the id that you want to include.</typeparam>
        /// <typeparam name="TInclude">The type of the object that you want to include.</typeparam>
        /// <param name="path">Path in documents in which server should look for a 'referenced' documents.</param>
        IDocumentQueryCustomization Include<TResult, TInclude>(Expression<Func<TResult, object>> path);

        /// <summary>
        ///     Includes the specified path in the query, loading the document specified in that path
        /// </summary>
        /// <param name="path">Path in documents in which server should look for a 'referenced' documents.</param>
        IDocumentQueryCustomization Include(string path);

        /// <summary>
        ///     Disables caching for query results.
        /// </summary>
        IDocumentQueryCustomization NoCaching();

        /// <summary>
        ///     Disables tracking for queried entities by Raven's Unit of Work.
        ///     Usage of this option will prevent holding query results in memory.
        /// </summary>
        IDocumentQueryCustomization NoTracking();

        /// <summary>
        ///     Adds an ordering for a specific field to the query
        ///		<param name="fieldName">Name of the field.</param>
        ///		<param name="descending">if set to <c>true</c> [descending].</param>
        /// </summary>
        IDocumentQueryCustomization AddOrder(string fieldName, bool descending = false);

        /// <summary>
        ///     Adds an ordering for a specific field to the query
        ///		<typeparam name="TResult">The type of the object that holds the property that you want to order by.</typeparam>
        ///		<param name="propertySelector">Property selector for the field.</param>
        ///		<param name="descending">if set to <c>true</c> [descending].</param>
        /// </summary>
        IDocumentQueryCustomization AddOrder<TResult>(Expression<Func<TResult, object>> propertySelector, bool descending = false);

        /// <summary>
        ///     Adds an ordering for a specific field to the query and specifies the type of field for sorting purposes
        ///		<param name="fieldName">Name of the field.</param>
        ///		<param name="descending">if set to <c>true</c> [descending].</param>
        ///		<param name="fieldType">the type of the field to be sorted.</param>
        /// </summary>
        IDocumentQueryCustomization AddOrder(string fieldName, bool descending, Type fieldType);

        /// <summary>
        ///		Order the search results in alphanumeric order
        ///		<param name="fieldName">The order by field name.</param>
        ///		<param name="descending">Should be ordered by descending.</param>
        /// </summary>
        IDocumentQueryCustomization AlphaNumericOrdering(string fieldName, bool descending = false);

        /// <summary>
        ///		Order the search results in alphanumeric order
        ///		<typeparam name="TResult">The type of the object that holds the property that you want to order by.</typeparam>
        ///		<param name="propertySelector">Property selector for the field.</param>
        ///		<param name="descending">if set to <c>true</c> [descending].</param>
        /// </summary>
        IDocumentQueryCustomization AlphaNumericOrdering<TResult>(Expression<Func<TResult, object>> propertySelector, bool descending = false);

        /// <summary>
        ///     Order the search results randomly
        /// </summary>
        IDocumentQueryCustomization RandomOrdering();

        /// <summary>
        ///     Order the search results randomly using the specified seed
        ///     this is useful if you want to have repeatable random queries
        /// </summary>
        IDocumentQueryCustomization RandomOrdering(string seed);

        /// <summary>
        /// Sort using custom sorter on the server
        /// </summary>
        IDocumentQueryCustomization CustomSortUsing(string typeName);

        /// <summary>
        /// Sort using custom sorter on the server
        /// </summary>
        IDocumentQueryCustomization CustomSortUsing(string typeName, bool descending);

        /// <summary>
        ///     Filter matches based on a given shape - only documents with the shape defined in fieldName that
        ///     have a relation rel with the given shapeWKT will be returned
        /// </summary>
        /// <param name="fieldName">Spatial field name.</param>
        /// <param name="shapeWKT">WKT formatted shape</param>
        /// <param name="rel">Spatial relation to check (Within, Contains, Disjoint, Intersects, Nearby)</param>
        /// <param name="distErrorPercent">"Gets the error distance that specifies how precise the query shape is."</param>
        IDocumentQueryCustomization RelatesToShape(string fieldName, string shapeWKT, SpatialRelation rel, double distErrorPercent=0.025);

        /// <summary>
        ///     If set to true, this property will send multiple index entries from the same document (assuming the index project
        ///     them)
        ///     to the result transformer function. Otherwise, those entries will be consolidate an the transformer will be
        ///     called just once for each document in the result set
        /// </summary>
        IDocumentQueryCustomization SetAllowMultipleIndexEntriesForSameDocumentToResultTransformer(bool val);

        /// <summary>
        ///     Sets the tags to highlight matches with.
        /// </summary>
        /// <param name="preTag">Prefix tag.</param>
        /// <param name="postTag">Postfix tag.</param>
        IDocumentQueryCustomization SetHighlighterTags(string preTag, string postTag);

        /// <summary>
        ///     Sets the tags to highlight matches with.
        /// </summary>
        /// <param name="preTags">Prefix tags.</param>
        /// <param name="postTags">Postfix tags.</param>
        IDocumentQueryCustomization SetHighlighterTags(string[] preTags, string[] postTags);

        /// <summary>
        ///     Enables calculation of timings for various parts of a query (Lucene search, loading documents, transforming
        ///     results). Default: false
        /// </summary>
        IDocumentQueryCustomization ShowTimings();

        /// <summary>
        ///     When using spatial queries, instruct the query to sort by the distance from the origin point
        /// </summary>
        IDocumentQueryCustomization SortByDistance();

        /// <summary>
        ///     Ability to use one factory to determine spatial shape that will be used in query.
        /// </summary>
        /// <param name="fieldName">Spatial field name.</param>
        /// <param name="clause">function with spatial criteria factory</param>
        IDocumentQueryCustomization Spatial(string fieldName, Func<SpatialCriteriaFactory, SpatialCriteria> clause);

        /// <summary>
        ///     Execute the transformation function on the results of this query.
        /// </summary>
        IDocumentQueryCustomization TransformResults(Func<IndexQuery, IEnumerable<object>, IEnumerable<object>> resultsTransformer);

        /// <summary>
        ///     EXPERT ONLY: Instructs the query to wait for non stale results.
        ///     This shouldn't be used outside of unit tests unless you are well aware of the implications
        /// </summary>
        IDocumentQueryCustomization WaitForNonStaleResults();

        /// <summary>
        ///     EXPERT ONLY: Instructs the query to wait for non stale results for the specified wait timeout.
        ///     This shouldn't be used outside of unit tests unless you are well aware of the implications
        /// </summary>
        /// <param name="waitTimeout">Maximum time to wait for index query results to become non-stale before exception is thrown.</param>
        IDocumentQueryCustomization WaitForNonStaleResults(TimeSpan waitTimeout);

        /// <summary>
        ///     Instructs the query to wait for non stale results as of the cutoff etag.
        /// </summary>
        /// <param name="cutOffEtag">
        ///     <para>Cutoff etag is used to check if the index has already process a document with the given</para>
        ///     <para>etag. Unlike Cutoff, which uses dates and is susceptible to clock synchronization issues between</para>
        ///     <para>machines, cutoff etag doesn't rely on both the server and client having a synchronized clock and </para>
        ///     <para>can work without it.</para>
        ///     <para>However, when used to query map/reduce indexes, it does NOT guarantee that the document that this</para>
        ///     <para>etag belong to is actually considered for the results. </para>
        ///     <para>What it does it guarantee that the document has been mapped, but not that the mapped values has been reduced. </para>
        ///     <para>Since map/reduce queries, by their nature,vtend to be far less susceptible to issues with staleness, this is </para>
        ///     <para>considered to be an acceptable tradeoff.</para>
        ///     <para>If you need absolute no staleness with a map/reduce index, you will need to ensure synchronized clocks and </para>
        ///     <para>use the Cutoff date option, instead.</para>
        /// </param>
        IDocumentQueryCustomization WaitForNonStaleResultsAsOf(long? cutOffEtag);

        /// <summary>
        ///     Instructs the query to wait for non stale results as of the cutoff etag for the specified timeout.
        /// </summary>
        /// <param name="cutOffEtag">
        ///     <para>Cutoff etag is used to check if the index has already process a document with the given</para>
        ///     <para>etag. Unlike Cutoff, which uses dates and is susceptible to clock synchronization issues between</para>
        ///     <para>machines, cutoff etag doesn't rely on both the server and client having a synchronized clock and </para>
        ///     <para>can work without it.</para>
        ///     <para>However, when used to query map/reduce indexes, it does NOT guarantee that the document that this</para>
        ///     <para>etag belong to is actually considered for the results. </para>
        ///     <para>What it does it guarantee that the document has been mapped, but not that the mapped values has been reduced. </para>
        ///     <para>Since map/reduce queries, by their nature,vtend to be far less susceptible to issues with staleness, this is </para>
        ///     <para>considered to be an acceptable tradeoff.</para>
        ///     <para>If you need absolute no staleness with a map/reduce index, you will need to ensure synchronized clocks and </para>
        ///     <para>use the Cutoff date option, instead.</para>
        /// </param>
        /// <param name="waitTimeout">Maximum time to wait for index query results to become non-stale before exception is thrown.</param>
        IDocumentQueryCustomization WaitForNonStaleResultsAsOf(long? cutOffEtag, TimeSpan waitTimeout);

        /// <summary>
        ///     Instructs the query to wait for non stale results as of the last write made by any session belonging to the
        ///     current document store.
        ///     This ensures that you'll always get the most relevant results for your scenarios using simple indexes (map only or
        ///     dynamic queries).
        ///     However, when used to query map/reduce indexes, it does NOT guarantee that the document that this etag belong to is
        ///     actually considered for the results.
        /// </summary>
        IDocumentQueryCustomization WaitForNonStaleResultsAsOfLastWrite();

        /// <summary>
        ///     Instructs the query to wait for non stale results as of the last write made by any session belonging to the
        ///     current document store.
        ///     This ensures that you'll always get the most relevant results for your scenarios using simple indexes (map only or
        ///     dynamic queries).
        ///     However, when used to query map/reduce indexes, it does NOT guarantee that the document that this etag belong to is
        ///     actually considered for the results.
        /// </summary>
        /// <param name="waitTimeout">Maximum time to wait for index query results to become non-stale before exception is thrown.</param>
        IDocumentQueryCustomization WaitForNonStaleResultsAsOfLastWrite(TimeSpan waitTimeout);

        /// <summary>
        ///     Instructs the query to wait for non stale results as of now.
        /// </summary>
        IDocumentQueryCustomization WaitForNonStaleResultsAsOfNow();

        /// <summary>
        ///     Instructs the query to wait for non stale results as of now for the specified timeout.
        /// </summary>
        /// <param name="waitTimeout">Maximum time to wait for index query results to become non-stale before exception is thrown.</param>
        IDocumentQueryCustomization WaitForNonStaleResultsAsOfNow(TimeSpan waitTimeout);

        /// <summary>
        ///     Filter matches to be inside the specified radius. This method assumes that spatial data is found under default
        ///     spatial field name: __spatial
        /// </summary>
        /// <param name="radius">Radius (in kilometers) in which matches should be found.</param>
        /// <param name="latitude">Latitude poiting to a circle center.</param>
        /// <param name="longitude">Longitude poiting to a circle center.</param>
        /// <param name="distErrorPercent">"Gets the error distance that specifies how precise the query shape is."</param>
        IDocumentQueryCustomization WithinRadiusOf(double radius, double latitude, double longitude, double distErrorPercent = 0.025);

        /// <summary>
        ///     Filter matches to be inside the specified radius
        /// </summary>
        /// <param name="fieldName">Spatial field name.</param>
        /// <param name="radius">Radius (in kilometers) in which matches should be found.</param>
        /// <param name="latitude">Latitude poiting to a circle center.</param>
        /// <param name="longitude">Longitude poiting to a circle center.</param>
        /// <param name="distErrorPercent">"Gets the error distance that specifies how precise the query shape is."</param>
        IDocumentQueryCustomization WithinRadiusOf(string fieldName, double radius, double latitude, double longitude, double distErrorPercent=0.025);

        /// <summary>
        ///     Filter matches to be inside the specified radius
        /// </summary>
        /// <param name="radius">Radius (measured in units passed to radiusUnits parameter) in which matches should be found.</param>
        /// <param name="latitude">Latitude poiting to a circle center.</param>
        /// <param name="longitude">Longitude poiting to a circle center.</param>
        /// <param name="radiusUnits">Units that will be used to measure distances (Kilometers, Miles).</param>
        /// <param name="distErrorPercent">"Gets the error distance that specifies how precise the query shape is."</param>
        IDocumentQueryCustomization WithinRadiusOf(double radius, double latitude, double longitude, SpatialUnits radiusUnits, double distErrorPercent = 0.025);

        /// <summary>
        ///     Filter matches to be inside the specified radius
        /// </summary>
        /// <param name="fieldName">Spatial field name.</param>
        /// <param name="radius">Radius (measured in units passed to radiusUnits parameter) in which matches should be found.</param>
        /// <param name="latitude">Latitude poiting to a circle center.</param>
        /// <param name="longitude">Longitude poiting to a circle center.</param>
        /// <param name="radiusUnits">Units that will be used to measure distances (Kilometers, Miles).</param>
        /// <param name="distErrorPercent">"Gets the error distance that specifies how precise the query shape is."</param>
        IDocumentQueryCustomization WithinRadiusOf(string fieldName, double radius, double latitude, double longitude, SpatialUnits radiusUnits, double distErrorPercent=0.025);

        /// <summary>
        /// When using spatial queries, instruct the query to sort by the distance from the origin point
        /// </summary>
        IDocumentQueryCustomization SortByDistance(double lat, double lng);

        /// <summary>
        /// When using spatial queries, instruct the query to sort by the distance from the origin point
        /// </summary>
        IDocumentQueryCustomization SortByDistance(double lat, double lng, string fieldName);
    }
}
