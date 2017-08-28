//-----------------------------------------------------------------------
// <copyright file="IDocumentQueryCustomization.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Linq.Expressions;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session.Operations;
using Sparrow.Json;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    ///     Customize the document query
    /// </summary>
    public interface IDocumentQueryCustomization
    {
        /// <summary>
        /// Get the raw query operation that will be sent to the server
        /// </summary>
        QueryOperation QueryOperation { get; }

        /// <summary>
        ///     Allow you to modify the index query before it is executed
        /// </summary>
        IDocumentQueryCustomization BeforeQueryExecuted(Action<IndexQuery> action);

        /// <summary>
        ///     Callback to get the results of the query
        /// </summary>
        IDocumentQueryCustomization AfterQueryExecuted(Action<QueryResult> action);

        /// <summary>
        ///     Callback to get the results of the stream
        /// </summary>
        IDocumentQueryCustomization AfterStreamExecuted(Action<BlittableJsonReaderObject> action);

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
        ///     Enables calculation of timings for various parts of a query (Lucene search, loading documents, transforming
        ///     results). Default: false
        /// </summary>
        IDocumentQueryCustomization ShowTimings();

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
        ///     <para>Since map/reduce queries, by their nature, tend to be far less susceptible to issues with staleness, this is </para>
        ///     <para>considered to be an acceptable trade-off.</para>
        ///     <para>If you need absolute no staleness with a map/reduce index, you will need to ensure synchronized clocks and </para>
        ///     <para>use the Cutoff date option, instead.</para>
        /// </param>
        IDocumentQueryCustomization WaitForNonStaleResultsAsOf(long cutOffEtag);

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
        ///     <para>Since map/reduce queries, by their nature, tend to be far less susceptible to issues with staleness, this is </para>
        ///     <para>considered to be an acceptable trade-off.</para>
        ///     <para>If you need absolute no staleness with a map/reduce index, you will need to ensure synchronized clocks and </para>
        ///     <para>use the Cutoff date option, instead.</para>
        /// </param>
        /// <param name="waitTimeout">Maximum time to wait for index query results to become non-stale before exception is thrown.</param>
        IDocumentQueryCustomization WaitForNonStaleResultsAsOf(long cutOffEtag, TimeSpan waitTimeout);

        /// <summary>
        ///     Instructs the query to wait for non stale results as of now.
        /// </summary>
        IDocumentQueryCustomization WaitForNonStaleResultsAsOfNow();

        /// <summary>
        ///     Instructs the query to wait for non stale results as of now for the specified timeout.
        /// </summary>
        /// <param name="waitTimeout">Maximum time to wait for index query results to become non-stale before exception is thrown.</param>
        IDocumentQueryCustomization WaitForNonStaleResultsAsOfNow(TimeSpan waitTimeout);
    }
}
