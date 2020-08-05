//-----------------------------------------------------------------------
// <copyright file="IDocumentQueryCustomization.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.Timings;
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

#if FEATURE_CUSTOM_SORTING
        /// <summary>
        /// Sort using custom sorter on the server
        /// </summary>
        IDocumentQueryCustomization CustomSortUsing(string typeName);

        /// <summary>
        /// Sort using custom sorter on the server
        /// </summary>
        IDocumentQueryCustomization CustomSortUsing(string typeName, bool descending);
#endif

        /// <summary>
        ///     Enables calculation of timings for various parts of a query (Lucene search, loading documents, transforming
        ///     results). Default: false
        /// </summary>
        IDocumentQueryCustomization Timings(out QueryTimings timings);
        
        /// <summary>
        ///   Instruct the query to wait for non stale results.
        ///   This shouldn't be used outside of unit tests unless you are well aware of the implications
        /// </summary>
        /// <param name = "waitTimeout">Maximum time to wait for index query results to become non-stale before exception is thrown. Default: 15 seconds.</param>
        IDocumentQueryCustomization WaitForNonStaleResults(TimeSpan? waitTimeout = null);

        IDocumentQueryCustomization Projection(ProjectionBehavior projectionBehavior);
    }
}
