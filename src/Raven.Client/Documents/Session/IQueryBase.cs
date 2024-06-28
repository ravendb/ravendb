using System;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.Timings;
using Sparrow.Json;

namespace Raven.Client.Documents.Session;

public interface IQueryBase<T, out TSelf>
    where TSelf : IQueryBase<T, TSelf>
{
    /// <summary>
    ///     Gets the document convention from the query session
    /// </summary>
    DocumentConventions Conventions { get; }

    /// <summary>
    ///     Callback to get the results of the query
    /// </summary>
    TSelf AfterQueryExecuted(Action<QueryResult> action);


    /// <summary>
    ///     Callback to get the results of the stream
    /// </summary>
    TSelf AfterStreamExecuted(Action<BlittableJsonReaderObject> action);

    /// <summary>
    ///     Allows you to modify the index query before it is sent to the server
    /// </summary>
    TSelf BeforeQueryExecuted(Action<IndexQuery> beforeQueryExecuted);

    /// <summary>
    ///     Called externally to raise the after query executed callback
    /// </summary>
    void InvokeAfterQueryExecuted(QueryResult result);

    /// <summary>
    ///     Called externally to raise the after query executed callback
    /// </summary>
    void InvokeAfterStreamExecuted(BlittableJsonReaderObject result);

    /// <summary>
    ///     Disables caching for query results.
    /// </summary>
    TSelf NoCaching();

    /// <summary>
    ///     Disables tracking for queried entities by Raven's Unit of Work.
    ///     Usage of this option will prevent holding query results in memory.
    /// </summary>
    TSelf NoTracking();

    /// <summary>
    ///     Enables calculation of query execution time. Returns both total time of query and time spent on each query part.
    ///     Timings are not enabled by default.
    /// </summary>
    /// <param name="timings">An out param that will be filled with the timings results.</param>
    /// <inheritdoc cref="DocumentationUrls.Session.Querying.QueryTimings"/>
    TSelf Timings(out QueryTimings timings);

    /// <summary>
    ///     Provide statistics about the query, such as total count of matching records
    /// </summary>
    TSelf Statistics(out QueryStatistics stats);

    /// <summary>
    ///     Select the default operator to use for this query
    /// </summary>
    TSelf UsingDefaultOperator(QueryOperator queryOperator);

    /// <summary>
    ///   Instruct the query to wait for non stale results.
    ///   This shouldn't be used outside of unit tests unless you are well aware of the implications
    /// </summary>
    /// <param name = "waitTimeout">Maximum time to wait for index query results to become non-stale before exception is thrown. Default: 15 seconds.</param>
    TSelf WaitForNonStaleResults(TimeSpan? waitTimeout = null);

    /// <summary>
    ///     Create the index query object for this query
    /// </summary>
    IndexQuery GetIndexQuery();

    /// <summary>
    /// Add a named parameter to the query
    /// </summary>
    TSelf AddParameter(string name, object value);
}
