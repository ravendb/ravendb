//-----------------------------------------------------------------------
// <copyright file="IDocumentQueryCustomization.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.Timings;
using Raven.Client.Documents.Session.Operations;
using Raven.Client.Documents.Session.Querying.Sharding;
using Sparrow.Json;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    ///     Customize the document query
    /// </summary>
    public interface IDocumentQueryCustomization
    {
        /// <summary>
        ///     Get the raw query operation that will be sent to the server.
        /// </summary>
        QueryOperation QueryOperation { get; }

        /// <summary>
        ///     Allows to modify the index query before it is executed.
        /// </summary>
        /// <param name="action">Action with index query parameter. Defines the method that will be executed before query execution.</param>
        /// <inheritdoc cref="DocumentationUrls.Session.Querying.HowToCustomizeQuery.BeforeQueryExecuted"/>
        IDocumentQueryCustomization BeforeQueryExecuted(Action<IndexQuery> action);

        /// <summary>
        ///     Allows to access raw query result after the execution.
        /// </summary>
        /// <param name="action">Action with query result parameter. Defines the method that will be executed after query execution.</param>
        /// <inheritdoc cref="DocumentationUrls.Session.Querying.HowToCustomizeQuery.AfterQueryExecuted"/>
        IDocumentQueryCustomization AfterQueryExecuted(Action<QueryResult> action);

        /// <summary>
        ///     Allows to access raw streaming query result (in form of BlittableJsonReaderObject) after the query execution.
        /// </summary>
        /// <param name="action">Action with stream result parameter. Defines the method that will be executed after streaming query execution.</param>
        /// <inheritdoc cref="DocumentationUrls.Session.Querying.HowToCustomizeQuery.AfterStreamExecuted"/>
        IDocumentQueryCustomization AfterStreamExecuted(Action<BlittableJsonReaderObject> action);

        /// <summary>
        ///     Disables caching of query results.
        ///     Forces RavenDB to always fetch query results from the server.
        ///     By default query results are cached.
        /// </summary>
        /// <inheritdoc cref="DocumentationUrls.Session.Querying.HowToCustomizeQuery.NoCaching"/>
        IDocumentQueryCustomization NoCaching();

        /// <summary>
        ///     Disables tracking of query results.
        ///     Any changes made to them will be ignored by RavenDB.
        ///     Usage of this option will prevent holding query results in memory.
        /// </summary>
        /// <inheritdoc cref="DocumentationUrls.Session.Querying.HowToCustomizeQuery.NoTracking"/>
        IDocumentQueryCustomization NoTracking();

        /// <summary>
        ///     Orders the query results randomly.
        /// </summary>
        /// <inheritdoc cref="DocumentationUrls.Session.Querying.SortQueryResults.OrderByRandom"/>
        IDocumentQueryCustomization RandomOrdering();

        /// <summary>
        ///     Orders the query results randomly using the specified seed.
        ///     Allows to repeat random query results.
        /// </summary>
        /// <param name="seed">Seed to be used for pseudorandom number generator.</param>
        /// <inheritdoc cref="DocumentationUrls.Session.Querying.SortQueryResults.OrderByRandom"/>
        IDocumentQueryCustomization RandomOrdering(string seed);

#if FEATURE_CUSTOM_SORTING
        /// <summary>
        ///     Sort query results using server-side custom sorter.
        ///     Requires custom sorting feature to be enabled.
        /// </summary>
        /// <param name="typeName">Name of the custom sorter to be used.</param>
        /// <inheritdoc cref="DocumentationUrls.Session.Querying.SortQueryResults.CustomSorters"/>
        IDocumentQueryCustomization CustomSortUsing(string typeName);

        /// <inheritdoc cref="CustomSortUsing(string)" />
        /// <param name="descending">Changes order to descending.</param>
        IDocumentQueryCustomization CustomSortUsing(string typeName, bool descending);
#endif

        /// <inheritdoc cref="IQueryBase{T,TSelf}.Timings" />
        IDocumentQueryCustomization Timings(out QueryTimings timings);
        
        /// <summary>
        ///     Instruct the query to wait for non-stale results.
        ///     This shouldn't be used outside of unit tests unless you are well aware of the implications.
        /// </summary>
        /// <param name="waitTimeout">Maximum time in seconds to wait for index query results to become non-stale before exception is thrown. Default: 15 seconds.</param>
        IDocumentQueryCustomization WaitForNonStaleResults(TimeSpan? waitTimeout = null);

        /// <inheritdoc cref="IRawDocumentQuery{T}.Projection" />
        IDocumentQueryCustomization Projection(ProjectionBehavior projectionBehavior);

        /// <summary>
        ///     Allows to execute query only on relevant shards.
        /// </summary>
        /// <param name="builder">Action with shard context parameter. Defines on which shards the query will be executed.</param>
        /// <inheritdoc cref="DocumentationUrls.Session.Sharding.QueryingASelectedShard"/>
        IDocumentQueryCustomization ShardContext(Action<IQueryShardedContextBuilder> builder);
    }
}
