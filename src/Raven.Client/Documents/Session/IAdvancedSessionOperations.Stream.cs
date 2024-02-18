//-----------------------------------------------------------------------
// <copyright file="ISyncAdvancedSessionOperation.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;
using System.IO;
using System.Linq;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Queries.TimeSeries;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    ///     Advanced synchronous session operations
    /// </summary>
    public partial interface IAdvancedSessionOperations : 
        IDocumentsSessionOperations,
        ITimeSeriesSessionStreamOperations<TimeSeriesAggregationResult>,
        ITimeSeriesSessionStreamOperations<TimeSeriesRawResult>,
        ITimeSeriesSessionStreamAggregationResultOperations,
        ITimeSeriesSessionStreamRawResultOperations
    {

    }

    public interface IDocumentsSessionOperations
    {
        /// <summary>
        ///     Creates a stream to fetch documents that satisfy criteria <paramref name="query"/>.
        /// <para>
        ///     Documents are converted to CLR type <typeparamref name="T"/> along the way and wrapped in <see cref="StreamResult{T}"/> containing document metadata.
        /// </para>
        /// </summary>
        /// <remarks>
        ///     Does NOT track the entities in the session and will not include its changes when <see cref="IDocumentSession.SaveChanges"/> is called.
        /// </remarks>
        /// <param name="query">The criteria by which to fetch documents from the database</param>
        /// <returns>An enumerator used to iterate over the collection of documents received via the stream</returns>
        IEnumerator<StreamResult<T>> Stream<T>(IQueryable<T> query);

        /// <summary>
        ///     Creates a stream to fetch documents that satisfy criteria <paramref name="query"/>.
        /// <para>
        ///     Documents are converted to CLR type <typeparamref name="T"/> along the way and wrapped in <see cref="StreamResult{T}"/> containing document metadata.
        /// </para>
        /// </summary>
        /// <remarks>
        ///     Does NOT track the entities in the session and will not include its changes when <see cref="IDocumentSession.SaveChanges"/> is called.
        /// </remarks>
        /// <param name="query">The criteria by which to fetch documents from the database</param>
        /// <param name="streamQueryStats">Information and statistics about the performed query</param>
        /// <returns>An enumerator used to iterate over the collection of documents received via the stream</returns>
        IEnumerator<StreamResult<T>> Stream<T>(IQueryable<T> query, out StreamQueryStatistics streamQueryStats);

        /// <inheritdoc cref="Stream{T}(IQueryable{T}) "/>
        IEnumerator<StreamResult<T>> Stream<T>(IDocumentQuery<T> query);

        /// <inheritdoc cref="Stream{T}(IQueryable{T}) "/>
        IEnumerator<StreamResult<T>> Stream<T>(IRawDocumentQuery<T> query);

        /// <inheritdoc cref="Stream{T}(IQueryable{T}, out StreamQueryStatistics) "/>
        IEnumerator<StreamResult<T>> Stream<T>(IRawDocumentQuery<T> query, out StreamQueryStatistics streamQueryStats);

        /// <inheritdoc cref="Stream{T}(IQueryable{T}, out StreamQueryStatistics) "/>
        IEnumerator<StreamResult<T>> Stream<T>(IDocumentQuery<T> query, out StreamQueryStatistics streamQueryStats);

        /// <summary>
        ///     Creates a stream to fetch documents whose IDs match the given parameters.
        /// <para>
        ///     Documents are converted to CLR type <typeparamref name="T"/> along the way and wrapped in <see cref="StreamResult{T}"/> containing document metadata.
        /// </para>
        /// </summary>
        /// <remarks>
        ///     Does NOT track the entities in the session and will not include its changes when <see cref="IDocumentSession.SaveChanges"/> is called.
        /// </remarks>
        /// <example></example>
        /// <param name="startsWith">ID prefix for which documents should be returned. e.g. "products/"</param>
        /// <param name="matches">
        ///     pipe ('|') separated values by which to match the part following <paramref name="startsWith"/>. <br/>
        ///     '?' - any single character <br/>
        ///     '*' - any characters
        /// </param>
        /// <param name="start">The number of documents that should be skipped</param>
        /// <param name="pageSize">The maximum number of documents that will be retrieved</param>
        /// <param name="startAfter">Skip document fetching until given ID is found and only return documents after that ID.</param>
        /// <returns>An enumerator used to iterate over the collection of documents received via the stream</returns>
        IEnumerator<StreamResult<T>> Stream<T>(string startsWith, string matches = null, int start = 0, int pageSize = int.MaxValue, string startAfter = null);

        /// <summary>
        ///     Feeds the query results into given stream <paramref name="output"/>.
        /// </summary>
        /// <remarks>
        ///     Does NOT track the entities in the session and will not include its changes when <see cref="IDocumentSession.SaveChanges"/> is called.
        /// </remarks>
        /// <param name="query">The criteria by which to fetch documents from the database</param>
        /// <param name="output">The stream to feed the results into</param>
        void StreamInto<T>(IDocumentQuery<T> query, Stream output);

        /// <inheritdoc cref="StreamInto{T}(IDocumentQuery{T}, Stream) "/>
        void StreamInto<T>(IRawDocumentQuery<T> query, Stream output);
    }

    public interface ITimeSeriesSessionStreamOperations<T>
    {
        IEnumerator<TimeSeriesStreamResult<T>> Stream(IQueryable<T> query);

        IEnumerator<TimeSeriesStreamResult<T>> Stream(IQueryable<T> query, out StreamQueryStatistics streamQueryStats);

        IEnumerator<TimeSeriesStreamResult<T>> Stream(IDocumentQuery<T> query);

        IEnumerator<TimeSeriesStreamResult<T>> Stream(IRawDocumentQuery<T> query);
        
        IEnumerator<TimeSeriesStreamResult<T>> Stream(IRawDocumentQuery<T> query, out StreamQueryStatistics streamQueryStats);

        IEnumerator<TimeSeriesStreamResult<T>> Stream(IDocumentQuery<T> query, out StreamQueryStatistics streamQueryStats);
    }

    public interface ITimeSeriesSessionStreamAggregationResultOperations
    {
        IEnumerator<TimeSeriesStreamResult<TimeSeriesAggregationResult<T>>> Stream<T>(IQueryable<TimeSeriesAggregationResult<T>> query) where T : new();

        IEnumerator<TimeSeriesStreamResult<TimeSeriesAggregationResult<T>>> Stream<T>(IQueryable<TimeSeriesAggregationResult<T>> query, out StreamQueryStatistics streamQueryStats) where T : new();

        IEnumerator<TimeSeriesStreamResult<TimeSeriesAggregationResult<T>>> Stream<T>(IDocumentQuery<TimeSeriesAggregationResult<T>> query) where T : new();

        IEnumerator<TimeSeriesStreamResult<TimeSeriesAggregationResult<T>>> Stream<T>(IRawDocumentQuery<TimeSeriesAggregationResult<T>> query) where T : new();
        
        IEnumerator<TimeSeriesStreamResult<TimeSeriesAggregationResult<T>>> Stream<T>(IRawDocumentQuery<TimeSeriesAggregationResult<T>> query, out StreamQueryStatistics streamQueryStats) where T : new();

        IEnumerator<TimeSeriesStreamResult<TimeSeriesAggregationResult<T>>> Stream<T>(IDocumentQuery<TimeSeriesAggregationResult<T>> query, out StreamQueryStatistics streamQueryStats) where T : new();
    }

    public interface ITimeSeriesSessionStreamRawResultOperations
    {
        IEnumerator<TimeSeriesStreamResult<TimeSeriesRawResult<T>>> Stream<T>(IQueryable<TimeSeriesRawResult<T>> query) where T : new();

        IEnumerator<TimeSeriesStreamResult<TimeSeriesRawResult<T>>> Stream<T>(IQueryable<TimeSeriesRawResult<T>> query, out StreamQueryStatistics streamQueryStats) where T : new();

        IEnumerator<TimeSeriesStreamResult<TimeSeriesRawResult<T>>> Stream<T>(IDocumentQuery<TimeSeriesRawResult<T>> query) where T : new();

        IEnumerator<TimeSeriesStreamResult<TimeSeriesRawResult<T>>> Stream<T>(IRawDocumentQuery<TimeSeriesRawResult<T>> query) where T : new();
        
        IEnumerator<TimeSeriesStreamResult<TimeSeriesRawResult<T>>> Stream<T>(IRawDocumentQuery<TimeSeriesRawResult<T>> query, out StreamQueryStatistics streamQueryStats) where T : new();

        IEnumerator<TimeSeriesStreamResult<TimeSeriesRawResult<T>>> Stream<T>(IDocumentQuery<TimeSeriesRawResult<T>> query, out StreamQueryStatistics streamQueryStats) where T : new();
    }
}
