//-----------------------------------------------------------------------
// <copyright file="ISessionDocumentTimeSeriesBase.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Session.TimeSeries;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    ///     Time series advanced in memory session operations
    /// </summary>
    public interface ISessionDocumentAppendTimeSeriesBase
    {
        /// <summary>
        /// Appends values (and an optional tag) to the time series at the provided timestamp.
        /// </summary>
        /// <remarks>
        /// For more information on the Time Series Append operation, see: <inheritdoc cref="DocumentationUrls.Session.TimeSeries.AppendOperation"/>.
        /// </remarks>
        /// <param name="timestamp">The timestamp at which the values should be appended to the time series.</param>
        /// <param name="values">An enumerable collection of values to append to the time series.</param>
        /// <param name="tag">An optional tag to associate with the appended values. Tags can be used to categorize or label the data.</param>
        void Append(DateTime timestamp, IEnumerable<double> values, string tag = null);

        /// <inheritdoc cref="Append"/>
        /// <param name="value">The value to append to the time series.</param>
        void Append(DateTime timestamp, double value, string tag = null);
    }

    public interface ISessionDocumentTypedAppendTimeSeriesBase<T> where T : new()
    {
        /// <summary>
        /// Appends values (and an optional tag) to the time series at the provided timestamp.
        /// </summary>
        /// <remarks>
        /// For more information on the Time Series Append operation, see: <inheritdoc cref="DocumentationUrls.Session.TimeSeries.AppendOperation"/>. <br/>
        /// For details about named time series values, refer to: <inheritdoc cref="DocumentationUrls.Session.TimeSeries.NamedValues"/>.
        /// </remarks>
        /// <param name="timestamp">The timestamp at which the values should be appended to the time series.</param>
        /// <param name="entry">An entry representing the typed values to append to the time series (<typeparamref name="T"/> indicates the value type).</param>
        /// <param name="tag">An optional tag to associate with the appended values. Tags can be used to categorize or label the data.</param>
        void Append(DateTime timestamp, T entry, string tag = null);

        /// <summary>
        /// Appends values (and an optional tag) to the time series at the provided timestamp.
        /// </summary>
        /// <remarks>
        /// For more information on the Time Series Append operation, see: <inheritdoc cref="DocumentationUrls.Session.TimeSeries.AppendOperation"/>. <br/>
        /// For details about named time series values, refer to: <inheritdoc cref="DocumentationUrls.Session.TimeSeries.NamedValues"/>.
        /// </remarks>
        /// <param name="entry">The time series arguments to append (timestamp, values, optional tag). <br/>
        /// <see cref="TimeSeriesEntry"/> is an object that aggregates the time series inputs, and <typeparamref name="T"/> indicates the value type.</param>
        void Append(TimeSeriesEntry<T> entry);
    }

    public interface ISessionDocumentRollupTypedAppendTimeSeriesBase<T> where T : new()
    {
        /// <summary>
        /// Appends rollup values to the time series at the provided timestamp.
        /// </summary>
        /// <remarks>
        /// For more information on the Time Series Append operation, see: <inheritdoc cref="DocumentationUrls.Session.TimeSeries.AppendOperation"/>. <br/>
        /// For details about named time series values, refer to: <inheritdoc cref="DocumentationUrls.Session.TimeSeries.NamedValues"/>.
        /// </remarks>
        /// <param name="entry">The time series arguments to append (timestamp and rollup values). <br/>
        /// <see cref="TimeSeriesRollupEntry{TValues}"/> is an extension of <see cref="TimeSeriesEntry"/>
        /// which contains the aggregated values of rollup time series [Min, Max, First, Last, Sum, Count, Average].</param>
        void Append(TimeSeriesRollupEntry<T> entry);
    }

    public interface ISessionDocumentDeleteTimeSeriesBase
    {
        /// <summary>
        /// Deletes a range of entries from a single time series.
        /// </summary>
        /// <remarks>
        /// For more information on the Time Series Delete operation, see: <inheritdoc cref="DocumentationUrls.Session.TimeSeries.DeleteOperation"/>.
        /// </remarks>
        /// <param name="from">The date and time from which to start deleting time series entries (inclusive). If not specified, the deletion will start from the earliest possible date and time (DateTime.MinValue).</param>
        /// <param name="to">The date and time indicating the end of the range for deleting time series entries (inclusive). If not specified, the deletion will continue until the latest possible date and time (DateTime.MaxValue).</param>
        void Delete(DateTime? from = null, DateTime? to = null);

        /// <inheritdoc cref="Delete"/>
        /// <param name="at">The specific date and time at which to delete the time series entry.</param>
        void Delete(DateTime at);
    }

    public interface ISessionDocumentIncrementTimeSeriesBase
    {
        /// <summary>
        /// Increments the values of the incremental time series at the provided timestamp. <br/>
        /// If no existing values are present, this method behaves as if setting the values.
        /// </summary>
        /// <remarks>
        /// For more information on the Time Series Increment operation, see: <inheritdoc cref="DocumentationUrls.Session.TimeSeries.IncrementOperation"/>.
        /// </remarks>
        /// <param name="timestamp">The timestamp at which to apply the increment to the original values of the incremental time series.</param>
        /// <param name="values">An enumerable collection of values indicating the deltas to increment the original values of the incremental time series.</param>
        void Increment(DateTime timestamp, IEnumerable<double> values);

        /// <summary><inheritdoc cref="Increment"/></summary>
        /// <remarks>
        /// Note that because there is no provided timestamp, the method will use the current date and time (DateTime.UtcNow) for the operation. <br/>
        /// <inheritdoc cref="Increment"/>
        /// </remarks>
        void Increment(IEnumerable<double> values);

        /// <inheritdoc cref = "Increment" />
        /// <param name="value">Indicating the delta to increment the original value of the incremental time series.</param>
        void Increment(DateTime timestamp, double value);

        /// <inheritdoc cref = "Increment(IEnumerable{double})" />
        /// <param name="value">Indicating the delta to increment the original value of the incremental time series.</param>
        void Increment(double value);
    }

    public interface ISessionDocumentTypedIncrementTimeSeriesBase<T> where T : new()
    {
        /// <summary>
        /// Increments the values of the incremental time series at the provided timestamp using the specified entry.
        /// </summary>
        /// <remarks>
        /// For more information on the Time Series Increment operation, see: <inheritdoc cref="DocumentationUrls.Session.TimeSeries.IncrementOperation"/>.<br/>
        /// For details about named time series values, refer to: <inheritdoc cref="DocumentationUrls.Session.TimeSeries.NamedValues"/>.
        /// </remarks>
        /// <param name="timestamp">The timestamp at which to apply the increment to the original values of the incremental time series.</param>
        /// <param name="entry">The entry representing the values to increment the original values of the incremental time series.</param>
        void Increment(DateTime timestamp, T entry);

        /// <inheritdoc cref = "Increment" />
        void Increment(T entry);
    }

    public interface ITimeSeriesStreamingBase<out T>
    {
        /// <summary>
        ///  Creates a stream to fetch time series entries within the specified time range.
        /// </summary>
        /// <param name="from">The starting timestamp for fetching time series entries (inclusive). If not specified, will start from the earliest possible date and time (DateTime.MinValue).</param>
        /// <param name="to">The ending timestamp for fetching time series entries (inclusive). If not specified, will continue until the latest possible date and time (DateTime.MaxValue).</param>
        /// <param name="offset">The offset to apply to the timestamps when fetching entries. If not specified, considered as 0.</param>
        /// <returns>An enumerator used to iterate over the time series entries received via the stream.</returns>
        IEnumerator<T> Stream(DateTime? from = null, DateTime? to = null, TimeSpan? offset = null);
    }

    public interface IAsyncTimeSeriesStreamingBase<T>
    {
        /// <inheritdoc cref = "ITimeSeriesStreamingBase{T}.Stream" />
        Task<IAsyncEnumerator<T>> StreamAsync(DateTime? from = null, DateTime? to = null, TimeSpan? offset = null, CancellationToken token = default);
    }
}
