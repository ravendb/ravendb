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
        /// <typeparamref name="TimeSeriesEntry"/> is an object that aggregates the time series inputs, and <typeparamref name="T"/> indicates the value type.</param>
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
        /// <typeparamref name="TimeSeriesRollupEntry"/> is an extension of <typeparamref name="TimeSeriesEntry"/>
        /// which contains the aggregated values of rollup time series [Min, Max, First, Last, Sum, Count, Average].</param>
        void Append(TimeSeriesRollupEntry<T> entry);
    }

    public interface ISessionDocumentDeleteTimeSeriesBase
    {
        /// <summary>
        /// Delete all the values in the time series in the range of from .. to.
        /// </summary>
        void Delete(DateTime? from = null, DateTime? to = null);

        /// <summary>
        /// Delete the value in the time series in the specified time stamp
        /// </summary>
        void Delete(DateTime at);

    }

    public interface ISessionDocumentIncrementTimeSeriesBase
    {
        /// <summary>
        /// Increment the value of the times series at the provided time stamp
        /// </summary>
        void Increment(DateTime timestamp, IEnumerable<double> values);

        void Increment(IEnumerable<double> values);

        void Increment(DateTime timestamp, double value);

        void Increment(double value);

    }

    public interface ISessionDocumentTypedIncrementTimeSeriesBase<T> where T : new()
    {
        void Increment(DateTime timestamp, T entry);

        void Increment(T entry);

    }

    public interface ITimeSeriesStreamingBase<out T>
    {
        IEnumerator<T> Stream(DateTime? from = null, DateTime? to = null, TimeSpan? offset = null);
    }

    public interface IAsyncTimeSeriesStreamingBase<T>
    {
        Task<IAsyncEnumerator<T>> StreamAsync(DateTime? from = null, DateTime? to = null, TimeSpan? offset = null, CancellationToken token = default);
    }
}
