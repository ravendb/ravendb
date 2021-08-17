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
        /// Append the the values (and optional tag) to the times series at the provided time stamp
        /// </summary>
        void Append(DateTime timestamp, IEnumerable<double> values, string tag = null);

        /// <summary>
        /// Append a single value (and optional tag) to the times series at the provided time stamp
        /// </summary>
        void Append(DateTime timestamp, double value, string tag = null);
    }

    public interface ISessionDocumentTypedAppendTimeSeriesBase<T> where T : new()
    {
        void Append(DateTime timestamp, T entry, string tag = null);
        void Append(TimeSeriesEntry<T> entry);
    }

    public interface ISessionDocumentRollupTypedAppendTimeSeriesBase<T> where T : new()
    {
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

    public interface ITimeSeriesStreamingBase<out T>
    {
        IEnumerator<T> Stream(DateTime? from = null, DateTime? to = null, TimeSpan? offset = null);
    }

    public interface IAsyncTimeSeriesStreamingBase<T>
    {
        Task<IAsyncEnumerator<T>> StreamAsync(DateTime? from = null, DateTime? to = null, TimeSpan? offset = null, CancellationToken token = default);
    }
}
