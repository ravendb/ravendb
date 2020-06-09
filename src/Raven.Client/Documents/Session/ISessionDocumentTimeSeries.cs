//-----------------------------------------------------------------------
// <copyright file="ISessionDocumentTimeSeries.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using Raven.Client.Documents.Session.TimeSeries;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    ///     Time series synchronous session operations
    /// </summary>
    public interface ISessionDocumentTimeSeries : ISessionDocumentAppendTimeSeriesBase, ISessionDocumentRemoveTimeSeriesBase
    {
        /// <summary>
        /// Return the time series values for the provided range
        /// </summary>
        TimeSeriesEntry[] Get(DateTime? from = null, DateTime? to = null, int start = 0, int pageSize = int.MaxValue);
    }

    /// <summary>
    ///     Time series typed synchronous session operations
    /// </summary>
    public interface ISessionDocumentTypedTimeSeries<TValues> : ISessionDocumentTypedAppendTimeSeriesBase<TValues>, ISessionDocumentRemoveTimeSeriesBase where TValues : new()
    {
        /// <summary>
        /// Return the time series values for the provided range
        /// </summary>
        TimeSeriesEntry<TValues>[] Get(DateTime? from = null, DateTime? to = null, int start = 0, int pageSize = int.MaxValue);
    }

    /// <summary>
    ///     Time series typed synchronous session operations
    /// </summary>
    public interface ISessionDocumentRollupTypedTimeSeries<TValues> : ISessionDocumentRollupTypedAppendTimeSeriesBase<TValues>, ISessionDocumentRemoveTimeSeriesBase where TValues : new()
    {
        /// <summary>
        /// Return the time series values for the provided range
        /// </summary>
        TimeSeriesRollupEntry<TValues>[] Get(DateTime? from = null, DateTime? to = null, int start = 0, int pageSize = int.MaxValue);
    }
}
