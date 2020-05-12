//-----------------------------------------------------------------------
// <copyright file="ISessionDocumentTimeSeries.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
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
        IEnumerable<TimeSeriesEntry> Get(DateTime? from = null, DateTime? to = null, int start = 0, int pageSize = int.MaxValue);

    }

    /// <summary>
    ///     Time series typed synchronous session operations
    /// </summary>
    public interface ISessionDocumentTypedTimeSeries<TValues> : ISessionDocumentTypedAppendTimeSeriesBase<TValues>, ISessionDocumentRemoveTimeSeriesBase  where TValues : TimeSeriesEntry
    {
        /// <summary>
        /// Return the time series values for the provided range
        /// </summary>
        IEnumerable<TValues> Get(DateTime? from = null, DateTime? to = null, int start = 0, int pageSize = int.MaxValue);
    }
}
