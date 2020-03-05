//-----------------------------------------------------------------------
// <copyright file="ISessionDocumentTimeSeriesBase.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    ///     Time series advanced in memory session operations
    /// </summary>
    public interface ISessionDocumentTimeSeriesBase
    {
        /// <summary>
        /// Append the the values (and optional tag) to the times series at the provided time stamp
        /// </summary>
        void Append(DateTime timestamp, IEnumerable<double> values, string tag = null);

        /// <summary>
        /// Append a single value (and optional tag) to the times series at the provided time stamp
        /// </summary>
        void Append(DateTime timestamp, double value, string tag = null);

        /// <summary>
        /// Remove all the values in the time series in the range of from .. to.
        /// </summary>
        void Remove(DateTime from, DateTime to);

        /// <summary>
        /// Remove the value in the time series in the specified time stamp
        /// </summary>
        void Remove(DateTime at);

    }
}
