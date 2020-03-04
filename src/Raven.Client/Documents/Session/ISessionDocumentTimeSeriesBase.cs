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
        /// Append the the values (and tag) to the times series at the provided timstamp
        /// </summary>
        void Append(DateTime timestamp, string tag, IEnumerable<double> values);

        /// <summary>
        /// Remove all the values in the timeseries in the range of from .. to.
        /// </summary>
        void Remove(DateTime from, DateTime to);

        /// <summary>
        /// Remove the value in the timeseries in the specified timestamp
        /// </summary>
        void Remove(DateTime at);

    }
}
