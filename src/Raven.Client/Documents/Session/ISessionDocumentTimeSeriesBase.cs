//-----------------------------------------------------------------------
// <copyright file="ISessionDocumentCountersBase.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;

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
        void Append(string timeseries, DateTime timestamp, string tag, double[] values);

        /// <summary>
        /// Remove all the values in the timeseries in the range of from .. to.
        /// </summary>
        /// <param name="counter">the counter name</param>
        void Remove(string timeseries, DateTime from, DateTime to);

    }
}
