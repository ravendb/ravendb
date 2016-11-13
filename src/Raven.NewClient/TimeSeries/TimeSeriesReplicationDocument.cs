//-----------------------------------------------------------------------
// <copyright file="ReplicationDocument.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;

namespace Raven.Abstractions.TimeSeries
{
    /// <summary>
    /// This class represent the list of replication destinations for the server
    /// </summary>
    public class TimeSeriesReplicationDocument
    {
        /// <summary>
        /// Gets or sets the list of replication destinations.
        /// </summary>
        public List<TimeSeriesReplicationDestination> Destinations { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="TimeSeriesReplicationDocument"/> class.
        /// </summary>
        public TimeSeriesReplicationDocument()
        {
            Destinations = new List<TimeSeriesReplicationDestination>();
        }
    }
}
