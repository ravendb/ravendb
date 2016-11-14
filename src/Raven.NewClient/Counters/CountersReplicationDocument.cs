//-----------------------------------------------------------------------
// <copyright file="ReplicationDocument.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;

namespace Raven.NewClient.Abstractions.Counters
{
    /// <summary>
    /// This class represent the list of replication destinations for the server
    /// </summary>
    public class CountersReplicationDocument
    {
        /// <summary>
        /// Gets or sets the list of replication destinations.
        /// </summary>
        public List<CounterReplicationDestination> Destinations { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="CountersReplicationDocument"/> class.
        /// </summary>
        public CountersReplicationDocument()
        {
            Destinations = new List<CounterReplicationDestination>();
        }
    }
}
