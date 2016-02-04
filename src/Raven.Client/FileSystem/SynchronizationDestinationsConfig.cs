// -----------------------------------------------------------------------
//  <copyright file="SynchronizationDocument.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;

namespace Raven.Abstractions.FileSystem
{
    public class SynchronizationDestinationsConfig
    {
        /// <summary>
        /// Gets or sets the list of synchronization destinations.
        /// </summary>
        public List<SynchronizationDestination> Destinations { get; set; }

        public SynchronizationDestinationsConfig()
        {
            Destinations = new List<SynchronizationDestination>();
        }
    }
}
