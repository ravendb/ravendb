// -----------------------------------------------------------------------
//  <copyright file="QueryResultsStreaming.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// ---------------------------------------------------------------------
using Raven.Client.Indexes;
using Raven.Tests.Core.Utils.Entities;
using System.Linq;

namespace Raven.Tests.Core.Utils.Indexes
{
    public class Events_SpatialIndex : AbstractIndexCreationTask<Event>
    {
        public Events_SpatialIndex()
        {
            Map = events => from e in events
                            select new
                            {
                                Name = e.Name,
                                __ = SpatialGenerate("Coordinates", e.Latitude, e.Longitude)
                            };
        }
    }
}
