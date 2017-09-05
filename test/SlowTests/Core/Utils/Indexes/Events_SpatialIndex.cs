// -----------------------------------------------------------------------
//  <copyright file="QueryResultsStreaming.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// ---------------------------------------------------------------------

using System.Linq;
using Raven.Client.Documents.Indexes;
using Event = SlowTests.Core.Utils.Entities.Event;

namespace SlowTests.Core.Utils.Indexes
{
    public class Events_SpatialIndex : AbstractIndexCreationTask<Event>
    {
        public class Result
        {
            public string Name { get; set; }

            public string Coordinates { get; set; }
        }

        public Events_SpatialIndex()
        {
            Map = events => from e in events
                            select new
                            {
                                Name = e.Name,
                                Coordinates = CreateSpatialField(e.Latitude, e.Longitude)
                            };
        }
    }
}
