using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Spatial;
using Raven.Client.Documents.Session;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_3646 : RavenTestBase
    {
        public RavenDB_3646(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void QueryWithCustomize()
        {
            using (var store = GetDocumentStore())
            {
                StoreData(store);
                store.ExecuteIndex(new Events_SpatialIndex());
                Indexes.WaitForIndexing(store);
                using (var session = store.OpenSession())
                {
                    QueryStatistics stats;
                    var rq = session.Query<Events_SpatialIndex.ReduceResult, Events_SpatialIndex>()
                        .Statistics(out stats)
                        .Spatial(x => x.Coordinates, x => x.WithinRadius(10000, 1, 1, SpatialUnits.Miles))
                        .OfType<Events_SpatialIndex.ReduceResult>();

                    var t = 0;

                    using (var enumerator = session.Advanced.Stream(rq.ProjectInto<Event>()))
                    {
                        while (enumerator.MoveNext())
                        {
                            t++;
                        }
                    }

                    Assert.Equal(300, t);
                }
            }
        }

        [Fact]
        public void QueryWithoutCustomize()
        {
            using (var store = GetDocumentStore())
            {
                StoreData(store);
                store.ExecuteIndex(new Events_SpatialIndex());
                Indexes.WaitForIndexing(store);
                using (var session = store.OpenSession())
                {
                    QueryStatistics stats;
                    var rq = session.Query<Events_SpatialIndex.ReduceResult, Events_SpatialIndex>()
                        .Statistics(out stats);

                    using (var enumerator = session.Advanced.Stream(rq.ProjectInto<Event>()))
                    {
                        var t = 0;
                        while (enumerator.MoveNext())
                        {
                            t++;
                        }
                        Assert.Equal(300, t);
                    }
                }
            }
        }

        private class Events_SpatialIndex : AbstractIndexCreationTask<Event, Events_SpatialIndex.ReduceResult>
        {
            public class ReduceResult
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


                SpatialIndexesStrings.Add("Coordinates", new SpatialOptions());
            }
        }

        private class Event
        {
            public string Name { get; set; }
            public double Latitude { get; set; }
            public double Longitude { get; set; }
        }

        private void StoreData(IDocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                for (int i = 0; i < 300; i++)
                    session.Store(new Event { Name = "e1" + i, Latitude = 1, Longitude = 1 });
                session.SaveChanges();
            }
        }
    }
}
