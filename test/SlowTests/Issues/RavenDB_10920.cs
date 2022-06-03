using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes.Spatial;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_10920 : RavenTestBase
    {
        public RavenDB_10920(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void CanOverrideDefaultUnitsWhenWktIsUsed(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Point
                    {
                        Latitude = 10,
                        Longitude = 10
                    });

                    session.Store(new Point
                    {
                        Latitude = 10.1,
                        Longitude = 10.1
                    });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query0 = session.Query<Point>()
                        .Spatial(f => f.Point(x => x.Latitude, x => x.Longitude), f => f.RelatesToShape("CIRCLE(10 10 d=100)", SpatialRelation.Within));

                    var iq = RavenTestHelper.GetIndexQuery(query0);
                    Assert.Equal("from 'Points' where spatial.within(spatial.point(Latitude, Longitude), spatial.wkt($p0))", iq.Query);

                    var query1 = session.Query<Point>()
                        .Spatial(f => f.Point(x => x.Latitude, x => x.Longitude), f => f.RelatesToShape("CIRCLE(10 10 d=10)", SpatialRelation.Within, SpatialUnits.Miles));

                    iq = RavenTestHelper.GetIndexQuery(query1);
                    Assert.Equal("from 'Points' where spatial.within(spatial.point(Latitude, Longitude), spatial.wkt($p0, 'Miles'))", iq.Query);

                    var results1 = query1.ToList();

                    var query2 = session.Query<Point>()
                        .Spatial(f => f.Point(x => x.Latitude, x => x.Longitude), f => f.RelatesToShape("CIRCLE(10 10 d=10)", SpatialRelation.Within, SpatialUnits.Kilometers));

                    iq = RavenTestHelper.GetIndexQuery(query2);
                    Assert.Equal("from 'Points' where spatial.within(spatial.point(Latitude, Longitude), spatial.wkt($p0, 'Kilometers'))", iq.Query);

                    var results2 = query2.ToList();

                    Assert.Equal(2, results1.Count);
                    Assert.Equal(1, results2.Count);
                }
            }
        }

        private class Point
        {
            public string Id { get; set; }

            public double Latitude { get; set; }

            public double Longitude { get; set; }
        }
    }
}
