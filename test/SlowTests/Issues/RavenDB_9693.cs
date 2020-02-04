using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_9693 : RavenTestBase
    {
        public RavenDB_9693(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void LinqOrderByDistanceShouldGenerateQueryProperly()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var query = session.Query<Item>()
                        .Where(x => x.Name == "John")
                        .Spatial(factory => factory.Point(x => x.Latitude, x => x.Longitude), factory => factory.WithinRadius(10, 10, 10))
                        .OrderByDistanceDescending("someField", 10, 10);

                    var iq = RavenTestHelper.GetIndexQuery(query);

                    Assert.Equal("from 'Items' where Name = $p0 and spatial.within(spatial.point(Latitude, Longitude), spatial.circle($p1, $p2, $p3)) order by spatial.distance(someField, spatial.point($p4, $p5)) desc", iq.Query);
                }
            }
        }

        private class Item
        {
            public string Name { get; set; }

            public double Latitude { get; set; }

            public double Longitude { get; set; }
        }
    }
}
