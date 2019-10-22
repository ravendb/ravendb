using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_9676 : RavenTestBase
    {
        public RavenDB_9676(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanOrderByDistanceOnDynamicSpatialField()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Item
                    {
                        Name = "Item1",
                        Latitude = 10,
                        Longitude = 10
                    });

                    session.Store(new Item
                    {
                        Name = "Item2",
                        Latitude = 11,
                        Longitude = 11
                    });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var items = session.Query<Item>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Spatial(factory => factory.Point(item => item.Latitude, item => item.Longitude), factory => factory.WithinRadius(1000, 10, 10))
                        .OrderByDistance(factory => factory.Point(item => item.Latitude, item => item.Longitude), 10, 10)
                        .ToList();

                    Assert.Equal(2, items.Count);
                    Assert.Equal("Item1", items[0].Name);
                    Assert.Equal("Item2", items[1].Name);

                    items = session.Query<Item>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Spatial(factory => factory.Point(item => item.Latitude, item => item.Longitude), factory => factory.WithinRadius(1000, 10, 10))
                        .OrderByDistanceDescending(factory => factory.Point(item => item.Latitude, item => item.Longitude), 10, 10)
                        .ToList();

                    Assert.Equal(2, items.Count);
                    Assert.Equal("Item2", items[0].Name);
                    Assert.Equal("Item1", items[1].Name);
                }

                using (var session = store.OpenSession())
                {
                    var items = session.Advanced.DocumentQuery<Item>()
                        .Spatial(factory => factory.Point(item => item.Latitude, item => item.Longitude), factory => factory.WithinRadius(1000, 10, 10))
                        .OrderByDistance(factory => factory.Point(item => item.Latitude, item => item.Longitude), 10, 10)
                        .ToList();

                    Assert.Equal(2, items.Count);
                    Assert.Equal("Item1", items[0].Name);
                    Assert.Equal("Item2", items[1].Name);

                    items = session.Advanced.DocumentQuery<Item>()
                        .Spatial(factory => factory.Point(item => item.Latitude, item => item.Longitude), factory => factory.WithinRadius(1000, 10, 10))
                        .OrderByDistanceDescending(factory => factory.Point(item => item.Latitude, item => item.Longitude), 10, 10)
                        .ToList();

                    Assert.Equal(2, items.Count);
                    Assert.Equal("Item2", items[0].Name);
                    Assert.Equal("Item1", items[1].Name);
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
