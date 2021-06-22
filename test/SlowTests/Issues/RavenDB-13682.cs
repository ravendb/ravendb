using System;
using System.Linq;
using FastTests;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13682 : RavenTestBase
    {
        public RavenDB_13682(ITestOutputHelper output) : base(output)
        {
        }

        private class Item
        {
            public double Lat, Lng;
            public string Name;
        }

        [Fact]
        public void CanQueryByRoundedSpatialRanges()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    // 35.1, -106.3 - destination
                    s.Store(new Item { Lat = 35.1, Lng = -107.1, Name = "a" }); // 3rd dist - 72.7 km
                    s.Store(new Item { Lat = 35.2, Lng = -107.0, Name = "b" }); // 2nd dist - 64.04 km
                    s.Store(new Item { Lat = 35.3, Lng = -106.5, Name = "c" }); // 1st dist - 28.71 km
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    // we sort first by spatial distance (but round it up to 25km)
                    // then we sort by name ascending, so within 25 range, we can apply a different sort

                    var result = s.Advanced.RawQuery<Item>(@"from Items  as a
order by spatial.distance(spatial.point(a.Lat, a.Lng), spatial.point(35.1, -106.3), 25), Name")
                        .ToList();

                    Assert.Equal(3, result.Count);
                    Assert.Equal("c", result[0].Name);
                    Assert.Equal("a", result[1].Name);
                    Assert.Equal("b", result[2].Name);
                }

                // dynamic query
                using (var s = store.OpenSession())
                {
                    // we sort first by spatial distance (but round it up to 25km)
                    // then we sort by name ascending, so within 25 range, we can apply a different sort

                    var query = s.Query<Item>()
                        .OrderByDistance(spatial => spatial.Point(item => item.Lat, item => item.Lng).RoundTo(25), 35.1, -106.3);
                    var result = query.ToList();

                    Assert.Equal(3, result.Count);
                    Assert.Equal("c", result[0].Name);
                    Assert.Equal("a", result[1].Name);
                    Assert.Equal("b", result[2].Name);
                }

                new SpatialIndex().Execute(store);
                WaitForIndexing(store);

                using (var s = store.OpenSession())
                {
                    // we sort first by spatial distance (but round it up to 25km)
                    // then we sort by name ascending, so within 25 range, we can apply a different sort

                    var query = s.Query<Item, SpatialIndex>()
                        .OrderByDistance("Coordinates", 35.1, -106.3, 25);
                    var result = query.ToList();

                    Assert.Equal(3, result.Count);
                    Assert.Equal("c", result[0].Name);
                    Assert.Equal("a", result[1].Name);
                    Assert.Equal("b", result[2].Name);
                }

                using (var s = store.OpenSession())
                {
                    // we sort first by spatial distance (but round it up to 25km)
                    // then we sort by name ascending, so within 25 range, we can apply a different sort

                    var query = s.Advanced.DocumentQuery<Item, SpatialIndex>()
                        .OrderByDistance("Coordinates", 35.1, -106.3, 25);
                    var result = query.ToList();

                    Assert.Equal(3, result.Count);
                    Assert.Equal("c", result[0].Name);
                    Assert.Equal("a", result[1].Name);
                    Assert.Equal("b", result[2].Name);
                }
            }
        }

        private class SpatialIndex : AbstractIndexCreationTask<Item>
        {
            public SpatialIndex()
            {
                Map =
                    entities =>
                    from e in entities
                    select new
                    {
                        e.Name,
                        Coordinates = CreateSpatialField(e.Lat, e.Lng)
                    };
            }
        }

        [Fact]
        public void CanUseDynamicQueryOrderBySpatial_WithAlias()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new CreateSampleDataOperation());

                using (var s = store.OpenSession())
                {
                    var d = s.Advanced.RawQuery<Order>(@"from Orders  as a
order by spatial.distance(
    spatial.point(a.ShipTo.Location.Latitude, a.ShipTo.Location.Longitude),
    spatial.point(35.2, -107.2 )
)
limit 1")
                        .Single();

                    var metadata = s.Advanced.GetMetadataFor(d);

                    Assert.Equal(48.99, Math.Round((double)metadata.GetObject("@spatial")["Distance"], 2));
                }
            }
        }

        [Fact]
        public void CanUseDynamicQueryOrderBySpatial()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new CreateSampleDataOperation());

                using (var s = store.OpenSession())
                {
                    var d = s.Advanced.RawQuery<Order>(@"from Orders
order by spatial.distance(
    spatial.point(ShipTo.Location.Latitude, ShipTo.Location.Longitude),
    spatial.point(35.2, -107.2 )
)
limit 1")
                        .Single();

                    var metadata = s.Advanced.GetMetadataFor(d);

                    Assert.Equal(48.99, Math.Round((double)metadata.GetObject("@spatial")["Distance"], 2));
                }
            }
        }

        [Fact]
        public void CanProjectDistanceComputation()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new CreateSampleDataOperation());

                using (var s = store.OpenSession())
                {
                    var d = s.Advanced.RawQuery<JObject>(@"from Orders  as a
where id() ='orders/830-A'
select id(), spatial.distance(35.2, -107.2 , a.ShipTo.Location.Latitude, a.ShipTo.Location.Longitude, 'kilometers') as Distance
limit 1")
                        .Single();

                    Assert.Equal(48.99, Math.Round(d.Value<double>("Distance"), 2));
                }

                using (var s = store.OpenSession())
                {
                    var d = s.Advanced.RawQuery<JObject>(@"from Orders  as a
where id() ='orders/830-A'
order by spatial.distance(
    spatial.point(a.ShipTo.Location.Latitude, a.ShipTo.Location.Longitude),
    spatial.point(35.2, -107.2 )
)
select {
    Id : id(a),
    D: getMetadata(a)['@distance'],
    Distance: spatial.distance(35.2, -107.2, a.ShipTo.Location.Latitude, a.ShipTo.Location.Longitude)
}")
                        .Single();

                    Assert.Equal(48.99, Math.Round(d.Value<double>("Distance"), 2));
                }
            }
        }

        [Fact]
        public void CanGetDistanceFromSpatialQuery()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new CreateSampleDataOperation(Raven.Client.Documents.Smuggler.DatabaseItemType.Documents | Raven.Client.Documents.Smuggler.DatabaseItemType.Indexes));

                WaitForIndexing(store);

                using (var s = store.OpenSession())
                {
                    var d = s.Query<Order>("Orders/ByShipment/Location")
                        .Where(x => x.Id == "orders/830-A")
                        .OrderByDistance("ShipmentLocation", 35.2, -107.1)
                        .Single();

                    var metadata = s.Advanced.GetMetadataFor(d);

                    Assert.Equal(40.1, Math.Round((double)metadata.GetObject("@spatial")["Distance"], 1));
                }
            }
        }
    }
}
