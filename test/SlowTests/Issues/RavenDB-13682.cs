using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using FastTests;
using FastTests.Server.Basic.Entities;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_13682 : RavenTestBase
    {
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

                    Assert.Equal(48.99, Math.Round((double)metadata["@distance"], 2));
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

                    Assert.Equal(48.99, Math.Round((double)metadata["@distance"], 2));
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
            using(var store = GetDocumentStore())
            {
                store.Maintenance.Send(new CreateSampleDataOperation());

                WaitForIndexing(store);

                using (var s = store.OpenSession())
                {
                    var d = s.Query<Order>("Orders/ByShipment/Location")
                        .Where(x=>x.Id == "orders/830-A")
                        .OrderByDistance("ShipmentLocation", 35.2, -107.1)
                        .Single();

                    var metadata = s.Advanced.GetMetadataFor(d);

                    Assert.Equal(40.1, Math.Round((double)metadata["@distance"], 1));
                }

            }
        }
    }
}
