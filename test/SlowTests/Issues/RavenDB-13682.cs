using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using FastTests;
using FastTests.Server.Basic.Entities;
using Raven.Client.Documents;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_13682 : RavenTestBase
    {
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
