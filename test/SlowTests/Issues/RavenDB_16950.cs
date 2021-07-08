using Xunit;
using FastTests;
using Tests.Infrastructure;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16950 : RavenTestBase
    {
        public RavenDB_16950(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CheckIfDecimalValueNotChangesAfterComputation()
        {
            int changesCounter;
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new CreateSampleDataOperation());
                using (var session = store.OpenSession())
                {
                    Order order = session.Load<Order>("orders/823-A");
                    order.ShipTo.Location.Latitude = order.ShipTo.Location.Latitude;
                    var changes = session.Advanced.WhatChanged();
                    changesCounter = changes.Count;
                }
            }
            Assert.Equal(0, changesCounter);
        }

        private class Address
        {
            public Location Location { get; set; }
        }

        private class Location
        {
            public double Latitude { get; set; }
            public double Longitude { get; set; }
        }

        private class Order
        {
            public string Id { get; set; }
            public Address ShipTo { get; set; }
        }
    }
}
