using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes.Spatial;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_11214 : RavenTestBase
    {
        [Fact]
        public void BetterExceptionWhenDynamicSpatialFieldIsUsedOnStaticIndexQuery()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var e = Assert.Throws<InvalidOperationException>(() => session.Query<Location>("Locations")
                        .Spatial(f => f.Point(x => x.Latitude, x => x.Longitude), c => c.WithinRadius(30, 32.56829122491778, 34.953954053921734, SpatialUnits.Kilometers))
                        .Single(x => x.Description == "Dor beach"));

                    Assert.Contains("Cannot execute query method 'Spatial'", e.Message);
                }
            }
        }

        private class Location
        {
            public string Description { get; set; }
            public double Longitude { get; set; }
            public double Latitude { get; set; }
        }
    }
}
