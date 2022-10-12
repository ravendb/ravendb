using System;
using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_19453 : RavenTestBase
{
    public RavenDB_19453(ITestOutputHelper output) : base(output)
    {
    }

    private class Item
    {
        public double Lat, Lng;
    }

    [Fact]
    public void CanGetSpatialDistanceFromJavaScriptProjection()
    {
        using var store = GetDocumentStore();

        using (var s = store.OpenSession())
        {
            s.Store(new Item{Lng = 10, Lat = 10});
            s.SaveChanges();
        }


        using (var s = store.OpenSession())
        {
            var i = s.Advanced.RawQuery<dynamic>($@"
from Items as i
order by spatial.distance(
    spatial.point(i.Lng, i.Lat),
    spatial.point(11,11)
)
select {{ Distance: getMetadata(i)['@spatial'].Distance }}
").Single();
            WaitForUserToContinueTheTest(store);
            Assert.Equal(155.93, Math.Round((double)i.Distance, 2));
        }
    }
}
