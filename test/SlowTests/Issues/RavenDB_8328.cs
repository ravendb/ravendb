using FastTests;
using Raven.Client.Documents;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_8328 : RavenTestBase
    {
        [Fact]
        public void T1()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Item
                    {
                        Latitude = 10,
                        Longitude = 20,
                        Name = "Name1"
                    });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var q = session.Query<Item>()
                        .Spatial(factory => factory.Point(x => x.Latitude, x => x.Longitude), factory => factory.WithinRadius(10, 10, 20));

                    var iq = RavenTestHelper.GetIndexQuery(q);
                    Assert.Equal("FROM Items WHERE within(point(Latitude, Longitude), circle($p0, $p1, $p2))", iq.Query);
                }

                using (var session = store.OpenSession())
                {
                    var q = session.Advanced.DocumentQuery<Item>()
                        .Spatial(factory => factory.Point(x => x.Latitude, x => x.Longitude), factory => factory.WithinRadius(10, 10, 20));

                    var iq = q.GetIndexQuery();
                    Assert.Equal("FROM Items WHERE within(point(Latitude, Longitude), circle($p0, $p1, $p2))", iq.Query);
                }
            }
        }

        private class Item
        {
            public string Id { get; set; }

            public string Name { get; set; }
            
            public double Latitude { get; set; }

            public double Longitude { get; set; }
        }
    }
}
