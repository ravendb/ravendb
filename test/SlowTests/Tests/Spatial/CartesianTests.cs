using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Spatial;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Tests.Spatial
{
    public class CartesianTests : RavenTestBase
    {
        public CartesianTests(ITestOutputHelper output) : base(output)
        {
        }

        private class SpatialDoc
        {
            public string Id { get; set; }
            public object Name { get; set; }
            public string WKT { get; set; }
        }

        private class CartesianIndex : AbstractIndexCreationTask<SpatialDoc>
        {
            public CartesianIndex()
            {
                Map = docs => from doc in docs select new { doc.Name, WKT = CreateSpatialField(doc.WKT) };

                Index(x => x.Name, FieldIndexing.Search);
                Store(x => x.Name, FieldStorage.Yes);

                Spatial(x => x.WKT, x => x.Cartesian.QuadPrefixTreeIndex(12, new SpatialBounds(0, 0, 2000, 2000)));
            }
        }

        [Fact]
        public void Points()
        {
            using (var store = GetDocumentStore())
            {
                store.Initialize();
                store.ExecuteIndex(new CartesianIndex());

                using (var session = store.OpenSession())
                {
                    session.Store(new SpatialDoc { WKT = "POINT (1950 1950)", Name = new { sdsdsd = "sdsds", sdsdsds = "sdsds" } });
                    session.Store(new SpatialDoc { WKT = "POINT (50 1950)", Name = "dog" });
                    session.Store(new SpatialDoc { WKT = "POINT (1950 50)", Name = "cat" });
                    session.Store(new SpatialDoc { WKT = "POINT (50 50)", Name = "dog" });
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var matches = session.Query<dynamic, CartesianIndex>()
                        .Spatial("WKT", factory => factory.WithinRadius(70, 1900, 1900))
                        .Any();

                    Assert.True(matches);
                }
            }
        }
    }
}
