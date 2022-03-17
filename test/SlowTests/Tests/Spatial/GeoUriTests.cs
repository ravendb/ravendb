using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Tests.Spatial
{
    public class GeoUriTests : RavenTestBase
    {
        public GeoUriTests(ITestOutputHelper output) : base(output)
        {
        }

        private class SpatialDoc
        {
            public string Id { get; set; }
            public string Point { get; set; }
        }

        private class PointIndex : AbstractIndexCreationTask<SpatialDoc>
        {
            public PointIndex()
            {
                Map = docs => from doc in docs select new { Point = CreateSpatialField(doc.Point) };

                Spatial(x => x.Point, x => x.Geography.Default());
            }
        }

        [Fact]
        public void PointTest()
        {
            using (var store = GetDocumentStore())
            {
                store.Initialize();
                store.ExecuteIndex(new PointIndex());

                using (var session = store.OpenSession())
                {
                    session.Store(new SpatialDoc { Point = "geo:45.0,45.0,-78.4" });
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var matches = session.Query<SpatialDoc, PointIndex>()
                                         .Spatial(x => x.Point, x => x.Within("geo:45.0,45.0,-78.4;u=100.0"))
                                         .Any();

                    Assert.True(matches);
                }
            }
        }
    }
}
