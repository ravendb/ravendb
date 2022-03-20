using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Spatial;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Tests.Spatial
{
    public class WktSanitizerTests : RavenTestBase
    {
        public WktSanitizerTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Rectangle()
        {
            var wkt = new WktSanitizer();

            Assert.Equal("10.8 34.9 89.0 78.2", wkt.Sanitize("10.8 34.9 89.0 78.2"));
        }

        [Fact]
        public void Points()
        {
            var wkt = new WktSanitizer();

            Assert.Equal("POINT (0 0)", wkt.Sanitize("POINT (0 0)"));
            Assert.Equal("POINT (0 0)", wkt.Sanitize("POINT (0 0 0)"));
            Assert.Equal("POINT (0 0)", wkt.Sanitize("POINT (0 0 0 0)"));
            Assert.Equal("POINT (0 0)", wkt.Sanitize("POINT Z (0 0 0)"));
            Assert.Equal("POINT (0 0)", wkt.Sanitize("POINT M (0 0 0)"));
            Assert.Equal("POINT (0 0)", wkt.Sanitize("POINT ZM (0 0 0 0)"));
        }

        [Fact]
        public void LineStrings()
        {
            var wkt = new WktSanitizer();

            Assert.Equal("LINESTRING (0 0, 1 1)", wkt.Sanitize("LINESTRING (0 0, 1 1)"));
            Assert.Equal("LINESTRING (0 0, 1 1)", wkt.Sanitize("LINESTRING (0 0 0, 1 1 1)"));
            Assert.Equal("LINESTRING (0 0, 1 1)", wkt.Sanitize("LINESTRING (0 0 0 0, 1 1 1 1)"));
            Assert.Equal("LINESTRING (0 0, 1 1)", wkt.Sanitize("LINESTRING Z (0 0 0, 1 1 1)"));
            Assert.Equal("LINESTRING (0 0, 1 1)", wkt.Sanitize("LINESTRING M (0 0 0, 1 1 1)"));
            Assert.Equal("LINESTRING (0 0, 1 1)", wkt.Sanitize("LINESTRING ZM (0 0 0 0, 1 1 1 1)"));
        }

        [Fact]
        public void Polygons()
        {
            var wkt = new WktSanitizer();

            Assert.Equal("POLYGON ((0 0, 1 1, 2 2, 0 0))", wkt.Sanitize("POLYGON ((0 0, 1 1, 2 2, 0 0))"));
            Assert.Equal("POLYGON ((0 0, 1 1, 2 2, 0 0))", wkt.Sanitize("POLYGON ((0 0 0, 1 1 1, 2 2 2, 0 0 0))"));
            Assert.Equal("POLYGON ((0 0, 1 1, 2 2, 0 0))", wkt.Sanitize("POLYGON ((0 0 0 0, 1 1 1 1, 2 2 2 2, 0 0 0 0))"));
            Assert.Equal("POLYGON ((0 0, 1 1, 2 2, 0 0))", wkt.Sanitize("POLYGON Z ((0 0 0, 1 1 1, 2 2 2, 0 0 0))"));
            Assert.Equal("POLYGON ((0 0, 1 1, 2 2, 0 0))", wkt.Sanitize("POLYGON M ((0 0 0, 1 1 1, 2 2 2, 0 0 0))"));
            Assert.Equal("POLYGON ((0 0, 1 1, 2 2, 0 0))", wkt.Sanitize("POLYGON ZM ((0 0 0 0, 1 1 1 1, 2 2 2 2, 0 0 0 0))"));
        }

        [Fact]
        public void Integration()
        {
            using (var store = GetDocumentStore())
            {
                store.Initialize();
                store.ExecuteIndex(new SpatialIndex());

                using (var session = store.OpenSession())
                {
                    session.Store(new SpatialDoc { WKT = "POINT (50.8 50.8)" });
                    session.Store(new SpatialDoc { WKT = "POINT (50.8 50.8 50.8)" });
                    session.Store(new SpatialDoc { WKT = "POINT (50.8 50.8 50.8 50.8)" });
                    session.Store(new SpatialDoc { WKT = "POINT Z (50.8 50.8 50.8)" });
                    session.Store(new SpatialDoc { WKT = "POINT M (50.8 50.8 50.8)" });
                    session.Store(new SpatialDoc { WKT = "POINT ZM (50.8 50.8 50.8 50.8)" });
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var matches = session.Query<dynamic, SpatialIndex>()
                        .Spatial("WKT", factory => factory.WithinRadius(150, 50.8, 50.8))
                        .Count();

                    Assert.True(matches == 6);
                }
            }
        }

        private class SpatialDoc
        {
            public string Id { get; set; }
            public string WKT { get; set; }
        }

        private class SpatialIndex : AbstractIndexCreationTask<SpatialDoc>
        {
            public SpatialIndex()
            {
                Map = docs => from doc in docs select new { WKT = CreateSpatialField(doc.WKT) };

                Spatial(x => x.WKT, x => x.Geography.Default());
            }
        }
    }
}
