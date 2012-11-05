using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.Spatial
{
    public class SimonBartlett
    {
        [Fact]
        public void LineStringsShouldIntersect()
        {
            using (var store = new EmbeddableDocumentStore { RunInMemory = true })
            {
                store.Initialize();
                store.ExecuteIndex(new GeoIndex());

                using (var session = store.OpenSession())
                {
                    session.Store(new GeoDocument { WKT = "LINESTRING (0 0, 1 1, 2 1)" });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var matches = session.Query<RavenJObject, GeoIndex>()
                        .Customize(x =>
                        {
                            x.RelatesToShape("WKT", "LINESTRING (1 0, 1 1, 1 2)", SpatialRelation.Intersects);
                            x.WaitForNonStaleResults();
                        }).Any();

                    Assert.True(matches);
                }
            }
        }

        public class GeoDocument
        {
            public string WKT { get; set; }
        }

        public class GeoIndex : AbstractIndexCreationTask<GeoDocument>
        {
            public GeoIndex()
            {
                Map = docs => from doc in docs
                              select new { _ = SpatialGenerate("WKT", doc.WKT) };
            }
        }
    }
}
