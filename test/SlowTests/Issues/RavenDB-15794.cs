using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Spatial;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15794 : RavenTestBase
    {
        public RavenDB_15794(ITestOutputHelper output) : base(output)
        {
        }
        

        [Fact]
        public void CirclesShouldIntersect()
        {
            using (var store = GetDocumentStore())
            {
                store.Initialize();
                store.ExecuteIndex(new GeoIndex());
                store.ExecuteIndex(new GeoIndexJava());

                using (var session = store.OpenSession())
                {
                    // 110km is approximately 1 degree
                    session.Store(new GeoDocument { WKT = "CIRCLE(0.000000 0.000000 d=110)" });
                    session.SaveChanges();
                }

                WaitForIndexing(store);
                WaitForUserToContinueTheTest(store);

                using (var session = store.OpenSession())
                {
                    var matches = session.Query<dynamic, GeoIndex>()
                        .Spatial("WKT", factory => factory.RelatesToShape("CIRCLE(0.000000 0.000000 d=110)", SpatialRelation.Intersects))
                        .Customize(x =>
                        {
                            x.WaitForNonStaleResults();
                        }).Any();

                    Assert.True(matches);
                }

                using (var session = store.OpenSession())
                {
                    var q = session.Query<dynamic, GeoIndexJava>()
                        .Spatial("WKT", factory => factory.RelatesToShape("CIRCLE(0.000000 0.000000 d=110)", SpatialRelation.Intersects))
                        .Customize(x =>
                        {
                            x.WaitForNonStaleResults();
                        });

                    var matches = q.Any();
                    Assert.True(matches);
                }
            }
        }


        private class GeoDocument
        {
            public string WKT { get; set; }
        }

        private class GeoIndex : AbstractIndexCreationTask<GeoDocument>
        {
            public GeoIndex()
            {
                Map = docs => from doc in docs
                    select new { WKT = CreateSpatialField(doc.WKT) };
            }
        }

        private class GeoIndexJava : AbstractJavaScriptIndexCreationTask
        {
            public GeoIndexJava()
            {
                Maps = new HashSet<string>
                {
                    @"
function createSpatialField(wkt){
    return { $spatial: wkt}
}
map('GeoDocuments', function (l){ return { WKT: createSpatialField(l.WKT) }})",
                };
            }
        }
    }
}
