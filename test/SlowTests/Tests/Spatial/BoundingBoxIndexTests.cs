using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Spatial;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Tests.Spatial
{
    public class BoundingBoxIndexTests : RavenTestBase
    {
        public BoundingBoxIndexTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void BoundingBoxTest()
        {
            // X XXX X
            // X XXX X
            // X XXX X
            // X	 X
            // XXXXXXX

            var polygon = "POLYGON ((0 0, 0 5, 1 5, 1 1, 5 1, 5 5, 6 5, 6 0, 0 0))";
            var rectangle1 = "2 2 4 4";
            var rectangle2 = "6 6 10 10";
            var rectangle3 = "0 0 6 6";

            using (var store = GetDocumentStore())
            {
                store.Initialize();
                new BBoxIndex().Execute(store);
                new QuadTreeIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new SpatialDoc { Shape = polygon });
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var result = session.Query<SpatialDoc>()
                                        .Count();

                    Assert.Equal(1, result);
                }

                using (var session = store.OpenSession())
                {
                    var result = session.Query<SpatialDoc, BBoxIndex>()
                                        .Spatial(x => x.Shape, x => x.Intersects(rectangle1))
                                        .Count();

                    Assert.Equal(1, result);
                }

                using (var session = store.OpenSession())
                {
                    var result = session.Query<SpatialDoc, BBoxIndex>()
                                        .Spatial(x => x.Shape, x => x.Intersects(rectangle2))
                                        .Count();

                    Assert.Equal(0, result);
                }

                using (var session = store.OpenSession())
                {
                    var result = session.Query<SpatialDoc, BBoxIndex>()
                                        .Spatial(x => x.Shape, x => x.Disjoint(rectangle2))
                                        .Count();

                    Assert.Equal(1, result);
                }

                using (var session = store.OpenSession())
                {
                    var result = session.Query<SpatialDoc, BBoxIndex>()
                                        .Spatial(x => x.Shape, x => x.Within(rectangle3))
                                        .Count();

                    Assert.Equal(1, result);
                }

                using (var session = store.OpenSession())
                {
                    var result = session.Query<SpatialDoc, QuadTreeIndex>()
                                        .Spatial(x => x.Shape, x => x.Intersects(rectangle2))
                                        .Count();

                    Assert.Equal(0, result);
                }

                using (var session = store.OpenSession())
                {
                    var result = session.Query<SpatialDoc, QuadTreeIndex>()
                                        .Spatial(x => x.Shape, x => x.Intersects(rectangle1))
                                        .Count();

                    Assert.Equal(0, result);
                }
            }
        }

        private class SpatialDoc
        {
            public string Id { get; set; }
            public string Shape { get; set; }
        }

        private class BBoxIndex : AbstractIndexCreationTask<SpatialDoc>
        {
            public BBoxIndex()
            {
                Map = docs => from doc in docs
                              select new
                              {
                                  Shape = CreateSpatialField(doc.Shape)
                              };

                Spatial(x => x.Shape, x => x.Cartesian.BoundingBoxIndex());
            }
        }

        private class QuadTreeIndex : AbstractIndexCreationTask<SpatialDoc>
        {
            public QuadTreeIndex()
            {
                Map = docs => from doc in docs
                              select new
                              {
                                  Shape = CreateSpatialField(doc.Shape)
                              };

                Spatial(x => x.Shape, x => x.Cartesian.QuadPrefixTreeIndex(6, new SpatialBounds(0, 0, 16, 16)));
            }
        }
    }
}
