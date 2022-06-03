using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server.Config;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_8328 : RavenTestBase
    {
        public RavenDB_8328(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenExplicitData(searchEngine: RavenSearchEngineMode.All)]
        public void SpatialOnAutoIndex(RavenTestParameters config)
        {
            var databaseName = $"{nameof(SpatialOnAutoIndex)}-{Guid.NewGuid()}";
            var path = NewDataPath();
            using (var store = GetDocumentStore(new Options
            {
                Path = path,
                ModifyDatabaseName = s => databaseName,
                DeleteDatabaseOnDispose = false,
                ModifyDatabaseRecord = d =>
                {
                    d.Settings[RavenConfiguration.GetKey(x => x.Indexing.AutoIndexingEngineType)] = config.SearchEngine.ToString();
                    d.Settings[RavenConfiguration.GetKey(x => x.Indexing.StaticIndexingEngineType)] = config.SearchEngine.ToString();
                }
            }))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Item
                    {
                        Latitude = 10,
                        Longitude = 20,
                        Latitude2 = 10,
                        Longitude2 = 20,
                        ShapeWkt = "POINT(20 10)",
                        Name = "Name1"
                    });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var q = session.Query<Item>()
                        .Spatial(factory => factory.Point(x => x.Latitude, x => x.Longitude), factory => factory.WithinRadius(10, 10, 20));

                    var iq = RavenTestHelper.GetIndexQuery(q);
                    Assert.Equal("from 'Items' where spatial.within(spatial.point(Latitude, Longitude), spatial.circle($p0, $p1, $p2))", iq.Query);

                    var dq = session.Advanced.DocumentQuery<Item>()
                        .Spatial(factory => factory.Point(x => x.Latitude, x => x.Longitude), factory => factory.WithinRadius(10, 10, 20));

                    iq = dq.GetIndexQuery();
                    Assert.Equal("from 'Items' where spatial.within(spatial.point(Latitude, Longitude), spatial.circle($p0, $p1, $p2))", iq.Query);

                    dq = session.Advanced.DocumentQuery<Item>()
                        .Spatial(factory => factory.Wkt(x => x.ShapeWkt), factory => factory.WithinRadius(10, 10, 20));

                    iq = dq.GetIndexQuery();
                    Assert.Equal("from 'Items' where spatial.within(spatial.wkt(ShapeWkt), spatial.circle($p0, $p1, $p2))", iq.Query);
                }

                using (var session = store.OpenSession())
                {
                    var results = session.Query<Item>()
                        .Statistics(out var stats)
                        .Spatial(factory => factory.Point(x => x.Latitude, x => x.Longitude), factory => factory.WithinRadius(10, 10, 20))
                        .ToList();
                    WaitForUserToContinueTheTest(store);

                    Assert.Equal(1, results.Count);
                    Assert.Equal("Auto/Items/BySpatial.point(Latitude|Longitude)", stats.IndexName);
                }
            }

            using (var store = GetDocumentStore(new Options
            {
                Path = path,
                ModifyDatabaseName = s => databaseName,
                CreateDatabase = false,
                ModifyDatabaseRecord = d =>
                {
                    d.Settings[RavenConfiguration.GetKey(x => x.Indexing.AutoIndexingEngineType)] = config.SearchEngine.ToString();
                    d.Settings[RavenConfiguration.GetKey(x => x.Indexing.StaticIndexingEngineType)] = config.SearchEngine.ToString();
                }
            }))
            {
                var indexes = store.Maintenance.Send(new GetIndexesOperation(0, 10)); // checking it index survived restart
                Assert.Equal(1, indexes.Length);
                Assert.Equal("Auto/Items/BySpatial.point(Latitude|Longitude)", indexes[0].Name);

                using (var session = store.OpenSession()) // validating matching
                {
                    var results = session.Query<Item>()
                        .Statistics(out var stats)
                        .Spatial(factory => factory.Point(x => x.Latitude, x => x.Longitude), factory => factory.WithinRadius(10, 10, 20))
                        .ToList();

                    Assert.Equal(1, results.Count);
                    Assert.Equal("Auto/Items/BySpatial.point(Latitude|Longitude)", stats.IndexName);
                }

                using (var session = store.OpenSession()) // validating extending
                {
                    var results = session.Query<Item>()
                        .Statistics(out var stats)
                        .Spatial(factory => factory.Point(x => x.Latitude2, x => x.Longitude2), factory => factory.WithinRadius(10, 10, 20))
                        .ToList();

                    Assert.Equal(1, results.Count);
                    Assert.Equal("Auto/Items/BySpatial.point(Latitude|Longitude)AndSpatial.point(Latitude2|Longitude2)", stats.IndexName);

                    results = session.Query<Item>()
                        .Statistics(out stats)
                        .Spatial(factory => factory.Wkt(x => x.ShapeWkt), factory => factory.WithinRadius(10, 10, 20))
                        .ToList();

                    Assert.Equal(1, results.Count);
                    Assert.Equal("Auto/Items/BySpatial.point(Latitude|Longitude)AndSpatial.point(Latitude2|Longitude2)AndSpatial.wkt(ShapeWkt)", stats.IndexName);
                }
            }
        }

        private class Item
        {
            public string Id { get; set; }

            public string Name { get; set; }

            public double Latitude { get; set; }

            public double Longitude { get; set; }

            public double Latitude2 { get; set; }

            public double Longitude2 { get; set; }

            public string ShapeWkt { get; set; }
        }
    }
}
