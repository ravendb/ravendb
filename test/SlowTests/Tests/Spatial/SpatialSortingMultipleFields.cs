using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Spatial;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Session;
using Raven.Server.Config;
using Raven.Server.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Tests.Spatial
{
    public class SpatialSortingMultipleFields : RavenTestBase
    {
        public SpatialSortingMultipleFields(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Spatial)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void OrderByDistanceUsesTheRequestedSpatialField(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                new People_ByHomeAndWork().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Person { Name = "A", HomeLat = 10, HomeLng = 10, WorkLat = 40, WorkLng = 40 });
                    session.Store(new Person { Name = "B", HomeLat = 20, HomeLng = 20, WorkLat = 11, WorkLng = 11 });
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var byHome = session.Query<Person, People_ByHomeAndWork>()
                        .OrderByDistance("Home", 10, 10)
                        .Select(x => x.Name)
                        .ToList();

                    Assert.Equal(new[] { "A", "B" }, byHome);

                    var byWork = session.Query<Person, People_ByHomeAndWork>()
                        .OrderByDistance("Work", 10, 10)
                        .Select(x => x.Name)
                        .ToList();

                    Assert.Equal(new[] { "B", "A" }, byWork);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Spatial)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void OrderByDistanceUsesTheNearestPointOfTheField(Options options)
        {
            options.ModifyDatabaseRecord += record => record.Settings[RavenConfiguration.GetKey(x => x.Indexing.CoraxIncludeSpatialDistance)] = true.ToString();

            using (var store = GetDocumentStore(options))
            {
                new Shops_ByBranches().Execute(store);
                StoreShops(store);

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    // the first (and lowest latitude) branch of X is the farthest one, the nearest one is closer than the branch of Y
                    var ascending = session.Query<Shop, Shops_ByBranches>()
                        .OrderByDistance("Branches", 10, 10)
                        .Select(x => x.Name)
                        .ToList();

                    Assert.Equal(3, ascending.Count);
                    Assert.Equal(new[] { "X", "Y" }, ascending.Where(x => x != "Z"));

                    var descending = session.Query<Shop, Shops_ByBranches>()
                        .OrderByDistanceDescending("Branches", 10, 10)
                        .Select(x => x.Name)
                        .ToList();

                    Assert.Equal(3, descending.Count);
                    Assert.Equal(new[] { "Y", "X" }, descending.Where(x => x != "Z"));
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.DocumentQuery<Shop, Shops_ByBranches>()
                        .WhereEquals(x => x.Name, "X")
                        .OrderByDistance("Branches", 10, 10);

                    using (var results = session.Advanced.Stream(query))
                    {
                        Assert.True(results.MoveNext());

                        var spatialResult = (IDictionary<string, object>)results.Current.Metadata[Constants.Documents.Metadata.SpatialResult];
                        Assert.Equal(10.5, (double)spatialResult["Latitude"], precision: 5);
                        Assert.Equal(10.5, (double)spatialResult["Longitude"], precision: 5);
                    }
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Spatial)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public async Task LuceneIndexesTheCenterPointsOfAFieldAsASingleTerm(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var index = new Shops_ByBranches();
                await index.ExecuteAsync(store);
                StoreShops(store);

                Indexes.WaitForIndexing(store);

                var database = await GetDatabase(store.Database);
                Assert.Equal(IndexDefinitionBaseServerSide.IndexVersion.SpatialSortByNearestPoint_62, database.IndexStore.GetIndex(index.IndexName).Definition.Version);

                var terms = await store.Maintenance.SendAsync(new GetTermsOperation(index.IndexName, "Branches_spatial(centers)", fromValue: null));

                Assert.Equal(new[] { "12 12", "5 40;10.5 10.5" }, terms.OrderBy(x => x));
            }
        }

        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Spatial | RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.Single)]
        public async Task LegacyIndexKeepsItsOrderingByDistanceUntilReset(Options options)
        {
            // created with index version LuceneExactDatesUseTimeTicks_62, the data and indexes are the same as in the tests above
            var isLucene = options.SearchEngineMode == RavenSearchEngineMode.Lucene;
            var snapshotName = isLucene ? "RavenDB_27747.ravendb-snapshot" : "RavenDB_27747_Corax.ravendb-snapshot";

            var backupPath = NewDataPath(forceCreateDir: true);
            var file = Path.Combine(backupPath, snapshotName);

            using (var fileStream = File.Create(file))
            using (var stream = GetType().Assembly.GetManifestResourceStream("SlowTests.Data.RavenDB_27747." + snapshotName))
            {
                await stream.CopyToAsync(fileStream);
            }

            using (var store = GetDocumentStore(options))
            {
                var databaseName = GetDatabaseName();
                using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration { BackupLocation = backupPath, DatabaseName = databaseName }))
                {
                    var shopsIndex = new Shops_ByBranches();
                    var peopleIndex = new People_ByHomeAndWork();

                    Indexes.WaitForIndexing(store, databaseName);

                    var database = await GetDatabase(databaseName);
                    Assert.Equal(IndexDefinitionBaseServerSide.IndexVersion.LuceneExactDatesUseTimeTicks_62, database.IndexStore.GetIndex(shopsIndex.IndexName).Definition.Version);
                    Assert.Equal(IndexDefinitionBaseServerSide.IndexVersion.LuceneExactDatesUseTimeTicks_62, database.IndexStore.GetIndex(peopleIndex.IndexName).Definition.Version);

                    // legacy behavior: Lucene calculates the distance from the first shape stored in the entry, regardless of the requested field,
                    // Corax from the point with the lowest latitude of the requested field
                    AssertOrder(store, databaseName, expectedShops: new[] { "Y", "X" }, expectedByWork: isLucene ? new[] { "A", "B" } : new[] { "B", "A" });

                    store.Maintenance.ForDatabase(databaseName).Send(new ResetIndexOperation(shopsIndex.IndexName));
                    store.Maintenance.ForDatabase(databaseName).Send(new ResetIndexOperation(peopleIndex.IndexName));

                    // wait for the reset indexes to replace the legacy ones before waiting for them to catch up
                    foreach (var indexName in new[] { shopsIndex.IndexName, peopleIndex.IndexName })
                    {
                        var version = WaitForValue(() => database.IndexStore.GetIndex(indexName)?.Definition.Version, IndexDefinitionBaseServerSide.IndexVersion.SpatialSortByNearestPoint_62);
                        Assert.Equal(IndexDefinitionBaseServerSide.IndexVersion.SpatialSortByNearestPoint_62, version);
                    }

                    Indexes.WaitForIndexing(store, databaseName);

                    AssertOrder(store, databaseName, expectedShops: new[] { "X", "Y" }, expectedByWork: new[] { "B", "A" });
                }
            }

            static void AssertOrder(IDocumentStore store, string databaseName, string[] expectedShops, string[] expectedByWork)
            {
                using (var session = store.OpenSession(databaseName))
                {
                    var shops = session.Query<Shop, Shops_ByBranches>()
                        .OrderByDistance("Branches", 10, 10)
                        .Select(x => x.Name)
                        .ToList();

                    Assert.Equal(expectedShops, shops.Where(x => x != "Z"));

                    var byWork = session.Query<Person, People_ByHomeAndWork>()
                        .OrderByDistance("Work", 10, 10)
                        .Select(x => x.Name)
                        .ToList();

                    Assert.Equal(expectedByWork, byWork);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Spatial)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void AutoIndexOrdersByDistanceUsingTheRequestedSpatialField(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Person { Name = "A", HomeLat = 10, HomeLng = 10, WorkLat = 40, WorkLng = 40 });
                    session.Store(new Person { Name = "B", HomeLat = 20, HomeLng = 20, WorkLat = 11, WorkLng = 11 });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    // the second query extends the auto index of the first one, so it ends up with two spatial fields - query both on it
                    session.Query<Person>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .OrderByDistance(factory => factory.Point(x => x.HomeLat, x => x.HomeLng), 10, 10)
                        .ToList();

                    var byWork = session.Query<Person>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .OrderByDistance(factory => factory.Point(x => x.WorkLat, x => x.WorkLng), 10, 10)
                        .Select(x => x.Name)
                        .ToList();

                    var byHome = session.Query<Person>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .OrderByDistance(factory => factory.Point(x => x.HomeLat, x => x.HomeLng), 10, 10)
                        .Select(x => x.Name)
                        .ToList();

                    Assert.Equal(new[] { "A", "B" }, byHome);
                    Assert.Equal(new[] { "B", "A" }, byWork);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Spatial)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene, DatabaseMode = RavenDatabaseMode.All)]
        public void OrderByDistanceUsesTheCenterOfTheNearestShape(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                new Places_ByAreas().Execute(store);

                using (var session = store.OpenSession())
                {
                    // squares centered at (lat 5, lng 40) and (lat 10.5, lng 10.5)
                    session.Store(new Place
                    {
                        Name = "X",
                        Areas = new[] { "POLYGON ((39.5 4.5, 40.5 4.5, 40.5 5.5, 39.5 5.5, 39.5 4.5))", "POLYGON ((10 10, 11 10, 11 11, 10 11, 10 10))" }
                    });
                    // square centered at (lat 12, lng 12)
                    session.Store(new Place { Name = "Y", Areas = new[] { "POLYGON ((11.5 11.5, 12.5 11.5, 12.5 12.5, 11.5 12.5, 11.5 11.5))" } });
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var ascending = session.Query<Place, Places_ByAreas>()
                        .OrderByDistance("Areas", 10, 10)
                        .Select(x => x.Name)
                        .ToList();

                    Assert.Equal(new[] { "X", "Y" }, ascending);

                    var descending = session.Query<Place, Places_ByAreas>()
                        .OrderByDistanceDescending("Areas", 10, 10)
                        .Select(x => x.Name)
                        .ToList();

                    Assert.Equal(new[] { "Y", "X" }, descending);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Spatial)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene, DatabaseMode = RavenDatabaseMode.All)]
        public void OrderByDistanceOnCartesianFieldUsesTheNearestPoint(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                new Places_ByCartesianAreas().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Place { Name = "X", Areas = new[] { "POINT (1900 1900)", "POINT (110 110)" } });
                    session.Store(new Place { Name = "Y", Areas = new[] { "POINT (150 150)" } });
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var ascending = session.Query<Place, Places_ByCartesianAreas>()
                        .OrderByDistance("Areas", "POINT (100 100)")
                        .Select(x => x.Name)
                        .ToList();

                    Assert.Equal(new[] { "X", "Y" }, ascending);

                    var descending = session.Query<Place, Places_ByCartesianAreas>()
                        .OrderByDistanceDescending("Areas", "POINT (100 100)")
                        .Select(x => x.Name)
                        .ToList();

                    Assert.Equal(new[] { "Y", "X" }, descending);
                }
            }
        }

        private static void StoreShops(IDocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Shop { Name = "X", Branches = new[] { new Location { Lat = 5, Lng = 40 }, new Location { Lat = 10.5, Lng = 10.5 } } });
                session.Store(new Shop { Name = "Y", Branches = new[] { new Location { Lat = 12, Lng = 12 } } });
                session.Store(new Shop { Name = "Z", Branches = new Location[0] });
                session.SaveChanges();
            }
        }

        private class Person
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public double HomeLat { get; set; }
            public double HomeLng { get; set; }
            public double WorkLat { get; set; }
            public double WorkLng { get; set; }
        }

        private class Shop
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public Location[] Branches { get; set; }
        }

        private class Place
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string[] Areas { get; set; }
        }

        private class Location
        {
            public double Lat { get; set; }
            public double Lng { get; set; }
        }

        private class People_ByHomeAndWork : AbstractIndexCreationTask<Person>
        {
            public People_ByHomeAndWork()
            {
                Map = people => from p in people
                                select new
                                {
                                    p.Name,
                                    Home = CreateSpatialField(p.HomeLat, p.HomeLng),
                                    Work = CreateSpatialField(p.WorkLat, p.WorkLng)
                                };
            }
        }

        private class Shops_ByBranches : AbstractIndexCreationTask<Shop>
        {
            public Shops_ByBranches()
            {
                Map = shops => from s in shops
                               select new
                               {
                                   s.Name,
                                   Branches = s.Branches.Select(b => CreateSpatialField(b.Lat, b.Lng))
                               };
            }
        }

        private class Places_ByAreas : AbstractIndexCreationTask<Place>
        {
            public Places_ByAreas()
            {
                Map = places => from p in places
                                select new
                                {
                                    p.Name,
                                    Areas = p.Areas.Select(a => CreateSpatialField(a))
                                };
            }
        }

        private class Places_ByCartesianAreas : AbstractIndexCreationTask<Place>
        {
            public Places_ByCartesianAreas()
            {
                Map = places => from p in places
                                select new
                                {
                                    p.Name,
                                    Areas = p.Areas.Select(a => CreateSpatialField(a))
                                };

                Spatial(x => x.Areas, x => x.Cartesian.QuadPrefixTreeIndex(12, new SpatialBounds(0, 0, 2000, 2000)));
            }
        }
    }
}
