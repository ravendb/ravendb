using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using Raven.Server.Config;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_19544 : RavenTestBase
{
    public RavenDB_19544(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Spatial)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, SearchEngineMode = RavenSearchEngineMode.All)]
    public async Task CanGetSpatialDistanceOnIndex_MapReduce(Options options)
    {
        IncludeDistancesAndScore(options);
        using (var store = GetDocumentStore(options))
        {
            await store.ExecuteIndexAsync(new MapReduceRestoIndex());

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Resto { Lat = 25.0676339, Lng = 55.141291 });

                await session.SaveChangesAsync();
            }

            Indexes.WaitForIndexing(store);
            using (var session = store.OpenAsyncSession())
            {
                var projection =
                    from result in session.Query<MapReduceRestoIndex.Result, MapReduceRestoIndex>()
                        .Spatial(x => x.Location, c => c.WithinRadius(100000, 25, 55))
                        .OrderByDistance(x => x.Location, 25, 55)
                    select new
                    {
                        Distance = (double?)((IMetadataDictionary)RavenQuery.Metadata(result)["@spatial"])["Distance"]
                    };

                var items = await projection.ToArrayAsync();
                Assert.NotEmpty(items);
                foreach (var item in items)
                {
                    Assert.NotNull(item.Distance);
                }
            }
        }
    }


    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Spatial)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All, SearchEngineMode = RavenSearchEngineMode.All)]
    public async Task CanGetSpatialDistanceOnIndex_Map(Options options)
    {
        IncludeDistancesAndScore(options);
        using (var store = GetDocumentStore(options))
        {
            await store.ExecuteIndexAsync(new RestoIndex());

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Resto { Lat = 25.0676339, Lng = 55.141291 });

                await session.SaveChangesAsync();
            }

            Indexes.WaitForIndexing(store);
            using (var session = store.OpenAsyncSession())
            {
                var projection =
                    from result in session.Query<RestoIndex.Result, RestoIndex>()
                        .Spatial(x => x.Location, c => c.WithinRadius(100000, 25, 55))
                        .OrderByDistance(x => x.Location, 25, 55)
                    select new
                    {
                        Distance = (double?)((IMetadataDictionary)RavenQuery.Metadata(result)["@spatial"])["Distance"]
                    };

                var items = await projection.ToArrayAsync();
                Assert.NotEmpty(items);
                foreach (var item in items)
                {
                    Assert.NotNull(item.Distance);
                }
            }
        }
    }

    private void IncludeDistancesAndScore(Options options)
    {
        if (options.SearchEngineMode is RavenSearchEngineMode.Corax)
        {
            options.ModifyDatabaseRecord += record =>
            {
                record.Settings[RavenConfiguration.GetKey(x => x.Indexing.CoraxIncludeSpatialDistance)] = true.ToString();
                record.Settings[RavenConfiguration.GetKey(x => x.Indexing.CoraxIncludeDocumentScore)] = true.ToString();
            };
        }
    }
    
    private class RestoIndex : AbstractIndexCreationTask<Resto, RestoIndex.Result>
    {
        public class Result
        {
            public string Id { get; set; } = null!;

            public object Location { get; set; }
        }

        public RestoIndex()
        {
            Map = restaurants => from restaurant in restaurants
                                 select new Result
                                 {
                                     Id = restaurant.Id,
                                     Location = CreateSpatialField(restaurant.Lat, restaurant.Lng)
                                 };
        }
    }

    private class Resto
    {
        public string Id { get; set; }

        public double Lat { get; set; }

        public double Lng { get; set; }
    }

    private class MapReduceRestoIndex : AbstractMultiMapIndexCreationTask<MapReduceRestoIndex.Result>
    {
        public class Result
        {
            public string Id { get; set; } = null!;

            public double LocationLatitude { get; set; }

            public double LocationLongitude { get; set; }

            public object Location { get; set; }
        }

        public MapReduceRestoIndex()
        {
            AddMap<Resto>(restaurants => from restaurant in restaurants
                                         select new Result
                                         {
                                             Id = restaurant.Id,
                                             LocationLatitude = (double)restaurant.Lat,
                                             LocationLongitude = (double)restaurant.Lng,
                                             Location = null,
                                         });

            Reduce = results => from result in results
                                group result by result.Id
                into g
                                let restaurant = g.FirstOrDefault()
                                select new Result
                                {
                                    Id = g.Key,
                                    LocationLatitude = restaurant.LocationLatitude,
                                    LocationLongitude = restaurant.LocationLongitude,
                                    Location = CreateSpatialField(restaurant.LocationLatitude, restaurant.LocationLongitude)
                                };

            StoreAllFields(FieldStorage.Yes);
        }
    }
}
