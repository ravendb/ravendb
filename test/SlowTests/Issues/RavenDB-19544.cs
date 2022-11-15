using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_19544 : RavenTestBase
{
    public RavenDB_19544(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public async Task CanGetSpatialDistanceOnIndex()
    {
        using (var store = GetDocumentStore())
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



    public class Resto
    {
        public string Id { get; set; }

        public double Lat { get; set; }

        public double Lng { get; set; }
    }

    public class RestoIndex : AbstractMultiMapIndexCreationTask<RestoIndex.Result>
    {
        public class Result
        {
            public string Id { get; set; } = null!;

            public double LocationLatitude { get; set; }

            public double LocationLongitude { get; set; }

            public object? Location { get; set; }
        }

        public RestoIndex()
        {
            AddMap<Resto>(restaurants => from restaurant in restaurants
                select new Result
                {
                    Id = restaurant.Id, LocationLatitude = (double)restaurant.Lat, LocationLongitude = (double)restaurant.Lng, Location = null,
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
