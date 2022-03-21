using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_8254 : RavenTestBase
    {
        public RavenDB_8254(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanUseTransformWithSpatial()
        {
            using (var store = GetDocumentStore())
            {
                new Index_Spatial().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Item
                    {
                        Name = "John",
                        Latitude = 50,
                        Longitude = 50
                    });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var commands = store.Commands())
                {
                    var iq = new IndexQuery
                    {
                        Query = $@"
declare function transform(item) {{ item.Name = 'transformed'; return item; }}
FROM INDEX '{new Index_Spatial().IndexName}' as i
WHERE spatial.within(Coordinates, spatial.circle(10, 50, 50))
SELECT transform(i)
"
                    };

                    var qr = commands.Query(iq);

                    Assert.Equal(1, qr.Results.Length);

                    var result = qr.Results[0] as BlittableJsonReaderObject;
                    Assert.NotNull(result);

                    Assert.True(result.TryGet("Name", out string name));
                    Assert.Equal("transformed", name);
                }
            }
        }

        private class Index_Spatial : AbstractIndexCreationTask<Item>
        {
            public Index_Spatial()
            {
                Map = items => from item in items
                               select new
                               {
                                   Name = item.Name,
                                   Coordinates = CreateSpatialField(item.Latitude, item.Longitude)
                               };
            }
        }

        private class Item
        {
            public string Name { get; set; }

            public double Latitude { get; set; }

            public double Longitude { get; set; }
        }
    }
}
