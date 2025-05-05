using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Raven.Client.Documents;
using Sparrow;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_23710 : RavenTestBase
{
    public RavenDB_23710(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Indexes)]
    public async Task IdFieldShouldBeDisplayedAsStatic()
    {
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Dto() { Name = "SomeName" });
                await session.SaveChangesAsync();

                _ = await session.Query<Dto>()
                    .Statistics(out var stats)
                    .Where(x => x.Name == "SomeName")
                    .ToListAsync();
                
                string url = $"{store.Urls.First()}/databases/{store.Database}/indexes/debug?name={stats.IndexName}&op=entries-fields&nodeTag=A";

                using (var client = new HttpClient())
                {
                    var response = (await client.SendAsync(new HttpRequestMessage(HttpMethod.Get, url))).Content;
                    var data = await response.ReadAsByteArrayAsync();
                    Assert.NotNull(data);
                    var indexFields = JsonConvert.DeserializeObject<Response>(Encodings.Utf8.GetString(data));

                    Assert.Equal(2, indexFields.Static.Length);
                    Assert.Equal(0, indexFields.Dynamic.Length);
                    
                    Assert.Contains("id()", indexFields.Static);
                }
            }
        }
    }
    
    private class Response
    {
        public string[] Static { get; set; }
        public string[] Dynamic { get; set; }
    }

    private class Dto
    {
        public string Name { get; set; }   
    }
}
