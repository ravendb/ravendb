using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Raven.Client.Documents;
using Raven.Server.Documents.Indexes.Debugging;
using Sparrow;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_23710(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public async Task IdFieldShouldBeDisplayedAsStatic(Options options)
    {
        using (var store = GetDocumentStore(options))
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
                    var indexFields = JsonConvert.DeserializeObject<List<FieldDebugInfo>>(Encodings.Utf8.GetString(data));

                    Assert.Equal(2, indexFields.Count(x => x.FieldType == IndexFieldType.Static));
                    Assert.Equal(0, indexFields.Count(x => x.FieldType == IndexFieldType.Dynamic));
                    var idField = indexFields.FirstOrDefault(x => x.Name == "id()");
                    Assert.NotNull(idField);
                    Assert.Equal(IndexFieldType.Static, idField.FieldType);
                }
            }
        }
    }
    
    private class Dto
    {
        public string Name { get; set; }   
    }
}
