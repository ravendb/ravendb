using System.IO;
using System.Net.Http;
using System.Threading.Tasks;
using FastTests;
using Raven.Server.Extensions;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_25767(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public async Task WillQuoteCsvExportStringFields(Options options)
    {
        using var store = GetDocumentStore(options);
        using (var s = store.OpenSession())
        {
            s.Store(new Item("1234", 5678));
            s.SaveChanges();
        }

        using var client = new HttpClient().WithConventions(store.Conventions);
        await using var stream = await client.GetStreamAsync($"{store.Urls[0]}/databases/{store.Database}/streams/queries?query=from Items&format=csv");
        using var reader = new StreamReader(stream);
        string csv = await reader.ReadToEndAsync();
        Assert.Contains("""
                        "1234",5678
                        """.Trim('\r', '\n'), csv);
    }

    private record Item(string Name, int Age);
}
