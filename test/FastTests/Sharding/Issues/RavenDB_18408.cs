using System.Threading.Tasks;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Sharding.Issues;

public class RavenDB_18408 : RavenTestBase
{
    public RavenDB_18408(ITestOutputHelper output) : base(output)
    {
    }

    [Theory]
    [RavenData(DatabaseMode = RavenDatabaseMode.Sharded)]
    public async Task Wrong_Json_in_BulkDocs(Options options)
    {
        options.RunInMemory = false;
        using var store = GetDocumentStore(options);
        using (var session = store.OpenAsyncSession())
        {
            var arr = new string[100000];
            for (int i = 0; i < arr.Length; i++)
            {
                arr[i] = "stv";
            }

            var user = new User { Arr = arr };
            await session.StoreAsync(user, "users/1");
            await session.SaveChangesAsync();
        }
    }

    private class User
    {
        public string[] Arr { get; set; }
    }
}
