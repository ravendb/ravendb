using System.Threading.Tasks;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Issues;

public class RavenDB_24077 : RavenTestBase
{
    public RavenDB_24077(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Indexes)]
    public async Task CreateIndexes_Should_Not_Throw_When_Indexes_List_Is_Empty()
    {
        using (var store = GetDocumentStore())
        {
            await IndexCreation.CreateIndexesAsync(typeof(string).Assembly, store);

            await store.ExecuteIndexesAsync([]);
        }
    }
}
