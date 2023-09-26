using System.Linq;
using System.Threading.Tasks;
using FastTests.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax;

public class RavenDB_21430 : RavenTestBase
{
    public RavenDB_21430(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Querying | RavenTestCategory.Indexes | RavenTestCategory.Corax)]
    public async Task CanIndexComplexFieldInAutoIndex()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new ComplexField(new Query.Order() {Company = "Maciej"}, new Query.Order(){Company = "Test"}));
            await session.StoreAsync(new ComplexField(null, null));
            await session.SaveChangesAsync();

            var result = await session.Query<ComplexField>()
                .Customize(i => i.WaitForNonStaleResults())
                .Statistics(out var stats).Where(x => x.Order != null && x.Field != null)
                .ToListAsync();
            WaitForUserToContinueTheTest(store);
            Assert.False(stats.IsStale);
            
            Assert.Equal(1, result.Count);
            var indexErrors = store.Maintenance.Send(new GetIndexErrorsOperation(new[] { stats.IndexName }));
            Assert.Empty(indexErrors[0].Errors);
        }
        
        
    }

    private sealed record ComplexField(Query.Order Order, Query.Order Field, string Id = null);
}
