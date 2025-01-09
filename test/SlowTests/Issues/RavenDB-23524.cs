using System.Threading.Tasks;
using FastTests;
using Orders;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_23524(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Vector)]
    public async Task VectorSearchWillNotOverrideTheAlreadyCalculatedScore()
    {
        const string spaghetti = "spaghetti";
        const string rome = "rome";
        const string cannoli = "cannoli";

        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        using var session = store.OpenAsyncSession();

        await session.StoreAsync(new Product() { Name = spaghetti });
        await session.StoreAsync(new Product() { Name = rome });
        await session.StoreAsync(new Product() { Name = cannoli });
        await session.SaveChangesAsync();

        var results = await session.Advanced.AsyncDocumentQuery<Product>()
            .WaitForNonStaleResults()
            .VectorSearch(f => f.WithText(p => p.Name),
                v => v.ByText("italian"))
            .OrElse()
            .VectorSearch(f => f.WithText(p => p.Name),
                v => v.ByText("food"))
            .ToListAsync();

        Assert.Equal(3, results.Count);
        Assert.Equal(spaghetti, results[0].Name);
        Assert.Equal(cannoli, results[1].Name);
        Assert.Equal(rome, results[2].Name);

        var exception = await Assert.ThrowsAsync<Raven.Client.Exceptions.RavenException>(() => session.Advanced.AsyncDocumentQuery<Product>()
            .VectorSearch(f => f.WithText(p => p.Name),
                v => v.ByText("italian")).Boost(10)
            .ToListAsync());
        
        Assert.Contains("System.NotSupportedException: Boosting the VectorSearchMatch is not supported yet.", exception.Message);
    }
}
