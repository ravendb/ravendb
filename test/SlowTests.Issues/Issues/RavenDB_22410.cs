using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_22410 : RavenTestBase
{
    public RavenDB_22410(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void SearchMethodWhenWordIsTransformedIntoMultipleTokensWeTreatThemAsPhraseQuery(Options options)
    {
        using var store = GetDocumentStore(options);
        using var session = store.OpenSession();
        session.Store(new Dto("Din ner"), "Dtos/1");
        session.Store(new Dto("Ner din"), "Dtos/2");
        session.SaveChanges();

        var results = session.Query<Dto>()
            .Customize(x => x.WaitForNonStaleResults())
            .Search(x => x.Search, "din%ner")
            .ToList();
        
        Assert.Equal(1, results.Count);
        Assert.Equal("Dtos/1", results[0].Id);
    }

    private record struct Dto(string Search, string Id = null);
}
