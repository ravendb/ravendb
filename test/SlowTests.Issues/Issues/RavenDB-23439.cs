using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_23439 : RavenTestBase
{
    public RavenDB_23439(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.ClientApi)]
    public void CanPerformProjectionWhenUsingVectorSearchViaLINQ()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        using var session = store.OpenSession();
        session.Store(new Dto1() { Vector = new[] { 1f, 2f, 3f } });
        session.SaveChanges();

        Dto2 singleOrDefault = session.Query<Dto1>().Customize(x => x.WaitForNonStaleResults())
            .VectorSearch(x => x.WithEmbedding(p => p.Vector), v => v.ByEmbedding(new[] { 1f, 2f, 3f }))
            .ProjectInto<Dto2>()
            .SingleOrDefault();

        Assert.NotNull(singleOrDefault);
    }
    
    private class Dto1
    {
        public float[] Vector { get; set; }
    }
    
    private class Dto2 // typeof(Dto1) != typeof(Dto2)
    {
        public float[] Vector { get; set; }
    }
}
