using System.Linq;
using FastTests;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Corax;

public class RavenDB_21177 : RavenTestBase
{
    public RavenDB_21177(ITestOutputHelper output) : base(output)
    {
    }
    
    [Fact]
    public void CoraxSortingPostingLists()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        using (var bulk = store.BulkInsert())
        {
            for (int i = 0; i < 63_000; ++i)
            {
                bulk.Store(new Dto($"{i % 5}"));
            }
        }

        using (var session = store.OpenSession())
        {
            var allDocsSortedByCorax = session.Advanced
                .DocumentQuery<Dto>()
                .WaitForNonStaleResults()
                .OrderBy(nameof(Dto.Num), OrderingType.String)
                .ToList();

            var allDocsFromServer = session.Advanced.DocumentQuery<Dto>().ToList();
           // WaitForUserToContinueTheTest(store);
           Assert.Equal(63_000, allDocsSortedByCorax.Count); 
           Assert.Equal(63_000, allDocsFromServer.Count); 

           Assert.Equal(allDocsFromServer.Select(i => i.Num).OrderBy(i => i), allDocsSortedByCorax.Select(i => i.Num));
        }
    }
    
    private record Dto(string Num, string Id = null);
}
