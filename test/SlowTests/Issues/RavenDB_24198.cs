using System.Linq;
using FastTests;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_24198 : RavenTestBase
{
    public RavenDB_24198(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void WhereInWithExistStatementShouldWork(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                for (var i = 0; i < 20; i++)
                {
                    session.Store(new Dto() { Names = ["spaces name", "other name"], Category = $"Category/{i}" });
                }
                
                session.SaveChanges();
                
                var inTerms = Enumerable.Range(0, 10)
                    .Select(i => $"Category/{i}")
                    .ToList();

                var res = session.Advanced.DocumentQuery<Dto>()
                    .WhereIn(x => x.Category, inTerms)
                    .AndAlso()
                    .WhereExists(x => x.Names)
                    .ToList();
                
                Assert.Equal(10, res.Count);
            }
        }
    }

    private class Dto
    {
        public string[] Names { get; set; }
        public string Category { get; set; }
    }
}
