using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_22086 : RavenTestBase
{
    public RavenDB_22086(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void Test(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                var d1 = new Dto() { Name = "CoolName", SomeValue = 21 };
                var d2 = new Dto() { Name = "CoolerName", SomeValue = 37 };
                
                session.Store(d1);
                session.Store(d2);
                
                session.SaveChanges();

                var resultsByScoreAscending = session.Query<Dto>().Where(x => x.Name == "CoolName" || x.SomeValue > 20).OrderByScore().ToList();
                
                Assert.Equal(2, resultsByScoreAscending.Count);
                
                Assert.Equal("CoolName", resultsByScoreAscending[0].Name);
                Assert.Equal("CoolerName", resultsByScoreAscending[1].Name);

                var resultsByScoreDescending = session.Query<Dto>().Where(x => x.Name == "CoolName" || x.SomeValue > 20).OrderByScoreDescending().ToList();
                
                Assert.Equal(2, resultsByScoreDescending.Count);
                
                Assert.Equal("CoolerName", resultsByScoreDescending[0].Name);
                Assert.Equal("CoolName", resultsByScoreDescending[1].Name);
            }
        }
    }
    
    private class DummyIndex : AbstractIndexCreationTask<Dto>
    {
        public DummyIndex()
        {
            Map = dtos => from dto in dtos
                select new {  };
        }
    }
    
    private class Dto
    {
        public string Name { get; set; }
        public int SomeValue { get; set; }
    }
}
