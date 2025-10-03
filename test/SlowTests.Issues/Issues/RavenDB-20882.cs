using FastTests;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_20882 : RavenTestBase
{
    public RavenDB_20882(ITestOutputHelper output) : base(output)
    {
    }

    
    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene, DatabaseMode = RavenDatabaseMode.All)]
    public void AlphanumericalSortCanHandleSurrogateCharacters(Options options)
    {
        var dto6 = new Dto("Rocket1");
        var dto1 = new Dto("Rocket🚀");
        var dto5 = new Dto("Rocket🚀2");
        var dto4 = new Dto("Rocket🚀3");
        var dto3 = new Dto("Rocket🚀🚀");
        var dto2 = new Dto("WithoutEmoji");
        
        using var store = GetDocumentStore(options);
        using (var session = store.OpenSession())
        {
            session.Store(dto6);
            session.Store(dto2);
            session.Store(dto1);
            session.Store(dto3);
            session.Store(dto4);
            session.Store(dto5);
            
            session.SaveChanges();

            var results = session.Advanced.DocumentQuery<Dto>().OrderBy(i => i.Text, OrderingType.AlphaNumeric).WaitForNonStaleResults().ToList();
            var idX = 0;
            Assert.Equal(dto6.Id, results[idX++].Id);
            Assert.Equal(dto1.Id, results[idX++].Id);
            Assert.Equal(dto5.Id, results[idX++].Id);
            Assert.Equal(dto4.Id, results[idX++].Id);

            Assert.Equal(dto3.Id, results[idX++].Id);

            Assert.Equal(dto2.Id, results[idX++].Id);
        }
        
    }

    private record Dto(string Text, string Id = null);
}
