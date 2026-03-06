using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Exceptions;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_23443 : RavenTestBase
{
    public RavenDB_23443(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Querying)]
    public void VectorSearchAndFilterShouldThrow()
    {
        const string expectedMessage = "Cannot use 'filter' when 'vector.search' is used in where statement.";
        
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                var ex = Assert.Throws<InvalidQueryException>(() => session.Query<Dto>()
                    .VectorSearch(x => x.WithText(p => p.Name), v => v.ByText("test"))
                    .Filter(x => x.Name == "test")
                    .ToList());

                Assert.Contains(expectedMessage, ex.Message);

                ex = Assert.Throws<InvalidQueryException>(() => session.Query<Dto>()
                    .Filter(x => x.Name == "test")
                    .VectorSearch(x => x.WithText(p => p.Name), v => v.ByText("test"))
                    .ToList());

                Assert.Contains(expectedMessage, ex.Message);
                
                ex = Assert.Throws<InvalidQueryException>(() => _ = session.Advanced.RawQuery<Dto>("from Dtos where vector.search(embedding.text(Name), $p0) filter (Name = $p1)")
                    .AddParameter("$p0", "test")
                    .AddParameter("$p1", "test")
                    .ToList());
                
                Assert.Contains(expectedMessage, ex.Message);
            }
        }
    }
    
    private class Dto
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }
}
