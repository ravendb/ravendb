using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Exceptions;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_23443 : RavenTestBase
{
    public RavenDB_23443(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Querying)]
    public void VectorSearchAndFilterShouldThrow()
    {
        const string expectedMessage = "Vector search is not supported in combination with filter.";
        
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                var ex = Assert.Throws<RavenException>(() => session.Query<Dto>()
                    .VectorSearch(x => x.WithText(p => p.Name), v => v.ByText("test"))
                    .Filter(x => x.Name == "test")
                    .ToList());

                Assert.Contains(expectedMessage, ex.InnerException?.Message);

                ex = Assert.Throws<RavenException>(() => session.Query<Dto>()
                    .Filter(x => x.Name == "test")
                    .VectorSearch(x => x.WithText(p => p.Name), v => v.ByText("test"))
                    .ToList());

                Assert.Contains(expectedMessage, ex.InnerException?.Message);
                
                ex = Assert.Throws<RavenException>(() => _ = session.Advanced.RawQuery<Dto>("from Dtos where vector.search(embedding.text(Name), $p0) filter (Name = $p1)")
                    .AddParameter("$p0", "test")
                    .AddParameter("$p1", "test")
                    .ToList());
                
                Assert.Contains(expectedMessage, ex.InnerException?.Message);
            }
        }
    }
    
    private class Dto
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }
}
