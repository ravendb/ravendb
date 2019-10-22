using FastTests;
using Raven.Client.Documents.Queries;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_8805 : RavenTestBase
    {
        public RavenDB_8805(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void ShouldNotThrow()
        {
            using (var store = GetDocumentStore())
            {
                using (var commands = store.Commands())
                {
                    commands.Query(new IndexQuery
                    {
                        Query = "from Orders where Freight in (11.61)"
                    });
                }
            }
        }
    }
}
