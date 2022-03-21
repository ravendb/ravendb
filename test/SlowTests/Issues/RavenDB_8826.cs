using FastTests;
using Raven.Client.Documents.Queries;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_8826 : RavenTestBase
    {
        public RavenDB_8826(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanUseEscapedFields()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new CreateSampleDataOperation());

                Indexes.WaitForIndexing(store);

                using (var commands = store.Commands())
                {
                    var result = commands.Query(new IndexQuery
                    {
                        Query = "FROM @all_docs WHERE exists('ExternalId')",
                        WaitForNonStaleResults = true
                    });

                    Assert.Equal(91, result.Results.Length);

                    result = commands.Query(new IndexQuery
                    {
                        Query = "FROM @all_docs WHERE boost('ExternalId' = 'ALFKI', 10)",
                        WaitForNonStaleResults = true
                    });

                    Assert.Equal(1, result.Results.Length);
                }
            }
        }
    }
}
