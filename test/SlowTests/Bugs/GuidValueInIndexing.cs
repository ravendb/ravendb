using FastTests;
using Raven.Client.Documents.Operations.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Bugs
{
    public class GuidValueInIndexing : RavenTestBase
    {
        public GuidValueInIndexing(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanBeUsed()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new CreateSampleDataOperation());

                var indexDef = new Raven.Client.Documents.Indexes.IndexDefinition
                {
                    Name = "Test",
                    Maps =
                    {
                        "from s in docs.Employees select new { A = Guid.NewGuid() }"
                    }
                };
                store.Maintenance.Send(new PutIndexesOperation(new[] { indexDef }));

                Indexes.WaitForIndexing(store);

                var stats = Indexes.WaitForIndexingErrors(store, errorsShouldExists: false);
                Assert.Null(stats);

            }
        }
    }
}
