using FastTests;
using Raven.Client.Documents.Operations.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Bugs
{
    public class GuidValueInIndexing : RavenTestBase
    {
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

                WaitForIndexing(store);

                var stats = store.Maintenance.Send(new GetIndexErrorsOperation(new[] { "Test" }));
                Assert.Empty(stats[0].Errors);

            }
        }
    }
}
