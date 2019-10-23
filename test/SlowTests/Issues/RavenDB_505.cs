using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_505 : RavenTestBase
    {
        public RavenDB_505(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CreateDeleteCreateIndex()
        {
            using (var store = GetDocumentStore())
            {
                var indexDefinition = new IndexDefinition
                {
                    Name = "test",
                    Maps = { "from d in docs select new {d.Name}" }
                };
                for (int i = 0; i < 10; i++)
                {
                    store.Maintenance.Send(new PutIndexesOperation(new[] { indexDefinition}));
                    store.Maintenance.Send(new DeleteIndexOperation("test"));
                }
            }
        }
    }
}
