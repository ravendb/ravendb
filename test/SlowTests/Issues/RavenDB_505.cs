using FastTests;
using Raven.NewClient.Client.Indexing;
using Raven.NewClient.Operations.Databases.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_505 : RavenNewTestBase
    {
        [Fact]
        public void CreateDeleteCreateIndex()
        {
            using (var store = GetDocumentStore())
            {
                var indexDefinition = new IndexDefinition
                {
                    Maps = { "from d in docs select new {d.Name}" }
                };
                for (int i = 0; i < 10; i++)
                {
                    store.Admin.Send(new PutIndexOperation("test", indexDefinition));
                    store.Admin.Send(new DeleteIndexOperation("test"));
                }
            }
        }
    }
}
