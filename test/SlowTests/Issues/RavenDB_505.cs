using FastTests;
using Raven.Client.Indexing;

using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_505 : RavenTestBase
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
                    store.DatabaseCommands.PutIndex("test", indexDefinition);
                    store.DatabaseCommands.DeleteIndex("test");
                }
            }
        }


    }
}
