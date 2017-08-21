using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_3647 : RavenTestBase
    {
        [Fact]
        public void CanLockIndexes()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new SimpleIndex());
                //Checking that we can lock index
                store.Admin.Send(new SetIndexLockOperation("SimpleIndex", IndexLockMode.LockedIgnore));
                var indexDefinition = store.Admin.Send(new GetIndexOperation("SimpleIndex"));
                var map = indexDefinition.Maps.First();
                Assert.Equal(indexDefinition.LockMode, IndexLockMode.LockedIgnore);
                //Checking that we can't change a locked index
                indexDefinition.Maps = new HashSet<string> { NewMap };
                store.Admin.Send(new PutIndexesOperation(indexDefinition));
                WaitForIndexing(store);
                indexDefinition = store.Admin.Send(new GetIndexOperation("SimpleIndex"));
                Assert.Equal(indexDefinition.Maps.First(), map);
                //Checking that we can unlock a index
                store.Admin.Send(new SetIndexLockOperation("SimpleIndex", IndexLockMode.Unlock));
                indexDefinition = store.Admin.Send(new GetIndexOperation("SimpleIndex"));
                Assert.Equal(indexDefinition.LockMode, IndexLockMode.Unlock);
                //checking that the index is indeed overridden
                indexDefinition.Maps = new HashSet<string> { NewMap };
                store.Admin.Send(new PutIndexesOperation(indexDefinition));
                WaitForIndexing(store);
                indexDefinition = store.Admin.Send(new GetIndexOperation("SimpleIndex"));
                Assert.Equal(NewMap, indexDefinition.Maps.First());
            }
        }

        private class SimpleData
        {
            public string Id { get; set; }

            public int Number { get; set; }
        }

        private const string NewMap = "from doc in docs.SimpleDatas select new { Id = doc.Id, Number = doc.Number };";

        private class SimpleIndex : AbstractIndexCreationTask<SimpleData>
        {
            public SimpleIndex()
            {
                Map = docs => from doc in docs
                              select new
                              {
                                  doc.Number
                              };
            }
        }
    }
}
