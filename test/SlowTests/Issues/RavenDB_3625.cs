using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_3625 : RavenTestBase
    {
        [Fact]
        public void CanExecuteMultipleIndexes()
        {
            using (var store = GetDocumentStore())
            {
                IndexCreation.CreateIndexes(new List<AbstractIndexCreationTask> { new TestStrIndex(), new TestIntIndex() }, store);
                var names = store.Admin.Send(new GetIndexNamesOperation(0, 3));

                Assert.Equal(2, names.Length);
                Assert.Contains("TestIntIndex", names);
                Assert.Contains("TestStrIndex", names);
            }
        }

        [Fact]
        public async Task CanExecuteMultipleIndexesAsync()
        {
            using (var store = GetDocumentStore())
            {
                await IndexCreation.CreateIndexesAsync(new List<AbstractIndexCreationTask> { new TestStrIndex(), new TestIntIndex() }, store);
                var names = await store.Admin.SendAsync(new GetIndexNamesOperation(0, 3));

                Assert.Equal(2, names.Length);
                Assert.Contains("TestIntIndex", names);
                Assert.Contains("TestStrIndex", names);
            }
        }

        private class TestIntIndex : AbstractIndexCreationTask<Data>
        {
            public TestIntIndex()
            {
                Map = docs => from doc in docs select new { doc.Int };
            }
        }

        private class TestStrIndex : AbstractIndexCreationTask<Data>
        {
            public TestStrIndex()
            {
                Map = docs => from doc in docs select new { doc.Str };
            }
        }

        private class Data
        {
            public int Int { get; set; }
            public string Str { get; set; }
        }
    }
}
