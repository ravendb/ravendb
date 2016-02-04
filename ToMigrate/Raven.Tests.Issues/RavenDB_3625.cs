using System;
using System.Collections.Generic;
using System.ComponentModel.Composition.Hosting;
using System.Linq;
using System.Reflection;
using System.Security.Policy;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_3625: RavenTest
    {
        [Fact]
        public void CanExecuteMultipleIndexes()
        {
            using (var store = NewDocumentStore(databaseName:"MultiIndexes"))
            {
                IndexCreation.CreateIndexes(new CompositionContainer(new TypeCatalog(typeof(TestStrIndex),typeof(TestIntIndex))), store);
                var names = store.DatabaseCommands.GetIndexNames(0,3);
                Assert.Contains("TestIntIndex", names);
                Assert.Contains("TestStrIndex", names);
            }
        }

        [Fact]
        public async Task CanExecuteMultipleIndexesAsync()
        {
            using (var store = NewDocumentStore(databaseName: "MultiIndexes"))
            {
                await IndexCreation.CreateIndexesAsync(new CompositionContainer(new TypeCatalog(typeof(TestStrIndex),typeof(TestIntIndex))), store).ConfigureAwait(false);
                var names = store.DatabaseCommands.GetIndexNames(0, 3);
                Assert.Contains("TestIntIndex", names);
                Assert.Contains("TestStrIndex", names);
            }
        }

        public class TestIntIndex : AbstractIndexCreationTask<Data>
        {
            public TestIntIndex()
            {
                Map = docs => from doc in docs select new { doc.Int };
            }
        }
        public class TestStrIndex : AbstractIndexCreationTask<Data>
        {
            public TestStrIndex()
            {
                Map = docs => from doc in docs select new { doc.Str };
            }
        }

        public class Data
        {
            public int Int { get; set; }
            public string Str { get; set; }
        }
    }
}
