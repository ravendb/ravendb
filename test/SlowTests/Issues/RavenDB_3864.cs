using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_3864 : RavenTestBase
    {
        [Fact]
        public void can_use_conventions_with_create_indexes_container()
        {
            using (var store = CreateDocumentStore())
            {
                IndexCreation.CreateIndexes(new AbstractIndexCreationTask[] { new CustomIdInIndexCreationTask() }, store, store.Conventions);
                Assert.True(TestFailed.Value == false);
            }
        }

        [Fact]
        public async Task can_use_conventions_with_create_indexes_async_container()
        {
            using (var store = CreateDocumentStore())
            {
                await IndexCreation.CreateIndexesAsync(new AbstractIndexCreationTask[] { new CustomIdInIndexCreationTask() }, store, store.Conventions);
                Assert.True(TestFailed.Value == false);
            }
        }

        [Fact]
        public void can_use_conventions_with_create_indexes()
        {
            using (var store = CreateDocumentStore())
            {
                var list = new List<AbstractIndexCreationTask>
                {
                    new CustomIdInIndexCreationTask(),
                    new CustomIdWithNameInIndexCreationTask()
                };

                store.ExecuteIndexes(list);
                Assert.True(TestFailed.Value == false);
            }
        }

        [Fact]
        public async Task can_use_conventions_with_create_indexes_async()
        {
            using (var store = CreateDocumentStore())
            {
                var list = new List<AbstractIndexCreationTask>
                {
                    new CustomIdInIndexCreationTask(),
                    new CustomIdWithNameInIndexCreationTask()
                };

                await store.ExecuteIndexesAsync(list);
                Assert.True(TestFailed.Value == false);
            }
        }

        [Fact]
        public void can_use_conventions_with_create_side_by_side_indexes()
        {
            using (var store = CreateDocumentStore())
            {
                var list = new List<AbstractIndexCreationTask>
                {
                    new CustomIdInIndexCreationTask(),
                    new CustomIdWithNameInIndexCreationTask()
                };

                store.ExecuteIndexes(list);
                Assert.True(TestFailed.Value == false);
            }
        }

        [Fact]
        public async Task can_use_conventions_with_create_side_by_side_indexes_async()
        {
            using (var store = CreateDocumentStore())
            {
                var list = new List<AbstractIndexCreationTask>
                {
                    new CustomIdInIndexCreationTask(),
                    new CustomIdWithNameInIndexCreationTask()
                };

                await store.ExecuteIndexesAsync(list);
                Assert.True(TestFailed.Value == false);
            }
        }

        private DocumentStore CreateDocumentStore([CallerMemberName] string caller = null)
        {
            return GetDocumentStore(new Options
            {
                ModifyDocumentStore = store => store.Conventions.PrettifyGeneratedLinqExpressions = false
            }, caller);
        }

        private static readonly AsyncLocal<bool> TestFailed = new AsyncLocal<bool>();

        private class CustomIdInIndexCreationTask : AbstractIndexCreationTask<Data>
        {
            public CustomIdInIndexCreationTask()
            {
                Map = docs => from doc in docs select new { doc.CustomId };
            }

            public override IndexDefinition CreateIndexDefinition()
            {
                if (Conventions == null || Conventions.PrettifyGeneratedLinqExpressions)
                    TestFailed.Value = true;

                return base.CreateIndexDefinition();
            }
        }

        private class CustomIdWithNameInIndexCreationTask : AbstractIndexCreationTask<Data>
        {
            public CustomIdWithNameInIndexCreationTask()
            {
                Map = docs => from doc in docs
                              select new
                              {
                                  doc.CustomId,
                                  doc.Name
                              };
            }

            public override IndexDefinition CreateIndexDefinition()
            {
                if (Conventions == null || Conventions.PrettifyGeneratedLinqExpressions)
                    TestFailed.Value = true;

                return base.CreateIndexDefinition();
            }
        }

        private class Data
        {
            public string CustomId { get; set; }
            public string Name { get; set; }
        }
    }
}
