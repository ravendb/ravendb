using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_3864 : RavenTestBase
    {
        public RavenDB_3864(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void can_use_conventions_with_create_indexes_container()
        {
            using (var store = GetDocumentStore())
            {
                IndexCreation.CreateIndexes(new AbstractIndexCreationTask[] { new CustomIdInIndexCreationTask() }, store, store.Conventions);
                Assert.True(TestFailed.Value == false);
            }
        }

        [Fact]
        public async Task can_use_conventions_with_create_indexes_async_container()
        {
            using (var store = GetDocumentStore())
            {
                await IndexCreation.CreateIndexesAsync(new AbstractIndexCreationTask[] { new CustomIdInIndexCreationTask() }, store, store.Conventions);
                Assert.True(TestFailed.Value == false);
            }
        }

        [Fact]
        public void can_use_conventions_with_create_indexes()
        {
            using (var store = GetDocumentStore())
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
            using (var store = GetDocumentStore())
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
            using (var store = GetDocumentStore())
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
            using (var store = GetDocumentStore())
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

        private static readonly AsyncLocal<bool> TestFailed = new AsyncLocal<bool>();

        private class CustomIdInIndexCreationTask : AbstractIndexCreationTask<Data>
        {
            public CustomIdInIndexCreationTask()
            {
                Map = docs => from doc in docs select new { doc.CustomId };
            }

            public override IndexDefinition CreateIndexDefinition()
            {
                if (Conventions == null)
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
                if (Conventions == null )
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
