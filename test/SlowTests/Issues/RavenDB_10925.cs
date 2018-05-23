using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.Documents.Indexes.MapReduce.Auto;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_10925 : RavenTestBase
    {
        [Fact]
        public async Task SurpassedAutoMapWillBeDeleted()
        {
            using (var store = GetDocumentStore())
            {
                var database = await GetDatabase(store.Database);

                var i0 = await database.IndexStore.CreateIndex(new AutoMapIndexDefinition(
                    "Orders",
                    new[]
                    {
                        AutoIndexField.Create("Name", new AutoIndexDefinition.AutoIndexFieldOptions())
                    }));

                var i1 = await database.IndexStore.CreateIndex(new AutoMapIndexDefinition(
                    "Companies",
                    new[]
                    {
                        AutoIndexField.Create("Name", new AutoIndexDefinition.AutoIndexFieldOptions())
                    }));

                var i2 = await database.IndexStore.CreateIndex(new AutoMapIndexDefinition(
                    "Companies",
                    new[]
                    {
                        AutoIndexField.Create("Name", new AutoIndexDefinition.AutoIndexFieldOptions()),
                        AutoIndexField.Create("City", new AutoIndexDefinition.AutoIndexFieldOptions()),
                    }));

                database.IndexStore.RunIdleOperations();

                var indexes = database
                    .IndexStore
                    .GetIndexes()
                    .ToList();

                Assert.Equal(2, indexes.Count);
                Assert.Contains(i0, indexes);
                Assert.Contains(i2, indexes);

                var i3 = await database.IndexStore.CreateIndex(new AutoMapIndexDefinition(
                    "Companies",
                    new[]
                    {
                        AutoIndexField.Create("Name", new AutoIndexDefinition.AutoIndexFieldOptions
                        {
                            Indexing = AutoFieldIndexing.Search
                        }),
                        AutoIndexField.Create("City", new AutoIndexDefinition.AutoIndexFieldOptions()),
                    }));

                database.IndexStore.RunIdleOperations();

                indexes = database
                    .IndexStore
                    .GetIndexes()
                    .ToList();

                Assert.Equal(2, indexes.Count);
                Assert.Contains(i0, indexes);
                Assert.Contains(i3, indexes);

                var i4 = await database.IndexStore.CreateIndex(new AutoMapIndexDefinition(
                    "Companies",
                    new[]
                    {
                        AutoIndexField.Create("Name", new AutoIndexDefinition.AutoIndexFieldOptions
                        {
                            Indexing = AutoFieldIndexing.Highlighting | AutoFieldIndexing.Search
                        }),
                        AutoIndexField.Create("City", new AutoIndexDefinition.AutoIndexFieldOptions()),
                    }));

                database.IndexStore.RunIdleOperations();

                indexes = database
                    .IndexStore
                    .GetIndexes()
                    .ToList();

                Assert.Equal(2, indexes.Count);
                Assert.Contains(i0, indexes);
                Assert.Contains(i4, indexes);

                var i5 = await database.IndexStore.CreateIndex(new AutoMapIndexDefinition(
                    "Companies",
                    new[]
                    {
                        AutoIndexField.Create("Name", new AutoIndexDefinition.AutoIndexFieldOptions
                        {
                            Indexing = AutoFieldIndexing.Exact
                        }),
                        AutoIndexField.Create("City", new AutoIndexDefinition.AutoIndexFieldOptions()),
                    }));

                database.IndexStore.RunIdleOperations();

                indexes = database
                    .IndexStore
                    .GetIndexes()
                    .ToList();

                Assert.Equal(3, indexes.Count);
                Assert.Contains(i0, indexes);
                Assert.Contains(i4, indexes);
                Assert.Contains(i5, indexes);
            }
        }

        [Fact]
        public async Task SurpassedAutoMapReduceWillBeDeleted()
        {
            using (var store = GetDocumentStore())
            {
                var database = await GetDatabase(store.Database);

                var i0 = await database.IndexStore.CreateIndex(new AutoMapReduceIndexDefinition(
                    "Orders",
                    new[]
                    {
                        AutoIndexField.Create("City", new AutoIndexDefinition.AutoIndexFieldOptions())
                    },
                    new[]
                    {
                        AutoIndexField.Create("Name", new AutoIndexDefinition.AutoIndexFieldOptions
                        {
                            GroupByArrayBehavior = GroupByArrayBehavior.ByIndividualValues
                        })
                    }));

                var i1 = await database.IndexStore.CreateIndex(new AutoMapReduceIndexDefinition(
                    "Companies",
                    new[]
                    {
                        AutoIndexField.Create("City", new AutoIndexDefinition.AutoIndexFieldOptions())
                    },
                    new[]
                    {
                        AutoIndexField.Create("Name", new AutoIndexDefinition.AutoIndexFieldOptions
                        {
                            GroupByArrayBehavior = GroupByArrayBehavior.ByIndividualValues
                        })
                    }));

                var i2 = await database.IndexStore.CreateIndex(new AutoMapReduceIndexDefinition(
                    "Companies",
                    new[]
                    {
                        AutoIndexField.Create("Count", new AutoIndexDefinition.AutoIndexFieldOptions()),
                        AutoIndexField.Create("City", new AutoIndexDefinition.AutoIndexFieldOptions())
                    },
                    new[]
                    {
                        AutoIndexField.Create("Name", new AutoIndexDefinition.AutoIndexFieldOptions
                        {
                            GroupByArrayBehavior = GroupByArrayBehavior.ByIndividualValues
                        })
                    }));

                var i3 = await database.IndexStore.CreateIndex(new AutoMapReduceIndexDefinition(
                    "Companies",
                    new[]
                    {
                        AutoIndexField.Create("Count", new AutoIndexDefinition.AutoIndexFieldOptions()),
                        AutoIndexField.Create("City", new AutoIndexDefinition.AutoIndexFieldOptions()),
                        AutoIndexField.Create("State", new AutoIndexDefinition.AutoIndexFieldOptions())
                    },
                    new[]
                    {
                        AutoIndexField.Create("Name", new AutoIndexDefinition.AutoIndexFieldOptions
                        {
                            GroupByArrayBehavior = GroupByArrayBehavior.ByContent
                        })
                    }));

                database.IndexStore.RunIdleOperations();

                var indexes = database
                    .IndexStore
                    .GetIndexes()
                    .ToList();

                Assert.Equal(3, indexes.Count);
                Assert.Contains(i0, indexes);
                Assert.Contains(i2, indexes);
                Assert.Contains(i3, indexes);
            }
        }
    }
}
