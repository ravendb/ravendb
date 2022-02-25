using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.Documents.Indexes.MapReduce.Auto;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_10925 : RavenTestBase
    {
        public RavenDB_10925(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task SurpassedAutoMapWillBeDeletedOrMerged()
        {
            using (var store = GetDocumentStore())
            {
                var database = await GetDatabase(store.Database);

                var i0 = await database.IndexStore.CreateIndex(new AutoMapIndexDefinition(
                    "Orders",
                    new[]
                    {
                        AutoIndexField.Create("Name", new AutoIndexDefinition.AutoIndexFieldOptions())
                    }), Guid.NewGuid().ToString());

                var i1 = await database.IndexStore.CreateIndex(new AutoMapIndexDefinition(
                    "Companies",
                    new[]
                    {
                        AutoIndexField.Create("Name", new AutoIndexDefinition.AutoIndexFieldOptions())
                    }), Guid.NewGuid().ToString());

                var i2 = await database.IndexStore.CreateIndex(new AutoMapIndexDefinition(
                    "Companies",
                    new[]
                    {
                        AutoIndexField.Create("Name", new AutoIndexDefinition.AutoIndexFieldOptions()),
                        AutoIndexField.Create("City", new AutoIndexDefinition.AutoIndexFieldOptions()),
                    }), Guid.NewGuid().ToString());

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
                    }), Guid.NewGuid().ToString());

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
                    }), Guid.NewGuid().ToString());

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
                    }), Guid.NewGuid().ToString());

                database.IndexStore.RunIdleOperations(); // it will merge the i5
                database.IndexStore.RunIdleOperations(); // need to run twice since we are extending one index at a time

                indexes = database
                    .IndexStore
                    .GetIndexes()
                    .ToList();

                Assert.Equal(2, indexes.Count);
                Assert.Contains(i0, indexes);

                var merged = indexes.Single(x => x != i0);
                var definition = (AutoIndexDefinitionBaseServerSide)merged.Definition;

                var nameField = (AutoIndexField)definition.MapFields["Name"];
                Assert.Equal(AutoFieldIndexing.Default | AutoFieldIndexing.Exact | AutoFieldIndexing.Search | AutoFieldIndexing.Highlighting, nameField.Indexing);

                var cityField = (AutoIndexField)definition.MapFields["City"];
                Assert.Equal(AutoFieldIndexing.Default, cityField.Indexing);
            }
        }

        [Fact]
        public async Task SurpassedAutoMapReduceWillBeDeletedOrMerged()
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
                    }), Guid.NewGuid().ToString());

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
                    }), Guid.NewGuid().ToString());

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
                    }), Guid.NewGuid().ToString());

                var i3 = await database.IndexStore.CreateIndex(new AutoMapReduceIndexDefinition(
                    "Companies",
                    new[]
                    {
                        AutoIndexField.Create("Count", new AutoIndexDefinition.AutoIndexFieldOptions()),
                        AutoIndexField.Create("City", new AutoIndexDefinition.AutoIndexFieldOptions()),
                        AutoIndexField.Create("State", new AutoIndexDefinition.AutoIndexFieldOptions()
                        {
                            Indexing = AutoFieldIndexing.Exact
                        })
                    },
                    new[]
                    {
                        AutoIndexField.Create("Name", new AutoIndexDefinition.AutoIndexFieldOptions
                        {
                            GroupByArrayBehavior = GroupByArrayBehavior.ByContent
                        })
                    }), Guid.NewGuid().ToString());

                Assert.Equal("Auto/Companies/ByCityAndCountAndExact(State)ReducedByArray(Name)", i3.Name);

                database.IndexStore.RunIdleOperations();

                var indexes = database
                    .IndexStore
                    .GetIndexes()
                    .ToList();

                Assert.Equal(3, indexes.Count);
                Assert.Contains(i0, indexes);
                Assert.Contains(i2, indexes);
                Assert.Contains(i3, indexes);

                var i4 = await database.IndexStore.CreateIndex(new AutoMapReduceIndexDefinition(
                    "Companies",
                    new[]
                    {
                        AutoIndexField.Create("Count", new AutoIndexDefinition.AutoIndexFieldOptions()),
                        AutoIndexField.Create("City", new AutoIndexDefinition.AutoIndexFieldOptions()),
                        AutoIndexField.Create("State", new AutoIndexDefinition.AutoIndexFieldOptions
                        {
                            Indexing = AutoFieldIndexing.Search
                        })
                    },
                    new[]
                    {
                        AutoIndexField.Create("Name", new AutoIndexDefinition.AutoIndexFieldOptions
                        {
                            GroupByArrayBehavior = GroupByArrayBehavior.ByContent
                        })
                    }), Guid.NewGuid().ToString());

                database.IndexStore.RunIdleOperations(); // i4 should be merged
                database.IndexStore.RunIdleOperations(); // need to run twice since we are extending one index at a time

                indexes = database
                    .IndexStore
                    .GetIndexes()
                    .ToList();

                Assert.Equal(3, indexes.Count);
                Assert.Contains(i0, indexes);
                Assert.Contains(i2, indexes);

                var merged = indexes.Single(x => x != i0 && x != i2);
                var definition = (AutoMapReduceIndexDefinition)merged.Definition;

                var countField = (AutoIndexField)definition.MapFields["Count"];
                Assert.Equal(AutoFieldIndexing.Default, countField.Indexing);

                var cityField = (AutoIndexField)definition.MapFields["City"];
                Assert.Equal(AutoFieldIndexing.Default, cityField.Indexing);

                var stateField = (AutoIndexField)definition.MapFields["State"];
                Assert.Equal(AutoFieldIndexing.Default | AutoFieldIndexing.Exact | AutoFieldIndexing.Search, stateField.Indexing);

                var nameField = definition.GroupByFields["Name"];
                Assert.Equal(AutoFieldIndexing.Default, nameField.Indexing);
                Assert.Equal(GroupByArrayBehavior.ByContent, nameField.GroupByArrayBehavior);
            }
        }
    }
}
