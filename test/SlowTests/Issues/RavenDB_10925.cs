using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.MapReduce.Auto;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_10925 : RavenTestBase
    {
        [Fact]
        public async Task CanCreateTwoSimilarMapReduceIndexesThatAreGroupingArrayUsingDifferentBehavior()
        {
            using (var store = GetDocumentStore())
            {
                var database = await GetDatabase(store.Database);

                var i0 = await database.IndexStore.CreateIndex(new AutoMapReduceIndexDefinition(
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

                var i1 = await database.IndexStore.CreateIndex(new AutoMapReduceIndexDefinition(
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
                            GroupByArrayBehavior = GroupByArrayBehavior.ByContent
                        })
                    }));

                Assert.Equal("Auto/Companies/ByCityAndCountReducedByArray(Name)", i1.Name);

                database.IndexStore.RunIdleOperations();

                var indexes = database
                    .IndexStore
                    .GetIndexes()
                    .ToList();

                Assert.Equal(2, indexes.Count);
                Assert.Contains(i0, indexes);
                Assert.Contains(i1, indexes);
            }
        }
    }
}
