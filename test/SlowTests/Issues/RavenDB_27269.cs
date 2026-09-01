using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config;
using Raven.Server.Config.Attributes;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Configuration;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_27269(ITestOutputHelper output) : RavenLowLevelTestBase(output)
    {
        [RavenFact(RavenTestCategory.Configuration | RavenTestCategory.Indexes)]
        public void PerIndexSettingsMustDeclareHowTheIndexIsUpdated()
        {
            var offenders = new List<string>();

            foreach (var property in typeof(SingleIndexConfiguration).GetProperties(BindingFlags.Public | BindingFlags.Instance))
            {
                var entry = property.GetCustomAttributes<ConfigurationEntryAttribute>().OrderBy(x => x.Order).FirstOrDefault();
                if (entry == null || entry.Scope != ConfigurationEntryScope.ServerWideOrPerDatabaseOrPerIndex)
                    continue;

                var updateType = property.GetCustomAttribute<IndexUpdateTypeAttribute>();

                if (updateType == null)
                {
                    offenders.Add($"{entry.Key} - no {nameof(IndexUpdateTypeAttribute)}");
                    continue;
                }

                if (updateType.UpdateType == IndexUpdateType.None)
                    offenders.Add($"{entry.Key} - {nameof(IndexUpdateType)}.{nameof(IndexUpdateType.None)}");
            }

            Assert.True(offenders.Count == 0,
                $"Per-index settings must be marked {nameof(IndexUpdateType)}.{nameof(IndexUpdateType.Refresh)} or " +
                $"{nameof(IndexUpdateType)}.{nameof(IndexUpdateType.Reset)}, otherwise changing them rebuilds the index " +
                $"side by side for nothing:{Environment.NewLine}{string.Join(Environment.NewLine, offenders)}");
        }

        [RavenFact(RavenTestCategory.Configuration | RavenTestCategory.Indexes)]
        public async Task ChangingHnswBuildParallelismDoesNotRebuildTheIndex()
        {
            using (var database = CreateDocumentDatabase())
            {
                var index = await database.IndexStore.CreateIndex(CreateIndexDefinition(), Guid.NewGuid().ToString());
                Assert.NotNull(index);

                var changed = CreateIndexDefinition();
                changed.Configuration[RavenConfiguration.GetKey(x => x.Indexing.MaximumConcurrentBatchesForHnswAcceleration)] = "256";

                var options = IndexStore.GetIndexCreationOptions(changed, index.Instance.ToIndexInformationHolder(), database.Configuration, out var differences);

                Assert.Equal(IndexDefinitionCompareDifferences.Configuration, differences);
                Assert.Equal(IndexCreationOptions.UpdateWithoutUpdatingCompiledIndex, options);
            }
        }

        [RavenFact(RavenTestCategory.Configuration | RavenTestCategory.Indexes)]
        public async Task NoneSettingDoesNotDowngradeARefreshSettingChangedAlongWithIt()
        {
            using (var database = CreateDocumentDatabase())
            {
                var index = await database.IndexStore.CreateIndex(CreateIndexDefinition(), Guid.NewGuid().ToString());
                Assert.NotNull(index);

                var changed = CreateIndexDefinition();
                changed.Configuration[RavenConfiguration.GetKey(x => x.Indexing.HistoryRevisionsNumber)] = "21234";
                changed.Configuration[RavenConfiguration.GetKey(x => x.Indexing.MapTimeout)] = "30";

                var currentConfiguration = (SingleIndexConfiguration)index.Instance.Configuration;
                var newConfiguration = new SingleIndexConfiguration(changed.Configuration, database.Configuration);

                Assert.Equal(IndexUpdateType.Refresh, currentConfiguration.CalculateUpdateType(newConfiguration));

                var options = IndexStore.GetIndexCreationOptions(changed, index.Instance.ToIndexInformationHolder(), database.Configuration, out _);
                Assert.Equal(IndexCreationOptions.UpdateWithoutUpdatingCompiledIndex, options);
            }
        }

        private static IndexDefinition CreateIndexDefinition() => new()
        {
            Name = "Users_ByName",
            Maps = { "from user in docs.Users select new { user.Name }" },
            Type = IndexType.Map
        };
    }
}
