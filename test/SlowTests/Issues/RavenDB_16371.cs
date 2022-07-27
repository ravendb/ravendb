using System;
using FastTests;
using Orders;
using Raven.Server.Config;
using Raven.Server.Config.Categories;
using System.Linq;
using System.Threading;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using SlowTests.Core.Utils.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16371 : RavenTestBase
    {
        public RavenDB_16371(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [InlineData(IndexingConfiguration.IndexStartupBehaviorType.Default)]
        [InlineData(IndexingConfiguration.IndexStartupBehaviorType.Delay)]
        [InlineData(IndexingConfiguration.IndexStartupBehaviorType.Immediate)]
        [InlineData(IndexingConfiguration.IndexStartupBehaviorType.Pause)]
        public void IndexStartupBehaviorType_Should_Work_Correctly(IndexingConfiguration.IndexStartupBehaviorType type)
        {
            DoNotReuseServer();

            using (var store = GetDocumentStore(new Options
            {
                RunInMemory = false,
                ModifyDatabaseRecord = r => r.Settings[RavenConfiguration.GetKey(x => x.Indexing.IndexStartupBehavior)] = type.ToString()
            }))
            {
                string autoIndexName;
                using (var session = store.OpenSession())
                {
                    session.Query<Company>()
                        .Statistics(out var stats)
                        .Where(x => x.Name == "HR")
                        .ToList(); // create auto-index

                    autoIndexName = stats.IndexName;
                }

                var index = new Companies_CompanyByType();
                index.Execute(store);
                var staticIndexName = index.IndexName;

                // IndexStartupBehaviorType should not affect newly created indexes
                var indexStats = store.Maintenance.Send(new GetIndexStatisticsOperation(autoIndexName));
                Assert.Equal(IndexRunningStatus.Running, indexStats.Status);

                indexStats = store.Maintenance.Send(new GetIndexStatisticsOperation(staticIndexName));
                Assert.Equal(IndexRunningStatus.Running, indexStats.Status);

                Server.ServerStore.DatabasesLandlord.UnloadDirectly(store.Database);

                if (type == IndexingConfiguration.IndexStartupBehaviorType.Delay)
                {
                    Server.ServerStore.DatabasesLandlord.ForTestingPurposesOnly().OnBeforeDocumentDatabaseInitialization = database =>
                    {
                        database.IndexStore.ForTestingPurposesOnly().AfterIndexesOpen = () =>
                        {
                            foreach (var index in database.IndexStore.GetIndexes())
                                Assert.Equal(IndexRunningStatus.Paused, index.Status);
                        };
                    };
                }

                IndexRunningStatus expectedStatus;
                switch (type)
                {
                    case IndexingConfiguration.IndexStartupBehaviorType.Default:
                    case IndexingConfiguration.IndexStartupBehaviorType.Immediate:
                    case IndexingConfiguration.IndexStartupBehaviorType.Delay:
                        expectedStatus = IndexRunningStatus.Running;
                        break;
                    case IndexingConfiguration.IndexStartupBehaviorType.Pause:
                        expectedStatus = IndexRunningStatus.Paused;
                        break;
                    default:
                        throw new ArgumentOutOfRangeException(nameof(type), type, null);
                }

                Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database); // start loading the database

                indexStats = store.Maintenance.Send(new GetIndexStatisticsOperation(autoIndexName));
                Assert.Equal(expectedStatus, indexStats.Status);

                indexStats = store.Maintenance.Send(new GetIndexStatisticsOperation(staticIndexName));
                Assert.Equal(expectedStatus, indexStats.Status);
            }
        }
    }
}
