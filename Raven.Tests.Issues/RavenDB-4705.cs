using System.ComponentModel.Composition.Hosting;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_4705 : RavenTestBase
    {
        public class Entity
        {
            public int Id { get; set; }
        }

        public class Entity_ById_V1 : AbstractIndexCreationTask<Entity>
        {
            public override string IndexName
            {
                get { return "Entity/ById"; }
            }

            public Entity_ById_V1()
            {
                Map = entities => from entity in entities select new { entity.Id, Version = 1 };
            }
        }

        public class Entity_ById_V2 : AbstractIndexCreationTask<Entity>
        {
            public override string IndexName
            {
                get { return "Entity/ById"; }
            }

            public Entity_ById_V2()
            {
                Map = entities => from entity in entities select new { entity.Id, Version = 2 };
            }
        }

        [Fact]
        public void should_not_reset_lock_mode_on_side_by_side_index_creation()
        {
            using (var documentStore = NewDocumentStore())
            {
                new Entity_ById_V1().SideBySideExecute(documentStore);
                WaitForIndexing(documentStore);

                documentStore.DatabaseCommands.SetIndexLock("Entity/ById", IndexLockMode.SideBySide);

                var databaseStatisticsBefore = documentStore.DatabaseCommands.GetStatistics();
                var indexStatsBefore = databaseStatisticsBefore.Indexes.Single(i => i.Name == "Entity/ById");

                Assert.Equal(2, databaseStatisticsBefore.Indexes.Length);
                Assert.Equal(IndexLockMode.SideBySide, indexStatsBefore.LockMode);

                while (true)
                {
                    var index = documentStore.DatabaseCommands.GetStatistics().Indexes.FirstOrDefault(x => x.Name == "Entity/ById");
                    if (index != null)
                        break;
                    Thread.Sleep(100);
                }

                new Entity_ById_V2().SideBySideExecute(documentStore);
                WaitForIndexing(documentStore);

                while (documentStore.DatabaseCommands.GetStatistics().Indexes.Length != 2)
                    Thread.Sleep(100);

                var databaseStatisticsAfter = documentStore.DatabaseCommands.GetStatistics();
                var indexStatsAfter = databaseStatisticsAfter.Indexes.Single(i => i.Name == "Entity/ById");

                Assert.Equal(2, databaseStatisticsAfter.Indexes.Length);
                Assert.Equal(IndexLockMode.SideBySide, indexStatsAfter.LockMode);
            }
        }

        [Fact]
        public async Task should_not_reset_lock_mode_on_async_side_by_side_index_creation()
        {
            using (var documentStore = NewDocumentStore())
            {
                await new Entity_ById_V1().SideBySideExecuteAsync(documentStore).ConfigureAwait(false);
                WaitForIndexing(documentStore);

                documentStore.DatabaseCommands.SetIndexLock("Entity/ById", IndexLockMode.SideBySide);

                var databaseStatisticsBefore = documentStore.DatabaseCommands.GetStatistics();
                var indexStatsBefore = databaseStatisticsBefore.Indexes.Single(i => i.Name == "Entity/ById");

                Assert.Equal(2, databaseStatisticsBefore.Indexes.Length);
                Assert.Equal(IndexLockMode.SideBySide, indexStatsBefore.LockMode);

                while (true)
                {
                    var index = documentStore.DatabaseCommands.GetStatistics().Indexes.FirstOrDefault(x => x.Name == "Entity/ById");
                    if (index != null)
                        break;
                    Thread.Sleep(100);
                }

                await new Entity_ById_V2().SideBySideExecuteAsync(documentStore).ConfigureAwait(false);
                WaitForIndexing(documentStore);

                while (documentStore.DatabaseCommands.GetStatistics().Indexes.Length != 2)
                    Thread.Sleep(100);

                var databaseStatisticsAfter = documentStore.DatabaseCommands.GetStatistics();
                var indexStatsAfter = databaseStatisticsAfter.Indexes.Single(i => i.Name == "Entity/ById");

                Assert.Equal(2, databaseStatisticsAfter.Indexes.Length);
                Assert.Equal(IndexLockMode.SideBySide, indexStatsAfter.LockMode);
            }
        }


        [Fact]
        public void should_not_reset_lock_mode_on_multiple_side_by_side_index_creation()
        {
            using (var documentStore = NewDocumentStore())
            {
                var container = new CompositionContainer(new TypeCatalog(typeof(Entity_ById_V1)));
                IndexCreation.SideBySideCreateIndexes(container, documentStore.DatabaseCommands, documentStore.Conventions);
                WaitForIndexing(documentStore);

                documentStore.DatabaseCommands.SetIndexLock("Entity/ById", IndexLockMode.SideBySide);

                var databaseStatisticsBefore = documentStore.DatabaseCommands.GetStatistics();
                var indexStatsBefore = databaseStatisticsBefore.Indexes.Single(i => i.Name == "Entity/ById");

                Assert.Equal(2, databaseStatisticsBefore.Indexes.Length);
                Assert.Equal(IndexLockMode.SideBySide, indexStatsBefore.LockMode);

                while (true)
                {
                    var index = documentStore.DatabaseCommands.GetStatistics().Indexes.FirstOrDefault(x => x.Name == "Entity/ById");
                    if (index != null)
                        break;
                    Thread.Sleep(100);
                }

                container = new CompositionContainer(new TypeCatalog(typeof(Entity_ById_V2)));
                IndexCreation.SideBySideCreateIndexes(container, documentStore.DatabaseCommands, documentStore.Conventions);
                WaitForIndexing(documentStore);

                while (documentStore.DatabaseCommands.GetStatistics().Indexes.Length != 2)
                    Thread.Sleep(100);

                var databaseStatisticsAfter = documentStore.DatabaseCommands.GetStatistics();
                var indexStatsAfter = databaseStatisticsAfter.Indexes.Single(i => i.Name == "Entity/ById");

                Assert.Equal(2, databaseStatisticsAfter.Indexes.Length);
                Assert.Equal(IndexLockMode.SideBySide, indexStatsAfter.LockMode);
            }
        }

        [Fact]
        public async Task should_not_reset_lock_mode_on_multiple_async_side_by_side_index_creation()
        {
            using (var documentStore = NewDocumentStore())
            {
                var container = new CompositionContainer(new TypeCatalog(typeof(Entity_ById_V1)));
                await IndexCreation
                    .SideBySideCreateIndexesAsync(container, documentStore.AsyncDatabaseCommands, documentStore.Conventions)
                    .ConfigureAwait(false);
                WaitForIndexing(documentStore);

                documentStore.DatabaseCommands.SetIndexLock("Entity/ById", IndexLockMode.SideBySide);

                var databaseStatisticsBefore = documentStore.DatabaseCommands.GetStatistics();
                var indexStatsBefore = databaseStatisticsBefore.Indexes.Single(i => i.Name == "Entity/ById");

                Assert.Equal(2, databaseStatisticsBefore.Indexes.Length);
                Assert.Equal(IndexLockMode.SideBySide, indexStatsBefore.LockMode);

                while (true)
                {
                    var index = documentStore.DatabaseCommands.GetStatistics().Indexes.FirstOrDefault(x => x.Name == "Entity/ById");
                    if (index != null)
                        break;
                    Thread.Sleep(100);
                }

                container = new CompositionContainer(new TypeCatalog(typeof(Entity_ById_V2)));
                await IndexCreation
                    .SideBySideCreateIndexesAsync(container, documentStore.AsyncDatabaseCommands, documentStore.Conventions)
                    .ConfigureAwait(false);
                WaitForIndexing(documentStore);

                while (documentStore.DatabaseCommands.GetStatistics().Indexes.Length != 2)
                    Thread.Sleep(100);

                var databaseStatisticsAfter = documentStore.DatabaseCommands.GetStatistics();
                var indexStatsAfter = databaseStatisticsAfter.Indexes.Single(i => i.Name == "Entity/ById");

                Assert.Equal(2, databaseStatisticsAfter.Indexes.Length);
                Assert.Equal(IndexLockMode.SideBySide, indexStatsAfter.LockMode);
            }
        }
    }
}