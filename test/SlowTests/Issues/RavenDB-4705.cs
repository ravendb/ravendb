using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Indexes;
using Raven.Client.Indexing;
using Raven.Client.Operations.Databases;
using Raven.Client.Operations.Databases.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_4705 : RavenNewTestBase
    {
        private class Entity
        {
            public int Id { get; set; }
        }

        private class Entity_ById_V1 : AbstractIndexCreationTask<Entity>
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

        private class Entity_ById_V2 : AbstractIndexCreationTask<Entity>
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

        [Fact(Skip = "RavenDB-5919")]
        public void should_not_reset_lock_mode_on_side_by_side_index_creation()
        {
            using (var documentStore = GetDocumentStore())
            {
                new Entity_ById_V1().SideBySideExecute(documentStore);
                WaitForIndexing(documentStore);

                documentStore.Admin.Send(new SetIndexLockOperation("Entity/ById", IndexLockMode.SideBySide));

                var databaseStatisticsBefore = documentStore.Admin.Send(new GetStatisticsOperation());
                var indexStatsBefore = databaseStatisticsBefore.Indexes.Single(i => i.Name == "Entity/ById");

                Assert.Equal(2, databaseStatisticsBefore.Indexes.Length);
                Assert.Equal(IndexLockMode.SideBySide, indexStatsBefore.LockMode);

                while (true)
                {
                    var index = documentStore.Admin.Send(new GetStatisticsOperation()).Indexes.FirstOrDefault(x => x.Name == "Entity/ById");
                    if (index != null)
                        break;
                    Thread.Sleep(100);
                }

                new Entity_ById_V2().SideBySideExecute(documentStore);
                WaitForIndexing(documentStore);

                while (documentStore.Admin.Send(new GetStatisticsOperation()).Indexes.Length != 2)
                    Thread.Sleep(100);

                var databaseStatisticsAfter = documentStore.Admin.Send(new GetStatisticsOperation());
                var indexStatsAfter = databaseStatisticsAfter.Indexes.Single(i => i.Name == "Entity/ById");

                Assert.Equal(2, databaseStatisticsAfter.Indexes.Length);
                Assert.Equal(IndexLockMode.SideBySide, indexStatsAfter.LockMode);
            }
        }

        [Fact(Skip = "RavenDB-5919")]
        public async Task should_not_reset_lock_mode_on_async_side_by_side_index_creation()
        {
            using (var documentStore = GetDocumentStore())
            {
                await new Entity_ById_V1().SideBySideExecuteAsync(documentStore).ConfigureAwait(false);
                WaitForIndexing(documentStore);

                documentStore.Admin.Send(new SetIndexLockOperation("Entity/ById", IndexLockMode.SideBySide));

                var databaseStatisticsBefore = documentStore.Admin.Send(new GetStatisticsOperation());
                var indexStatsBefore = databaseStatisticsBefore.Indexes.Single(i => i.Name == "Entity/ById");

                Assert.Equal(2, databaseStatisticsBefore.Indexes.Length);
                Assert.Equal(IndexLockMode.SideBySide, indexStatsBefore.LockMode);

                while (true)
                {
                    var index = documentStore.Admin.Send(new GetStatisticsOperation()).Indexes.FirstOrDefault(x => x.Name == "Entity/ById");
                    if (index != null)
                        break;
                    Thread.Sleep(100);
                }

                await new Entity_ById_V2().SideBySideExecuteAsync(documentStore).ConfigureAwait(false);
                WaitForIndexing(documentStore);

                while (documentStore.Admin.Send(new GetStatisticsOperation()).Indexes.Length != 2)
                    Thread.Sleep(100);

                var databaseStatisticsAfter = documentStore.Admin.Send(new GetStatisticsOperation());
                var indexStatsAfter = databaseStatisticsAfter.Indexes.Single(i => i.Name == "Entity/ById");

                Assert.Equal(2, databaseStatisticsAfter.Indexes.Length);
                Assert.Equal(IndexLockMode.SideBySide, indexStatsAfter.LockMode);
            }
        }


        [Fact(Skip = "RavenDB-5919")]
        public void should_not_reset_lock_mode_on_multiple_side_by_side_index_creation()
        {
            using (var documentStore = GetDocumentStore())
            {
                //var container = new CompositionContainer(new TypeCatalog(typeof(Entity_ById_V1)));
                //IndexCreation.SideBySideCreateIndexes(container, documentStore.DatabaseCommands, documentStore.Conventions);
                WaitForIndexing(documentStore);

                documentStore.Admin.Send(new SetIndexLockOperation("Entity/ById", IndexLockMode.SideBySide));

                var databaseStatisticsBefore = documentStore.Admin.Send(new GetStatisticsOperation());
                var indexStatsBefore = databaseStatisticsBefore.Indexes.Single(i => i.Name == "Entity/ById");

                Assert.Equal(2, databaseStatisticsBefore.Indexes.Length);
                Assert.Equal(IndexLockMode.SideBySide, indexStatsBefore.LockMode);

                while (true)
                {
                    var index = documentStore.Admin.Send(new GetStatisticsOperation()).Indexes.FirstOrDefault(x => x.Name == "Entity/ById");
                    if (index != null)
                        break;
                    Thread.Sleep(100);
                }

                //container = new CompositionContainer(new TypeCatalog(typeof(Entity_ById_V2)));
                //IndexCreation.SideBySideCreateIndexes(container, documentStore.DatabaseCommands, documentStore.Conventions);
                WaitForIndexing(documentStore);

                while (documentStore.Admin.Send(new GetStatisticsOperation()).Indexes.Length != 2)
                    Thread.Sleep(100);

                var databaseStatisticsAfter = documentStore.Admin.Send(new GetStatisticsOperation());
                var indexStatsAfter = databaseStatisticsAfter.Indexes.Single(i => i.Name == "Entity/ById");

                Assert.Equal(2, databaseStatisticsAfter.Indexes.Length);
                Assert.Equal(IndexLockMode.SideBySide, indexStatsAfter.LockMode);
            }
        }

        [Fact(Skip = "RavenDB-5919")]
        public Task should_not_reset_lock_mode_on_multiple_async_side_by_side_index_creation()
        {
            using (var documentStore = GetDocumentStore())
            {
                //var container = new CompositionContainer(new TypeCatalog(typeof(Entity_ById_V1)));
                //await IndexCreation
                //    .SideBySideCreateIndexesAsync(container, documentStore.AsyncDatabaseCommands, documentStore.Conventions)
                //    .ConfigureAwait(false);
                WaitForIndexing(documentStore);

                documentStore.Admin.Send(new SetIndexLockOperation("Entity/ById", IndexLockMode.SideBySide));

                var databaseStatisticsBefore = documentStore.Admin.Send(new GetStatisticsOperation());
                var indexStatsBefore = databaseStatisticsBefore.Indexes.Single(i => i.Name == "Entity/ById");

                Assert.Equal(2, databaseStatisticsBefore.Indexes.Length);
                Assert.Equal(IndexLockMode.SideBySide, indexStatsBefore.LockMode);

                while (true)
                {
                    var index = documentStore.Admin.Send(new GetStatisticsOperation()).Indexes.FirstOrDefault(x => x.Name == "Entity/ById");
                    if (index != null)
                        break;
                    Thread.Sleep(100);
                }

                //container = new CompositionContainer(new TypeCatalog(typeof(Entity_ById_V2)));
                //await IndexCreation
                //    .SideBySideCreateIndexesAsync(container, documentStore.AsyncDatabaseCommands, documentStore.Conventions)
                //    .ConfigureAwait(false);
                WaitForIndexing(documentStore);

                while (documentStore.Admin.Send(new GetStatisticsOperation()).Indexes.Length != 2)
                    Thread.Sleep(100);

                var databaseStatisticsAfter = documentStore.Admin.Send(new GetStatisticsOperation());
                var indexStatsAfter = databaseStatisticsAfter.Indexes.Single(i => i.Name == "Entity/ById");

                Assert.Equal(2, databaseStatisticsAfter.Indexes.Length);
                Assert.Equal(IndexLockMode.SideBySide, indexStatsAfter.LockMode);
            }

            return Task.CompletedTask;
        }
    }
}
