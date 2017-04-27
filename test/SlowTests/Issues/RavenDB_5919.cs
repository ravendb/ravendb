using System.Linq;
using System.Net.Http;
using FastTests;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_5919 : RavenTestBase
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

        [Fact]
        public void ShouldInheritLockMode()
        {
            using (var documentStore = GetDocumentStore())
            {
                new Entity_ById_V1().Execute(documentStore);

                documentStore.Admin.Send(new StopIndexingOperation());

                new Entity_ById_V2().Execute(documentStore);

                documentStore.Admin.Send(new SetIndexLockOperation("Entity/ById", IndexLockMode.LockedIgnore));

                var index1 = documentStore.Admin.Send(new GetIndexOperation("Entity/ById"));
                var index2 = documentStore.Admin.Send(new GetIndexOperation($"{Constants.Documents.Indexing.SideBySideIndexNamePrefix}Entity/ById"));

                Assert.Equal(IndexLockMode.LockedIgnore, index1.LockMode);
                Assert.Equal(IndexLockMode.LockedIgnore, index2.LockMode);

                documentStore.Admin.Send(new StartIndexingOperation());

                WaitForIndexing(documentStore);

                var stats = documentStore.Admin.Send(new GetStatisticsOperation());

                Assert.Equal(1, stats.CountOfIndexes);
                Assert.Equal(IndexLockMode.LockedIgnore, stats.Indexes[0].LockMode);
            }
        }


        [Fact]
        public void ShouldInheritPriority()
        {
            using (var documentStore = GetDocumentStore())
            {
                new Entity_ById_V1().Execute(documentStore);

                documentStore.Admin.Send(new StopIndexingOperation());

                new Entity_ById_V2().Execute(documentStore);

                documentStore.Admin.Send(new SetIndexPriorityOperation("Entity/ById", IndexPriority.High));

                var index1 = documentStore.Admin.Send(new GetIndexOperation("Entity/ById"));
                var index2 = documentStore.Admin.Send(new GetIndexOperation($"{Constants.Documents.Indexing.SideBySideIndexNamePrefix}Entity/ById"));

                Assert.Equal(IndexPriority.High, index1.Priority);
                Assert.Equal(IndexPriority.High, index2.Priority);

                documentStore.Admin.Send(new StartIndexingOperation());

                WaitForIndexing(documentStore);

                var stats = documentStore.Admin.Send(new GetStatisticsOperation());

                Assert.Equal(1, stats.CountOfIndexes);
                Assert.Equal(IndexPriority.High, stats.Indexes[0].Priority);
            }
        }

        [Fact]
        public void ShouldNotRecreateReplacementIndexIfItIsTheSame()
        {
            using (var documentStore = GetDocumentStore())
            {
                new Entity_ById_V1().Execute(documentStore);

                documentStore.Admin.Send(new StopIndexingOperation());

                new Entity_ById_V2().Execute(documentStore);

                var index1 = documentStore.Admin.Send(new GetIndexOperation($"{Constants.Documents.Indexing.SideBySideIndexNamePrefix}Entity/ById"));

                new Entity_ById_V2().Execute(documentStore);

                var index2 = documentStore.Admin.Send(new GetIndexOperation($"{Constants.Documents.Indexing.SideBySideIndexNamePrefix}Entity/ById"));

                Assert.Equal(index1.Etag, index2.Etag);
            }
        }

        [Fact]
        public void ShouldBeAbleToForceReplacement()
        {
            using (var documentStore = GetDocumentStore())
            {
                new Entity_ById_V1().Execute(documentStore);

                documentStore.Admin.Send(new StopIndexingOperation());

                new Entity_ById_V2().Execute(documentStore);

                var stats = documentStore.Admin.Send(new GetStatisticsOperation());
                Assert.Equal(2, stats.CountOfIndexes);

                using (var commands = documentStore.Commands())
                {
                    commands.ExecuteJson($"indexes/replace?name={new Entity_ById_V2().IndexName}", HttpMethod.Post, null);
                }

                stats = documentStore.Admin.Send(new GetStatisticsOperation());
                Assert.Equal(1, stats.CountOfIndexes);
            }
        }

        [Fact]
        public void ChangingLockModeOrPriorityOnlyShouldNotResetIndex()
        {
            using (var store = GetDocumentStore())
            {
                var definition = new IndexDefinition
                {
                    Name = "Test",
                    Maps = { "from doc in docs select new { doc.Name }" }
                };

                var result1 = store.Admin.Send(new PutIndexesOperation(definition))[0];

                Assert.Equal(definition.Name, result1.Index);

                definition.LockMode = IndexLockMode.LockedError;
                definition.Priority = IndexPriority.High;

                store.Admin.Send(new PutIndexesOperation(definition));
                
                var serverDefinition = store.Admin.Send(new GetIndexOperation(definition.Name));
                Assert.Equal(serverDefinition.Priority, definition.Priority);
                Assert.Equal(serverDefinition.LockMode, definition.LockMode);
            }
        }
    }
}