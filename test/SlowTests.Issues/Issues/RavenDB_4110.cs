using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_4110 : RavenTestBase
    {
        public RavenDB_4110(ITestOutputHelper output) : base(output)
        {
        }

        private class People_ByName : AbstractIndexCreationTask<Person>
        {
            public People_ByName()
            {
                Map = persons => from p in persons
                                 select new
                                 {
                                     p.Name
                                 };
            }
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public void WhenIndexDefinitionDidNotChangeThenWeShouldNotThrowErrorIfIndexIsInLockedErrorState()
        {
            using (var store = GetDocumentStore())
            {
                var index = new People_ByName();
                index.Execute(store);

                store.Maintenance.Send(new SetIndexesLockOperation(index.IndexName, IndexLockMode.LockedError));

                index.Execute(store);

                store.Maintenance.Send(new SetIndexesLockOperation(index.IndexName, IndexLockMode.Unlock));

                var definition = index.CreateIndexDefinition();
                definition.LockMode = IndexLockMode.LockedError;
                definition.Fields["Name"] = new IndexFieldOptions
                {
                    Storage = FieldStorage.Yes
                };

                store.Maintenance.Send(new PutIndexesOperation(definition));

                Indexes.WaitForIndexing(store);

                var e = Assert.Throws<IndexCreationException>(() => index.Execute(store));
                Assert.Contains("IndexAlreadyExistException", e.Message);
            }
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public async Task WhenIndexDefinitionDidNotChangeThenWeShouldNotThrowErrorIfIndexIsInLockedErrorStateAsync()
        {
            using (var store = GetDocumentStore())
            {
                var index = new People_ByName();
                await index.ExecuteAsync(store);

                await store.Maintenance.SendAsync(new SetIndexesLockOperation(index.IndexName, IndexLockMode.LockedError));

                await index.ExecuteAsync(store);

                await store.Maintenance.SendAsync(new SetIndexesLockOperation(index.IndexName, IndexLockMode.Unlock));

                var definition = index.CreateIndexDefinition();
                definition.LockMode = IndexLockMode.LockedError;
                definition.Fields["Name"] = new IndexFieldOptions
                {
                    Storage = FieldStorage.Yes
                };
                store.Maintenance.Send(new PutIndexesOperation(definition));

                await Indexes.WaitForIndexingAsync(store);

                var c = await Assert.ThrowsAsync<IndexCreationException>(() => index.ExecuteAsync(store));
                Assert.Contains("IndexAlreadyExistException", c.Message);
            }
        }
    }
}