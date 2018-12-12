// -----------------------------------------------------------------------
//  <copyright file="RavenDB_4110.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_4110 : RavenTestBase
    {
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

        [Fact]
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

                WaitForIndexing(store);

                var e = Assert.Throws<IndexCreationException>(() => index.Execute(store));
                Assert.Contains("IndexAlreadyExistException", e.Message);
            }
        }

        [Fact]
        public async Task WhenIndexDefinitionDidNotChangeThenWeShouldNotThrowErrorIfIndexIsInLockedErrorStateAsync()
        {
            using (var store = GetDocumentStore())
            {
                var index = new People_ByName();
                await index.ExecuteAsync(store).ConfigureAwait(false);

                store.Maintenance.Send(new SetIndexesLockOperation(index.IndexName, IndexLockMode.LockedError));

                await index.ExecuteAsync(store).ConfigureAwait(false);

                store.Maintenance.Send(new SetIndexesLockOperation(index.IndexName, IndexLockMode.Unlock));

                var definition = index.CreateIndexDefinition();
                definition.LockMode = IndexLockMode.LockedError;
                definition.Fields["Name"] = new IndexFieldOptions
                {
                    Storage = FieldStorage.Yes
                };
                store.Maintenance.Send(new PutIndexesOperation(definition));

                WaitForIndexing(store);

                var c = await Assert.ThrowsAsync<IndexCreationException>(() => index.ExecuteAsync(store));
                Assert.Contains("IndexAlreadyExistException", c.Message);
            }
        }
    }
}
