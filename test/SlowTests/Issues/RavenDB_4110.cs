// -----------------------------------------------------------------------
//  <copyright file="RavenDB_4110.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Exceptions.Indexes;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Exceptions;
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

                store.Admin.Send(new SetIndexLockOperation(index.IndexName, IndexLockMode.LockedError));

                index.Execute(store);

                store.Admin.Send(new SetIndexLockOperation(index.IndexName, IndexLockMode.Unlock));

                var definition = index.CreateIndexDefinition();
                definition.LockMode = IndexLockMode.LockedError;
                definition.Fields["Name"] = new IndexFieldOptions
                {
                    Sort = SortOptions.Numeric
                };

                store.Admin.Send(new PutIndexesOperation(definition));

                WaitForIndexing(store);

                Assert.Throws<IndexOrTransformerAlreadyExistException>(() => index.Execute(store));
            }
        }

        [Fact]
        public async Task WhenIndexDefinitionDidNotChangeThenWeShouldNotThrowErrorIfIndexIsInLockedErrorStateAsync()
        {
            using (var store = GetDocumentStore())
            {
                var index = new People_ByName();
                await index.ExecuteAsync(store).ConfigureAwait(false);

                store.Admin.Send(new SetIndexLockOperation(index.IndexName, IndexLockMode.LockedError));

                await index.ExecuteAsync(store).ConfigureAwait(false);

                store.Admin.Send(new SetIndexLockOperation(index.IndexName, IndexLockMode.Unlock));

                var definition = index.CreateIndexDefinition();
                definition.LockMode = IndexLockMode.LockedError;
                definition.Fields["Name"] = new IndexFieldOptions
                {
                    Sort = SortOptions.Numeric
                };

                store.Admin.Send(new PutIndexesOperation(definition));

                WaitForIndexing(store);

                await Assert.ThrowsAsync<IndexOrTransformerAlreadyExistException>(() => index.ExecuteAsync(store));
            }
        }
    }
}