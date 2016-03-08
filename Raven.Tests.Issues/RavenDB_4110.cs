// -----------------------------------------------------------------------
//  <copyright file="RavenDB_4110.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using System.Threading.Tasks;

using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto;

using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_4110 : RavenTest
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
            using (var store = NewDocumentStore())
            {
                var index = new People_ByName();
                index.Execute(store);

                store.DatabaseCommands.SetIndexLock(index.IndexName, IndexLockMode.LockedError);

                index.Execute(store);
                index.SideBySideExecute(store);

                store.DatabaseCommands.SetIndexLock(index.IndexName, IndexLockMode.Unlock);

                var definition = index.CreateIndexDefinition();
                definition.SortOptions["Name"] = SortOptions.Int;
                store.DatabaseCommands.PutIndex(index.IndexName, definition, true);

                store.DatabaseCommands.SetIndexLock(index.IndexName, IndexLockMode.LockedError);

                Assert.Throws<IndexCompilationException>(() => index.Execute(store));
                Assert.Throws<InvalidOperationException>(() => index.SideBySideExecute(store));
            }
        }

        [Fact]
        public async Task WhenIndexDefinitionDidNotChangeThenWeShouldNotThrowErrorIfIndexIsInLockedErrorStateAsync()
        {
            using (var store = NewDocumentStore())
            {
                var index = new People_ByName();
                await index.ExecuteAsync(store).ConfigureAwait(false);

                store.DatabaseCommands.SetIndexLock(index.IndexName, IndexLockMode.LockedError);

                await index.ExecuteAsync(store).ConfigureAwait(false);
                await index.SideBySideExecuteAsync(store).ConfigureAwait(false);

                store.DatabaseCommands.SetIndexLock(index.IndexName, IndexLockMode.Unlock);

                var definition = index.CreateIndexDefinition();
                definition.SortOptions["Name"] = SortOptions.Int;
                store.DatabaseCommands.PutIndex(index.IndexName, definition, true);

                store.DatabaseCommands.SetIndexLock(index.IndexName, IndexLockMode.LockedError);

                await AssertAsync.Throws<IndexCompilationException>(() => index.ExecuteAsync(store));
                await AssertAsync.Throws<InvalidOperationException>(() => index.SideBySideExecuteAsync(store));
            }
        }
    }
}