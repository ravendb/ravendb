using System;
using System.IO;
using FastTests;
using Raven.Client.Indexes;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Client.Data;
using Raven.Client.Data.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_5489 : RavenTestBase
    {
        [Fact]
        public async Task IfIndexEncountersCorruptionItShouldBeMarkedAsErrored()
        {
            using (var store = GetDocumentStore())
            {
                new Users_ByName().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "John"
                    });

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                Assert.Equal(IndexState.Normal, store.DatabaseCommands.GetStatistics().Indexes[0].State);

                var database = await GetDatabase(store.DefaultDatabase);
                var index = database.IndexStore.GetIndex(1);
                index._indexStorage._simulateCorruption = true;

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Bob"
                    });

                    session.SaveChanges();
                }

                var result = SpinWait.SpinUntil(() => store.DatabaseCommands.GetStatistics().Indexes[0].State == IndexState.Error, TimeSpan.FromSeconds(5));
                Assert.True(result);

                var e = Assert.Throws<InvalidOperationException>(() => store.DatabaseCommands.Query(new Users_ByName().IndexName, new IndexQuery()));
                Assert.Contains("Simulated corruption", e.InnerException.Message);

                var errors = store.DatabaseCommands.GetIndexErrors(new Users_ByName().IndexName);
                Assert.Equal(1, errors.Errors.Length);
                Assert.Contains("Simulated corruption", errors.Errors[0].Error);
            }
        }

        private class User
        {
            public string Name { get; set; }
        }

        private class Users_ByName : AbstractIndexCreationTask<User>
        {
            public Users_ByName()
            {
                Map = users => from u in users
                               select new
                               {
                                   u.Name
                               };
            }
        }
    }
}