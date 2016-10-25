using System;
using System.IO;
using FastTests;
using Raven.Client.Indexes;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
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

                Assert.Equal(IndexingPriority.Normal, store.DatabaseCommands.GetStatistics().Indexes[0].Priority);

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

                var result = SpinWait.SpinUntil(() => store.DatabaseCommands.GetStatistics().Indexes[0].Priority == IndexingPriority.Error, TimeSpan.FromSeconds(5));
                Assert.True(result);
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