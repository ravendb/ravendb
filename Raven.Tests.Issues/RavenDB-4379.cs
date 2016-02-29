using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Smuggler;
using Raven.Client;
using Raven.Smuggler;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_4379 : RavenTestBase
    {
        private class User
        {
            public string Name { get; set; }
        }

        [Fact]
        public async Task Using_smuggler_between_servers_should_take_into_accound_files_related_while_smuggling()
        {
            using (var server1 = GetNewServer(8090))
            using (var server2 = GetNewServer(8091))
            using (var store1 = NewRemoteDocumentStore(ravenDbServer: server1))
            using (var store2 = NewRemoteDocumentStore(ravenDbServer: server2))
            {
                var smugglerApi = new SmugglerDatabaseApi();

                var createdUsers = AddDocuments(store1);
                var users = new ConcurrentBag<User>();
                foreach (var user in createdUsers)
                    users.Add(user);

                var addDocsTask = Task.Run(() =>
                {
                    var createdUsersInParallel = AddDocuments(store1);
                    foreach(var user in createdUsersInParallel)
                        users.Add(user);
                });
                var smugglingTask = smugglerApi.Between(new SmugglerBetweenOptions<RavenConnectionStringOptions>
                {
                    From = new RavenConnectionStringOptions
                    {
                        Url = store1.Url,
                        DefaultDatabase = store1.DefaultDatabase
                    },
                    To = new RavenConnectionStringOptions
                    {
                        Url = store2.Url,
                        DefaultDatabase = store2.DefaultDatabase
                    }
                });
                
                await smugglingTask;
                await addDocsTask; //not necessary -> smuggling task should effectively do this as well

                WaitForIndexing(store2);
                using (var session = store2.OpenSession())
                    Assert.Equal(users.Count, session.Query<User>().Count());
            }
        }

        private IEnumerable<User> AddDocuments(IDocumentStore store, int docCount = 10000)
        {
            using (var bulkInsert = store.BulkInsert())
            {
                for (int i = 0; i < docCount; i++)
                {
                    var user = new User
                    {
                        Name = "John Doe " + Guid.NewGuid()
                    };
                    bulkInsert.Store(user);
                    yield return user;
                }
            }
        }
    }
}
