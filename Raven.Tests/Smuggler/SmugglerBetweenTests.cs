// -----------------------------------------------------------------------
//  <copyright file="SmugglerBetweenTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using System.Threading.Tasks;
using Lucene.Net.Documents;
using Raven.Abstractions.Data;
using Raven.Abstractions.Smuggler;
using Raven.Client;
using Raven.Client.Extensions;
using Raven.Client.Indexes;
using Raven.Smuggler;
using Xunit;

namespace Raven.Tests.Smuggler
{
    public class SmugglerBetweenTests : RavenTest
    {
        [Fact]
        public async Task ShouldWork()
        {
            using (var server1 = GetNewServer(port: 8079))
            using (var store1 = NewRemoteDocumentStore(ravenDbServer: server1))
            {
                store1.DatabaseCommands.GlobalAdmin.EnsureDatabaseExists("Database1");
                await new UsersIndex().ExecuteAsync(store1);
                using (var session = store1.OpenAsyncSession("Database1"))
                {
                    await session.StoreAsync(new User {Name = "Oren Eini"});
                    await session.StoreAsync(new User {Name = "Fitzchak Yitzchaki"});
                    await session.SaveChangesAsync();
                }

                using (var server2 = GetNewServer(port: 8078))
                {
                    await SmugglerOp.Between(new SmugglerBetweenOptions
                    {
                        From = new RavenConnectionStringOptions
                        {
                            Url = server1.Server.Configuration.ServerUrl,
                            DefaultDatabase = "Database1",
                        },
                        To = new RavenConnectionStringOptions
                        {
                            Url = server1.Server.Configuration.ServerUrl,
                            DefaultDatabase = "Database2",
                        },
                    });

                    using (var store2 = NewRemoteDocumentStore(ravenDbServer: server2))
                    {
                        await AssertDatabaseHasIndex<UsersIndex>(store2);

                        using (var session2 = store2.OpenAsyncSession("Database2"))
                        {
                            Assert.Equal(2, await session2.Query<User>().CountAsync());
                        }
                    }
                }
            }
        }

        private async Task AssertDatabaseHasIndex<TIndex>(IDocumentStore store) where TIndex : AbstractIndexCreationTask, new()
        {
            var indexes = await store.AsyncDatabaseCommands.GetIndexesAsync(0, 25);
            var indexName = new TIndex().IndexName;
            Assert.True(indexes.Any(definition => definition.Name == indexName), "Index " + indexName + " is missing");
        }

        public class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        public class UsersIndex : AbstractIndexCreationTask<User>
        {
            public UsersIndex()
            {
                Map = users => from user in users
                               select new {user.Name};
            }
        }
    }
}