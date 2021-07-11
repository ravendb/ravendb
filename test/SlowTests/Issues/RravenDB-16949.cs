using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Server.Config;
using SlowTests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RravenDB_16949 : RavenTestBase
    {
        public RravenDB_16949(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task limitRevisionDeletion()
        {
            using (var server = GetNewServer(
                new ServerCreationOptions
                {
                    CustomSettings = new Dictionary<string, string>
                    {
                        [RavenConfiguration.GetKey(x => x.Databases.MaxRevisionsToDeleteUponDocumentUpdate)] = 100.ToString()
                    },
                    RegisterForDisposal = false
                }))
            using (var store = GetDocumentStore(new Options
            {
                Server = server
            }))
            {
                var configuration = new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration
                    {
                        Disabled = false,
                        MinimumRevisionsToKeep = 1000000
                    },

                };
                await RevisionsHelper.SetupRevisions(store, server.ServerStore, configuration);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User {Name = "Toli"}, "users/1");
                    await session.SaveChangesAsync();
                }

                for (int i = 0; i < 500; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User { Name = "Toli" + i }, "users/1");
                        await session.SaveChangesAsync();
                    }
                }
                using (var session = store.OpenAsyncSession())
                {

                    var revisionCount = session.Advanced.Revisions.GetForAsync<User>("users/1", 0, 1000).Result.Count;
                    Assert.Equal(501, revisionCount);
                }

                configuration = new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration
                    {
                        Disabled = false,
                        MinimumRevisionsToKeep = 10
                    },

                };
                await RevisionsHelper.SetupRevisions(store, server.ServerStore, configuration);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Toli" }, "users/1");
                    await session.SaveChangesAsync();

                    var revisionCount = session.Advanced.Revisions.GetForAsync<User>("users/1", 0, 1000).Result.Count;
                    Assert.Equal(402, revisionCount);
                }

                for (int i = 0; i < 4; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User { Name = "Toli" + i }, "users/1");
                        await session.SaveChangesAsync();
                    }
                }

                using (var session = store.OpenAsyncSession())
                {
                    var revisionCount = session.Advanced.Revisions.GetForAsync<User>("users/1", 0, 1000).Result.Count;
                    Assert.Equal(10, revisionCount);
                }
            }
        }
    }
}
