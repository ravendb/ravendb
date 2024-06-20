using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Server.ServerWide;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_22456 : ReplicationTestBase
    {
        public RavenDB_22456(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Revisions)]
        public async Task RevertByDocument()
        {
            var user1 = new User { Id = "Users/1-A", Name = "Shahar1" };
            var user2 = new User { Id = "Users/2-B", Name = "Shahar2" };

            using var store = GetDocumentStore();

            var configuration = new RevisionsConfiguration { Default = new RevisionsCollectionConfiguration { Disabled = false } };
            await RevisionsHelper.SetupRevisionsAsync(store, configuration: configuration);

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(user1);
                await session.StoreAsync(user2);
                await session.SaveChangesAsync();
            }

            for (int i = 0; i < 10; i++)
            {
                using (var session = store.OpenAsyncSession())
                {
                    var u1 = await session.LoadAsync<User>(user1.Id);
                    u1.Name = $"Shahar1_{i}";

                    var u2 = await session.LoadAsync<User>(user2.Id);
                    u2.Name = $"Shahar2_{i}";

                    await session.SaveChangesAsync();
                }
            }

            string user1revertCv = string.Empty;

            using (var session = store.OpenAsyncSession())
            {
                var count1 = await session.Advanced.Revisions.GetCountForAsync(user1.Id);
                Assert.Equal(11, count1);
                var count2 = await session.Advanced.Revisions.GetCountForAsync(user2.Id);
                Assert.Equal(11, count2);

                var u1Metadata = await session
                    .Advanced
                    .Revisions
                    .GetMetadataForAsync(id: user1.Id);

                user1revertCv = u1Metadata[5].GetString(Constants.Documents.Metadata.ChangeVector);
            }

            var database = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
            using (var token = new OperationCancelToken(database.Configuration.Databases.OperationTimeout.AsTimeSpan, database.DatabaseShutdown, CancellationToken.None))
            {
                await database.DocumentsStorage.RevisionsStorage.RevertDocumentsToRevisions(new Dictionary<string, string>() { { user1.Id, user1revertCv } },
                    token: token);
            }

            using (var session = store.OpenAsyncSession())
            {
                var u1 = await session.LoadAsync<User>(user1.Id);
                Assert.Equal(u1.Name, "Shahar1_4");

                var count1 = await session.Advanced.Revisions.GetCountForAsync(user1.Id);
                Assert.Equal(12, count1);
                var count2 = await session.Advanced.Revisions.GetCountForAsync(user2.Id);
                Assert.Equal(11, count2);
            }
        }

        
        [RavenTheory(RavenTestCategory.Revisions)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task RevertByDocumentEP(Options options)
        {
            var user1 = new User { Id = "Users/1-A", Name = "Shahar1" };
            var user2 = new User { Id = "Users/2-B", Name = "Shahar2" };

            using var store = GetDocumentStore(options);

            var configuration = new RevisionsConfiguration { Default = new RevisionsCollectionConfiguration { Disabled = false } };
            await RevisionsHelper.SetupRevisionsAsync(store, configuration: configuration);

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(user1);
                await session.StoreAsync(user2);
                await session.SaveChangesAsync();
            }

            for (int i = 0; i < 10; i++)
            {
                using (var session = store.OpenAsyncSession())
                {
                    var u1 = await session.LoadAsync<User>(user1.Id);
                    u1.Name = $"Shahar1_{i}";

                    var u2 = await session.LoadAsync<User>(user2.Id);
                    u2.Name = $"Shahar2_{i}";

                    await session.SaveChangesAsync();
                }
            }

            string user1revertCv = string.Empty;

            using (var session = store.OpenAsyncSession())
            {
                var count1 = await session.Advanced.Revisions.GetCountForAsync(user1.Id);
                Assert.Equal(11, count1);
                var count2 = await session.Advanced.Revisions.GetCountForAsync(user2.Id);
                Assert.Equal(11, count2);

                var u1Metadata = await session
                    .Advanced
                    .Revisions
                    .GetMetadataForAsync(id: user1.Id);

                user1revertCv = u1Metadata[5].GetString(Constants.Documents.Metadata.ChangeVector);
            }

            await store.Maintenance.SendAsync(new RevertDocumentsToRevisionsOperation(new Dictionary<string, string>() { { user1.Id, user1revertCv } }));

            using (var session = store.OpenAsyncSession())
            {
                var u1 = await session.LoadAsync<User>(user1.Id);
                Assert.Equal(u1.Name, "Shahar1_4");

                var count1 = await session.Advanced.Revisions.GetCountForAsync(user1.Id);
                Assert.Equal(12, count1);
                var count2 = await session.Advanced.Revisions.GetCountForAsync(user2.Id);
                Assert.Equal(11, count2);
            }
        }
        

        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }
    }
}
