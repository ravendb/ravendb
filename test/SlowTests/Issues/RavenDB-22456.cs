using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Session;
using Raven.Server.ServerWide;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using static SlowTests.RavenDB_20425;

namespace SlowTests.Issues
{
    public class RavenDB_22456 : ReplicationTestBase
    {
        public RavenDB_22456(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Revisions)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task RevertByDocument(Options options)
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

            // WaitForUserToContinueTheTest(store, false);

            var database = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
            using (var token = new OperationCancelToken(database.Configuration.Databases.OperationTimeout.AsTimeSpan, database.DatabaseShutdown, CancellationToken.None))
            {
                var result = (RevertResult)await database.DocumentsStorage.RevisionsStorage.RevertRevisions(onProgress: null,
                    id: user1.Id, cv: user1revertCv,
                    token: token);
            }

            // WaitForUserToContinueTheTest(store, false);

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
