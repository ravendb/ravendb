using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Revisions;
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
                await database.DocumentsStorage.RevisionsStorage.RevertDocumentsToRevisionsAsync(new Dictionary<string, string>() { { user1.Id, user1revertCv } }, token);
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

            await store.Operations.SendAsync(new RevertDocumentsToRevisionsOperation(user1.Id, user1revertCv));

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
        public async Task RevertByDocumentEpMultipleDocs(Options options)
        {
            var user1 = new User { Id = "Users/1-A", Name = "Shahar1" };
            var user2 = new User { Id = "Users/2-B", Name = "Shahar2" };
            var company1 = new User { Id = "Companies/1-A", Name = "RavenDB1" };
            var company2 = new User { Id = "Companies/2-B", Name = "RavenDB2" };

            using var store = GetDocumentStore(options);

            var configuration = new RevisionsConfiguration { Default = new RevisionsCollectionConfiguration { Disabled = false } };
            await RevisionsHelper.SetupRevisionsAsync(store, configuration: configuration);

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(user1);
                await session.StoreAsync(user2);
                await session.StoreAsync(company1);
                await session.StoreAsync(company2);
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

                    var c1 = await session.LoadAsync<User>(company1.Id);
                    c1.Name = $"RavenDB1_{i}";

                    var c2 = await session.LoadAsync<User>(company2.Id);
                    c2.Name = $"RavenDB2_{i}";

                    await session.SaveChangesAsync();
                }
            }

            var user1revertCv = string.Empty;
            var company1revertCv = string.Empty;

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

                var c1Metadata = await session
                    .Advanced
                    .Revisions
                    .GetMetadataForAsync(id: company1.Id);

                company1revertCv = c1Metadata[5].GetString(Constants.Documents.Metadata.ChangeVector);
            }

            await store.Operations.SendAsync(
                new RevertDocumentsToRevisionsOperation(new Dictionary<string, string>() { { user1.Id, user1revertCv }, { company1.Id, company1revertCv } }));

            using (var session = store.OpenAsyncSession())
            {
                var u1 = await session.LoadAsync<User>(user1.Id);
                Assert.Equal(u1.Name, "Shahar1_4");

                var u1Count = await session.Advanced.Revisions.GetCountForAsync(user1.Id);
                Assert.Equal(12, u1Count);


                var c1 = await session.LoadAsync<User>(company1.Id);
                Assert.Equal(c1.Name, "RavenDB1_4");

                var c1Count = await session.Advanced.Revisions.GetCountForAsync(company1.Id);
                Assert.Equal(12, c1Count);


                var u2 = await session.LoadAsync<User>(user2.Id);
                Assert.Equal(u2.Name, "Shahar2_9");

                var u2Count = await session.Advanced.Revisions.GetCountForAsync(user2.Id);
                Assert.Equal(11, u2Count);

                var c2 = await session.LoadAsync<User>(company2.Id);
                Assert.Equal(c2.Name, "RavenDB2_9");

                var c2Count = await session.Advanced.Revisions.GetCountForAsync(company2.Id);
                Assert.Equal(11, c2Count);
            }
        }


        [RavenTheory(RavenTestCategory.Revisions)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task RevertByDocumentEpFail(Options options)
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
                    await session.SaveChangesAsync();
                }
            }

            using (var session = store.OpenAsyncSession())
            {
                var count1 = await session.Advanced.Revisions.GetCountForAsync(user1.Id);
                Assert.Equal(11, count1);
            }

            var user2revertCv = string.Empty;
            using (var session = store.OpenAsyncSession())
            {
                var count1 = await session.Advanced.Revisions.GetCountForAsync(user1.Id);
                Assert.Equal(11, count1);
                var count2 = await session.Advanced.Revisions.GetCountForAsync(user2.Id);
                Assert.Equal(11, count2);

                var u2Metadata = await session
                    .Advanced
                    .Revisions
                    .GetMetadataForAsync(id: user2.Id);

                user2revertCv = u2Metadata[5].GetString(Constants.Documents.Metadata.ChangeVector);
            }


            Exception e = await Assert.ThrowsAsync<RavenException>(() => store.Operations.SendAsync(
                new RevertDocumentsToRevisionsOperation(user1.Id, "A:253-jyJQ+3eQ5kuHIGZ+aqSVow")));

            e = e.InnerException;

            Assert.Contains("Revision with the cv \"A:253-jyJQ+3eQ5kuHIGZ+aqSVow\" doesn't exist (id: 'Users/1-A')", e.Message);

            e = await Assert.ThrowsAsync<RavenException>(() => store.Operations.SendAsync(
                new RevertDocumentsToRevisionsOperation(user1.Id, user2revertCv)));

            e = e.InnerException;

            if (options.DatabaseMode == RavenDatabaseMode.Single)
            {
                Assert.Contains(
                    $"Revision with the cv \"{user2revertCv}\" doesn't belong to the doc \"Users/1-A\" but to the doc \"Users/2-B\"",
                    e.Message);
            }
            else
            {
                var u1shard = await Sharding.GetShardNumberForAsync(store, "Users/1-A");
                var u2shard = await Sharding.GetShardNumberForAsync(store, "Users/2-B");
                var expectedMsg = string.Empty;
                if (u1shard == u2shard)
                    expectedMsg = $"Revision with the cv \"{user2revertCv}\" doesn't belong to the doc \"Users/1-A\" but to the doc \"Users/2-B\"";
                else
                {
                    // if users/1-A and Users/2-B are in a separate shards, the shard won't find the cv at all
                    expectedMsg = $"Revision with the cv \"{user2revertCv}\" doesn't exist (id: 'Users/1-A')";
                }

                Assert.Contains(
                    expectedMsg,
                    e.Message); 
            }

            using (var session = store.OpenAsyncSession())
            {
                var count1 = await session.Advanced.Revisions.GetCountForAsync(user1.Id);
                Assert.Equal(11, count1);
            }
        }


        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        private class Company
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }
    }
}
