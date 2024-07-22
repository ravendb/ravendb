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

namespace SlowTests.Issues;

internal class RavenDB_17494 : ClusterTestBase
{
    public RavenDB_17494(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Revisions)]
    public async Task DeleteRevisionsManuallyTest()
    {
        var user1 = new User { Id = "Users/1-A", Name = "Shahar1" };
        var user2 = new User { Id = "Users/2-B", Name = "Shahar2" };
        var company1 = new Company { Id = "Companies/1-A", Name = "Shahar1" };
        var company2 = new Company { Id = "Companies/2-B", Name = "Shahar2" };
        var company3 = new Company { Id = "Companies/3-C", Name = "Shahar3" };

        using var store = GetDocumentStore();

        var configuration = new RevisionsConfiguration
        {
            Default = new RevisionsCollectionConfiguration
            {
                Disabled = false,
                AllowDeleteRevisionsManually = true
            }
        };
        await RevisionsHelper.SetupRevisionsAsync(store, configuration: configuration);

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(user1);
            await session.StoreAsync(user2);
            await session.StoreAsync(company1);
            await session.StoreAsync(company2);
            await session.StoreAsync(company3);
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

                var c1 = await session.LoadAsync<Company>(company1.Id);
                c1.Name = $"RavenDB1_{i}";

                var c2 = await session.LoadAsync<Company>(company2.Id);
                c2.Name = $"RavenDB2_{i}";

                var c3 = await session.LoadAsync<Company>(company3.Id);
                c3.Name = $"RavenDB3_{i}";

                await session.SaveChangesAsync();
            }
        }

        string user1deleteCv = string.Empty;
        string user2deleteCv = string.Empty;
        string company1deleteCv = string.Empty;

        using (var session = store.OpenAsyncSession())
        {
            Assert.Equal(11, await session.Advanced.Revisions.GetCountForAsync(user1.Id));
            Assert.Equal(11, await session.Advanced.Revisions.GetCountForAsync(user2.Id));
            Assert.Equal(11, await session.Advanced.Revisions.GetCountForAsync(company1.Id));
            Assert.Equal(11, await session.Advanced.Revisions.GetCountForAsync(company2.Id));
            Assert.Equal(11, await session.Advanced.Revisions.GetCountForAsync(company3.Id));

            var u1revisionsCvs = await GetRevisionsCvs(session, user1.Id);
            user1deleteCv = u1revisionsCvs[5];

            var u2revisionsCvs = await GetRevisionsCvs(session, user2.Id);
            user2deleteCv = u2revisionsCvs[5];

            var c1revisionsCvs = await GetRevisionsCvs(session, company1.Id);
            company1deleteCv = c1revisionsCvs[5];
        }

        var database = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
        await database.DocumentsStorage.RevisionsStorage.DeleteRevisionsByChangeVectorManuallyAsync(new List<string>(){ user1deleteCv, company1deleteCv, user2deleteCv }, 100);
        await database.DocumentsStorage.RevisionsStorage.DeleteRevisionsByDocumentIdManuallyAsync(new List<string>() { company3.Id }, 100);

        using (var session = store.OpenAsyncSession())
        {
            Assert.Equal(10, await session.Advanced.Revisions.GetCountForAsync(user1.Id));
            Assert.Equal(10, await session.Advanced.Revisions.GetCountForAsync(user2.Id));
            Assert.Equal(10, await session.Advanced.Revisions.GetCountForAsync(company1.Id));
            Assert.Equal(11, await session.Advanced.Revisions.GetCountForAsync(company2.Id));
            Assert.Equal(0, await session.Advanced.Revisions.GetCountForAsync(company3.Id));

            var u1revisionsCvs = await GetRevisionsCvs(session, user1.Id);
            Assert.False(u1revisionsCvs.Any(cv => cv == user1deleteCv));

            var u2revisionsCvs = await GetRevisionsCvs(session, user2.Id);
            Assert.False(u2revisionsCvs.Any(cv => cv == user2deleteCv));

            var c1revisionsCvs = await GetRevisionsCvs(session, company1.Id);
            Assert.False(c1revisionsCvs.Any(cv => cv == company1deleteCv));
        }

        // WaitForUserToContinueTheTest(store, false);
    }

    [RavenFact(RavenTestCategory.Revisions)]
    public async Task DeleteRevisionsManuallyExceptionsTest()
    {
        var user1 = new User { Id = "Users/1-A", Name = "Shahar1" };
        var user2 = new User { Id = "Users/2-B", Name = "Shahar2" };
        var company1 = new Company { Id = "Companies/1-A", Name = "Shahar1" };
        var company2 = new Company { Id = "Companies/2-B", Name = "Shahar2" };
        var company3 = new Company { Id = "Companies/3-C", Name = "Shahar3" };

        using var store = GetDocumentStore();

        var configuration = new RevisionsConfiguration
        {
            Default = new RevisionsCollectionConfiguration
            {
                Disabled = false
            }
        };
        await RevisionsHelper.SetupRevisionsAsync(store, configuration: configuration);

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(user1);
            await session.StoreAsync(user2);
            await session.StoreAsync(company1);
            await session.StoreAsync(company2);
            await session.StoreAsync(company3);
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

                var c1 = await session.LoadAsync<Company>(company1.Id);
                c1.Name = $"RavenDB1_{i}";

                var c2 = await session.LoadAsync<Company>(company2.Id);
                c2.Name = $"RavenDB2_{i}";

                var c3 = await session.LoadAsync<Company>(company3.Id);
                c3.Name = $"RavenDB3_{i}";

                await session.SaveChangesAsync();
            }
        }

        string user1deleteCv = string.Empty;
        string user2deleteCv = string.Empty;
        string company1deleteCv = string.Empty;

        using (var session = store.OpenAsyncSession())
        {
            Assert.Equal(11, await session.Advanced.Revisions.GetCountForAsync(user1.Id));
            Assert.Equal(11, await session.Advanced.Revisions.GetCountForAsync(user2.Id));
            Assert.Equal(11, await session.Advanced.Revisions.GetCountForAsync(company1.Id));
            Assert.Equal(11, await session.Advanced.Revisions.GetCountForAsync(company2.Id));
            Assert.Equal(11, await session.Advanced.Revisions.GetCountForAsync(company3.Id));

            var u1revisionsCvs = await GetRevisionsCvs(session, user1.Id);
            user1deleteCv = u1revisionsCvs[5];

            var u2revisionsCvs = await GetRevisionsCvs(session, user2.Id);
            user2deleteCv = u2revisionsCvs[5];

            var c1revisionsCvs = await GetRevisionsCvs(session, company1.Id);
            company1deleteCv = c1revisionsCvs[5];
        }

        var database = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);

        var e1 = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            database.DocumentsStorage.RevisionsStorage.DeleteRevisionsByChangeVectorManuallyAsync(new List<string>() { user1deleteCv, company1deleteCv, user2deleteCv }, 100));
        Assert.Contains("You are trying to delete revisions of 'users/1-a' but it isn't allowed by its revisions configuration.", e1.Message);

        var e2 = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            database.DocumentsStorage.RevisionsStorage.DeleteRevisionsByDocumentIdManuallyAsync(new List<string>() { company3.Id }, 100));
        Assert.Contains("You are trying to delete revisions of 'Companies/3-C' but it isn't allowed by its revisions configuration.", e2.Message);

        configuration = new RevisionsConfiguration
        {
            Default = new RevisionsCollectionConfiguration
            {
                Disabled = false,
                AllowDeleteRevisionsManually = true
            }
        };
        await RevisionsHelper.SetupRevisionsAsync(store, configuration: configuration);

        var e3 = await Assert.ThrowsAsync<InvalidOperationException>(() =>
                database.DocumentsStorage.RevisionsStorage.DeleteRevisionsByChangeVectorManuallyAsync(new List<string>() { user1deleteCv, company1deleteCv, user2deleteCv }, 2));
        Assert.Contains("You are trying to delete more revisions then the limit: 2", e3.Message);

        var e4 = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            database.DocumentsStorage.RevisionsStorage.DeleteRevisionsByDocumentIdManuallyAsync(new List<string>() { company3.Id }, 9));
        Assert.Contains("You are trying to delete more revisions then the limit: 9 (stopped on 'Companies/3-C)'.", e4.Message);

        using (var session = store.OpenAsyncSession())
        {
            Assert.Equal(11, await session.Advanced.Revisions.GetCountForAsync(user1.Id));
            Assert.Equal(11, await session.Advanced.Revisions.GetCountForAsync(user2.Id));
            Assert.Equal(11, await session.Advanced.Revisions.GetCountForAsync(company1.Id));
            Assert.Equal(11, await session.Advanced.Revisions.GetCountForAsync(company2.Id));
            Assert.Equal(11, await session.Advanced.Revisions.GetCountForAsync(company3.Id));
        }
    }

    private async Task<List<string>> GetRevisionsCvs(IAsyncDocumentSession session, string id)
    {
        var cvs = (await session
            .Advanced
            .Revisions
            .GetMetadataForAsync(id)).Select(m => m.GetString(Constants.Documents.Metadata.ChangeVector));

        return cvs.ToList();
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
