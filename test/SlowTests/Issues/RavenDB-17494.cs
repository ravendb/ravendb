using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Raven.Server.ServerWide;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_17494 : ClusterTestBase
{
    public RavenDB_17494(ITestOutputHelper output) : base(output)
    {
    }

    private async Task<List<string>> GetRevisionsCvs(IAsyncDocumentSession session, string id)
    {
        var cvs = (await session
            .Advanced
            .Revisions
            .GetMetadataForAsync(id)).Select(m => m.GetString(Constants.Documents.Metadata.ChangeVector));

        return cvs.ToList();
    }

    [RavenTheory(RavenTestCategory.Revisions)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public async Task DeleteRevisionsManuallyEPTest(Options options)
    {
        var user1 = new User { Id = "Users/1-A", Name = "Shahar1" };
        var user2 = new User { Id = "Users/2-B", Name = "Shahar2" };
        var company1 = new Company { Id = "Companies/1-A", Name = "Shahar1" };
        var company2 = new Company { Id = "Companies/2-B", Name = "Shahar2" };
        var company3 = new Company { Id = "Companies/3-C", Name = "Shahar3" };

        using var store = GetDocumentStore(options);

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

        DateTime from = default; // after 5th revision
        DateTime to = default; // before 9th revision

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

                if (i == 9)
                {
                    to = DateTime.UtcNow;
                }

                await session.SaveChangesAsync();

                if (i == 5)
                {
                    from = DateTime.UtcNow;
                }
            }
        }

        List<string> user1revisionsToDelete = null;

        using (var session = store.OpenAsyncSession())
        {
            Assert.Equal(11, await session.Advanced.Revisions.GetCountForAsync(user1.Id));
            Assert.Equal(11, await session.Advanced.Revisions.GetCountForAsync(user2.Id));
            Assert.Equal(11, await session.Advanced.Revisions.GetCountForAsync(company1.Id));
            Assert.Equal(11, await session.Advanced.Revisions.GetCountForAsync(company2.Id));
            Assert.Equal(11, await session.Advanced.Revisions.GetCountForAsync(company3.Id));

            var u1revisionsCvs = await GetRevisionsCvs(session, user1.Id);
            user1revisionsToDelete = new List<string>() { u1revisionsCvs[5], u1revisionsCvs[6], u1revisionsCvs[7] };
        }

        var result = await store.Maintenance.SendAsync(
            new DeleteRevisionsOperation(user1.Id, user1revisionsToDelete ));
        Assert.Equal(3, result.TotalDeletes);

        var result2 = await store.Maintenance.SendAsync(new DeleteRevisionsOperation(documentId: company1.Id));
        Assert.Equal(11, result2.TotalDeletes);

        var result3 = await store.Maintenance.SendAsync(new DeleteRevisionsOperation(documentId: company2.Id, from, to));
        Assert.Equal(3, result3.TotalDeletes);

        using (var session = store.OpenAsyncSession())
        {
            Assert.Equal(11 - 3, await session.Advanced.Revisions.GetCountForAsync(user1.Id));
            Assert.Equal(11, await session.Advanced.Revisions.GetCountForAsync(user2.Id));

            Assert.Equal(0, await session.Advanced.Revisions.GetCountForAsync(company1.Id));
            Assert.Equal(11 - 3, await session.Advanced.Revisions.GetCountForAsync(company2.Id)); // missing revisions 6, 7, 8
            Assert.Equal(11, await session.Advanced.Revisions.GetCountForAsync(company3.Id));

            var u1revisionsCvs = await GetRevisionsCvs(session, user1.Id);

            var notDeletedRevisions = u1revisionsCvs.Intersect(user1revisionsToDelete).ToList();
            Assert.True(notDeletedRevisions.Count == 0, string.Join(',', notDeletedRevisions));
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
