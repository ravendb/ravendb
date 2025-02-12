using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Server.ServerWide;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using static SlowTests.RavenDB_20425;

namespace SlowTests.Issues;
public class RavenDB_21934 : RavenTestBase
{
    public RavenDB_21934(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Revisions)]
    public async Task EnforceRevisionsConfigurationShouldntSkipDocs()
    {
        using var store = GetDocumentStore();

        var collections = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "Companies",
            "Users"
        };

        var configuration = new RevisionsConfiguration
        {
            Default = new RevisionsCollectionConfiguration
            {
                Disabled = false,
            }
        };
        await RevisionsHelper.SetupRevisionsAsync(store, Server.ServerStore, configuration: configuration);

        // 1026 users
        for (int i = 0; i < 1026; i++)
        {
            var user = new User { Id = $"Users/{i}", Name = "user" };
            await StoreRevisionsAsync(store, user);
        }

        // 1028 companies
        for (int i = 0; i < 1028; i++)
        {
            var company = new Company { Id = $"Companies/{i}", Name = "company" };
            await StoreRevisionsAsync(store, company);
        }

        configuration = new RevisionsConfiguration
        {
            Default = new RevisionsCollectionConfiguration
            {
                Disabled = false,
                MinimumRevisionsToKeep = 2
            }
        };
        await RevisionsHelper.SetupRevisionsAsync(store, Server.ServerStore, configuration: configuration);

        var db = await Databases.GetDocumentDatabaseInstanceFor(store);
        using (var token = new OperationCancelToken(db.Configuration.Databases.OperationTimeout.AsTimeSpan, db.DatabaseShutdown, CancellationToken.None))
            await db.DocumentsStorage.RevisionsStorage.EnforceConfigurationAsync(_ => { }, true, collections, token: token);

        using (var session = store.OpenAsyncSession())
        {
            var company1Count = await session.Advanced.Revisions.GetCountForAsync("Companies/1");
            Assert.Equal(2, company1Count);
            var company2Count = await session.Advanced.Revisions.GetCountForAsync("Companies/2");
            Assert.Equal(2, company2Count);
        }

    }

    private static async Task StoreRevisionsAsync(IDocumentStore store, IEntity o)
    {
        for (int i = 0; i < 3; i++)
        {
            using (var session = store.OpenAsyncSession())
            {
                o.Name += i;
                await session.StoreAsync(o);
                await session.SaveChangesAsync();
            }
        }
    }

    private class User : IEntity
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }

    private class Company : IEntity
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }

    private interface IEntity
    {
        string Id { get; set; }
        string Name { get; set; }
    }
}

