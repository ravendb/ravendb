using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Utils;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Server.ServerWide;
using SlowTests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;
public class RavenDB_21780 : ClusterTestBase
{
    public RavenDB_21780(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Revisions)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
    public async Task EnforceRevisionsConfigurationForSpecificCollctionsWhichOneOfThemDoesNotExistShouldntFail(Options options)
    {
        using var store = GetDocumentStore(options);

        var configuration = new RevisionsConfiguration
        {
            Default = new RevisionsCollectionConfiguration
            {
                Disabled = false,
                MinimumRevisionsToKeep = 100
            }
        };
        await RevisionsHelper.SetupRevisionsAsync(store, Server.ServerStore, configuration: configuration);

        var user = new User { Id = "Users/1", Name = "Shahar" };

        using (var session = store.OpenAsyncSession())
        {
            for (int i = 0; i < 10; i++)
            {
                user.Name += i;
                await session.StoreAsync(user);
                await session.SaveChangesAsync();
            }

            var revisionsCount = await session.Advanced.Revisions.GetCountForAsync(user.Id);
            Assert.Equal(10, revisionsCount);
        }


        configuration.Default.MinimumRevisionsToKeep = 2;
        await RevisionsHelper.SetupRevisionsAsync(store, Server.ServerStore, configuration: configuration);


        // enforce
        var db = await Databases.GetDocumentDatabaseInstanceFor(Server, store, store.Database);
        using (var token = new OperationCancelToken(db.Configuration.Databases.OperationTimeout.AsTimeSpan, db.DatabaseShutdown, CancellationToken.None))
            await db.DocumentsStorage.RevisionsStorage.EnforceConfigurationAsync(_ => { }, includeForceCreated: true, collections: new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "Users" }, token: token);


        using (var session = store.OpenAsyncSession())
        {
            var revisionsCount = await session.Advanced.Revisions.GetCountForAsync(user.Id);
            Assert.Equal(2, revisionsCount);
        }
    }

}
