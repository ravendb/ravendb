using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_26938 : ClusterTestBase
{
    public RavenDB_26938(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Indexes)]
    public void SideBySideReplacementFromResetShouldSurviveDatabaseRecordChange()
    {
        using (var store = GetDocumentStore())
        {
            new DocIndex().Execute(store); // simple map index

            using (var session = store.OpenSession())
            {
                session.Store(new Doc { Id = "doc-1", StrVal = "value" });
                session.SaveChanges();
            }

            Indexes.WaitForIndexing(store);

            // stop indexing so the side-by-side replacement cannot complete and swap
            store.Maintenance.Send(new StopIndexingOperation());

            store.Maintenance.Send(new ResetIndexOperation(nameof(DocIndex), IndexResetMode.SideBySide));

            var replacementName = Constants.Documents.Indexing.SideBySideIndexNamePrefix + nameof(DocIndex);

            var names = store.Maintenance.Send(new GetIndexNamesOperation(0, 100));
            Assert.Contains(replacementName, names); // replacement exists

            // any database record change; here: deploying an unrelated index
            new OtherIndex().Execute(store);

            names = store.Maintenance.Send(new GetIndexNamesOperation(0, 100));
            Assert.Contains(replacementName, names); // the replacement should still be there
        }
    }

    [RavenFact(RavenTestCategory.Indexes)]
    public async Task SideBySideReplacementFromResetShouldSurviveServerRestart()
    {
        using var server = GetNewServer(new ServerCreationOptions { RunInMemory = false });
        using var store = GetDocumentStore(new Options { RunInMemory = false, Server = server });

        new DocIndex().Execute(store); // simple map index

        using (var session = store.OpenSession())
        {
            session.Store(new Doc { Id = "doc-1", StrVal = "value" });
            session.SaveChanges();
        }

        Indexes.WaitForIndexing(store);

        // stop indexing so the side-by-side replacement cannot complete and swap while we set it up
        store.Maintenance.Send(new StopIndexingOperation());

        store.Maintenance.Send(new ResetIndexOperation(nameof(DocIndex), IndexResetMode.SideBySide));

        var replacementName = Constants.Documents.Indexing.SideBySideIndexNamePrefix + nameof(DocIndex);

        var names = store.Maintenance.Send(new GetIndexNamesOperation(0, 100));
        Assert.Contains(replacementName, names); // replacement exists

        // disable the replacement so it stays paused across the restart (StopIndexing is in-memory only and is lost
        // on restart, which would let the replacement run and immediately swap into the original)
        store.Maintenance.Send(new DisableIndexOperation(replacementName));

        // restart the server, keeping the data on disk
        var result = await DisposeServerAndWaitForFinishOfDisposalAsync(server);
        using var newServer = GetNewServer(new ServerCreationOptions
        {
            DeletePrevious = false,
            RunInMemory = false,
            DataDirectory = result.DataDirectory,
            CustomSettings = new Dictionary<string, string> { [RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = result.Url }
        });

        await WaitForIndexInitialization(newServer, store);

        names = store.Maintenance.Send(new GetIndexNamesOperation(0, 100));
        Assert.Contains(replacementName, names); // the replacement should still be there after restart
    }

    private async Task WaitForIndexInitialization(RavenServer server, IDocumentStore store)
    {
        if (server.ServerStore.Initialized == false)
            await server.ServerStore.InitializationCompleted.WaitAsync();

        long lastRaftIndex;
        using (server.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext ctx))
        using (ctx.OpenReadTransaction())
        {
            lastRaftIndex = server.ServerStore.Engine.GetLastCommitIndex(ctx);
        }

        var database = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
        await database.RachisLogIndexNotifications.WaitForIndexNotification(lastRaftIndex, TimeSpan.FromSeconds(15));
    }


    private class Doc
    {
        public string Id { get; set; }
        public string StrVal { get; set; }
    }

    private class Other
    {
        public string Id { get; set; }
        public string StrVal { get; set; }
    }

    private class DocIndex : AbstractIndexCreationTask<Doc>
    {
        public DocIndex()
        {
            Map = docs => from doc in docs
                select new { doc.StrVal };
        }
    }

    private class OtherIndex : AbstractIndexCreationTask<Other>
    {
        public OtherIndex()
        {
            Map = others => from other in others
                select new { other.StrVal };
        }
    }
}
