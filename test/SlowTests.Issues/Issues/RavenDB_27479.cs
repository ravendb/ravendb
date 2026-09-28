using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;
using FastTests;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Raven.Client.Documents.Conventions;
using Raven.Server.Config;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Server;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

[Collection(UnsecuredHttp2OnlyServerCollection.Name)]
public class RavenDB_27479 : RavenTestBase
{
    private readonly HttpVersionPolicy? _defaultHttpVersionPolicy = DocumentConventions.DefaultHttpVersionPolicy;

    public RavenDB_27479(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.ClientApi | RavenTestCategory.ChangesApi | RavenTestCategory.Sharding)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public async Task UnsecuredHttp2OnlyServerCanTalkToItself(Options options)
    {
        // no server conventions, so internal requests rely on the policy set by RavenServer.Initialize
        options.Server = GetNewServer(new ServerCreationOptions
        {
            CustomSettings = new Dictionary<string, string> { [RavenConfiguration.GetKey(x => x.Http.Protocols)] = HttpProtocols.Http2.ToString() }
        });

        // the client has to send h2c on its own
        options.ModifyDocumentStore = s => s.Conventions.HttpVersionPolicy = HttpVersionPolicy.RequestVersionExact;

        // database creation waits for the raft index on the server itself (/rachis/waitfor)
        using var store = GetDocumentStore(options);

        // sharded: the orchestrator talks to its shards on the same server, over HTTP and over the changes web socket
        var mre = new AsyncManualResetEvent();
        var changes = await store.Changes().EnsureConnectedNow();
        var forAllDocuments = changes.ForAllDocuments();
        forAllDocuments.Subscribe(_ => mre.Set());
        await forAllDocuments.EnsureSubscribedNow();

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new User(), "users/1");
            await session.SaveChangesAsync();
        }

        Assert.True(await mre.WaitAsync(TimeSpan.FromSeconds(15)));
    }

    public override async ValueTask DisposeAsync()
    {
        try
        {
            await base.DisposeAsync();
        }
        finally
        {
            DocumentConventions.DefaultHttpVersionPolicy = _defaultHttpVersionPolicy;
        }
    }
}

// RavenServer.Initialize sets the process-wide DocumentConventions.DefaultHttpVersionPolicy for an unsecured HTTP/2-only server,
// so tests booting one must not run next to other tests and must restore it
[CollectionDefinition(Name, DisableParallelization = true)]
public class UnsecuredHttp2OnlyServerCollection
{
    public const string Name = nameof(UnsecuredHttp2OnlyServerCollection);
}
