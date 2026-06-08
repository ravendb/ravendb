using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.ServerWide;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Documents.Replication.Incoming;
using Raven.Server.Documents.Replication.Outgoing;
using Raven.Server.ServerWide.Commands;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Replication;

public sealed class FilteredReplicationTestsPullReplicationCompositeChangeVectors : ReplicationTestBase
{
    private const string HashedRevisionPk = "HashedRevisionPk";

    public FilteredReplicationTestsPullReplicationCompositeChangeVectors(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Replication)]
    public async Task NewDatabaseEnablesPullReplicationCompositeChangeVectorsByDefault()
    {
        using var store = GetDocumentStore();

        var database = await Databases.GetDocumentDatabaseInstanceFor(store);

        Assert.True(database.SupportedFeatures.SupportedFeatureTypes.PullReplicationCompositeChangeVectors);
    }

    [RavenFact(RavenTestCategory.Replication)]
    public async Task DatabaseRecordWithoutPullReplicationCompositeChangeVectorsTokenKeepsRuntimeFeatureOff()
    {
        using var store = GetDocumentStore(new Options { ModifyDatabaseRecord = StripPullReplicationCompositeChangeVectorsToken });

        var database = await Databases.GetDocumentDatabaseInstanceFor(store);

        Assert.False(database.SupportedFeatures.SupportedFeatureTypes.PullReplicationCompositeChangeVectors);
    }

    [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Setup)]
    public async Task DatabaseRecordWithoutPullReplicationCompositeChangeVectorsTokenKeepsRuntimeFeatureOffAfterServerRestart()
    {
        var databaseName = GetDatabaseName();
        var server = GetNewServer(new ServerCreationOptions
        {
            RunInMemory = false
        });

        using (var store = GetDocumentStore(new Options
        {
            Server = server,
            DeleteDatabaseOnDispose = false,
            ModifyDatabaseName = _ => databaseName,
            ModifyDatabaseRecord = StripPullReplicationCompositeChangeVectorsToken
        }))
        {
            var database = await GetDatabase(server, store.Database);
            Assert.False(database.SupportedFeatures.SupportedFeatureTypes.PullReplicationCompositeChangeVectors);
        }

        var disposedServer = await DisposeServerAndWaitForFinishOfDisposalAsync(server);

        server = await ReviveNodeAsync(disposedServer);

        var restartedDatabase = await GetDatabase(server, databaseName);
        Assert.False(restartedDatabase.SupportedFeatures.SupportedFeatureTypes.PullReplicationCompositeChangeVectors);
    }

    [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Cluster)]
    public async Task ToggleCommandUpdatesPullReplicationCompositeChangeVectorsFeatureState()
    {
        using var store = GetDocumentStore();

        var database = await Databases.GetDocumentDatabaseInstanceFor(store);
        Assert.True(database.SupportedFeatures.SupportedFeatureTypes.PullReplicationCompositeChangeVectors);

        var disableResult = await database.ServerStore.SendToLeaderAsync(
            new SetPullReplicationCompositeChangeVectorsFeatureCommand(database.Name, enabled: false, RaftIdGenerator.NewId()));
        await database.RachisLogIndexNotifications.WaitForIndexNotification(disableResult.Index, database.ServerStore.Engine.OperationTimeout);

        Assert.False(database.SupportedFeatures.SupportedFeatureTypes.PullReplicationCompositeChangeVectors);

        var enableResult = await database.ServerStore.SendToLeaderAsync(
            new SetPullReplicationCompositeChangeVectorsFeatureCommand(database.Name, enabled: true, RaftIdGenerator.NewId()));
        await database.RachisLogIndexNotifications.WaitForIndexNotification(enableResult.Index, database.ServerStore.Engine.OperationTimeout);

        Assert.True(database.SupportedFeatures.SupportedFeatureTypes.PullReplicationCompositeChangeVectors);
    }

    [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
    public async Task FilteredPullReplicationCreatesCompositeChangeVectorWhenBothDatabasesSupportFeature()
    {
        var changeVector = await ReplicateFilteredHubToSinkAndGetChangeVectorAsync(hubSupportsFeature: true, sinkSupportsFeature: true);

        Assert.Contains("|", changeVector);
    }

    [RavenTheory(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
    [RavenData(false, false, DatabaseMode = RavenDatabaseMode.Single)]
    [RavenData(true, false, DatabaseMode = RavenDatabaseMode.Single)]
    [RavenData(false, true, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task FilteredPullReplicationStaysLegacyWhenEitherDatabaseDoesNotSupportFeature(Options _, bool hubSupportsFeature, bool sinkSupportsFeature)
    {
        var changeVector = await ReplicateFilteredHubToSinkAndGetChangeVectorAsync(hubSupportsFeature, sinkSupportsFeature);

        Assert.DoesNotContain("|", changeVector);
    }

    [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
    public async Task LiveFeatureToggleReopensFilteredPullReplicationOnCompositeChangeVectorLane()
    {
        var certificates = Certificates.SetupServerAuthentication();
        using var hub = GetDocumentStore(new Options
        {
            ClientCertificate = certificates.ServerCertificateForCommunication.Value,
            AdminCertificate = certificates.ServerCertificateForCommunication.Value,
            ModifyDatabaseRecord = StripPullReplicationCompositeChangeVectorsToken
        });
        using var sink = GetDocumentStore(new Options
        {
            ClientCertificate = certificates.ServerCertificateForCommunication.Value,
            AdminCertificate = certificates.ServerCertificateForCommunication.Value,
            ModifyDatabaseRecord = StripPullReplicationCompositeChangeVectorsToken
        });

#pragma warning disable SYSLIB0057
        using var pullCert = new X509Certificate2(
            certificates.ClientCertificate1.Value.Export(X509ContentType.Pfx), (string)null,
            X509KeyStorageFlags.Exportable);
#pragma warning restore SYSLIB0057

        await SetupFilteredHubToSinkPullReplicationAsync(hub, sink, pullCert);

        await StoreTestItemAsync(hub, "items/include/before-toggle", "before-toggle");
        Assert.True(WaitForDocument(sink, "items/include/before-toggle", 30_000));

        var beforeToggleChangeVector = GetChangeVectorFor(sink, "items/include/before-toggle");
        Assert.DoesNotContain("|", beforeToggleChangeVector);

        var hubDatabase = await Databases.GetDocumentDatabaseInstanceFor(hub);

        var hubOutgoingHandler = await AssertWaitForNotNullAsync(
            () => Task.FromResult(hubDatabase.ReplicationLoader.OutgoingHandlers.OfType<OutgoingPullReplicationHandlerAsHub>().SingleOrDefault()),
            timeout: 30_000);
        Assert.False(hubOutgoingHandler.BothSidesSupportCompositeChangeVectors);

        var sinkDatabase = await Databases.GetDocumentDatabaseInstanceFor(sink);

        await SetPullReplicationCompositeChangeVectorsFeatureAsync(hubDatabase, enabled: true);
        await SetPullReplicationCompositeChangeVectorsFeatureAsync(sinkDatabase, enabled: true);

        await AssertWaitForTrueAsync(
            () => Task.FromResult(hubDatabase.ReplicationLoader.OutgoingHandlers.OfType<OutgoingPullReplicationHandlerAsHub>().Any(x => x.BothSidesSupportCompositeChangeVectors)),
            timeout: 30_000);

        await StoreTestItemAsync(hub, "items/include/after-toggle", "after-toggle");
        Assert.True(WaitForDocument(sink, "items/include/after-toggle", 30_000));

        var afterToggleChangeVector = GetChangeVectorFor(sink, "items/include/after-toggle");
        Assert.Contains("|", afterToggleChangeVector);
    }

    private async Task<string> ReplicateFilteredHubToSinkAndGetChangeVectorAsync(bool hubSupportsFeature, bool sinkSupportsFeature)
    {
        var certificates = Certificates.SetupServerAuthentication();
        using var hub = GetDocumentStore(new Options
        {
            ClientCertificate = certificates.ServerCertificateForCommunication.Value,
            AdminCertificate = certificates.ServerCertificateForCommunication.Value,
            ModifyDatabaseRecord = hubSupportsFeature ? null : StripPullReplicationCompositeChangeVectorsToken
        });
        using var sink = GetDocumentStore(new Options
        {
            ClientCertificate = certificates.ServerCertificateForCommunication.Value,
            AdminCertificate = certificates.ServerCertificateForCommunication.Value,
            ModifyDatabaseRecord = sinkSupportsFeature ? null : StripPullReplicationCompositeChangeVectorsToken
        });

#pragma warning disable SYSLIB0057
        using var pullCert = new X509Certificate2(
            certificates.ClientCertificate1.Value.Export(X509ContentType.Pfx), (string)null,
            X509KeyStorageFlags.Exportable);
#pragma warning restore SYSLIB0057

        await SetupFilteredHubToSinkPullReplicationAsync(hub, sink, pullCert);

        using (var session = hub.OpenAsyncSession())
        {
            await session.StoreAsync(new TestItem { Name = "included" }, "items/include/1");
            await session.StoreAsync(new TestItem { Name = "excluded" }, "items/exclude/1");
            await session.SaveChangesAsync();
        }

        Assert.True(WaitForDocument(sink, "items/include/1", 30_000));

        return GetChangeVectorFor(sink, "items/include/1");
    }

    private static async Task SetupFilteredHubToSinkPullReplicationAsync(DocumentStore hub, DocumentStore sink, X509Certificate2 pullCert)
    {
        const string pullName = "filtered";

        await hub.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition
        {
            Name = pullName,
            Mode = PullReplicationMode.HubToSink,
            WithFiltering = true
        }));

        await hub.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(pullName,
            new ReplicationHubAccess
            {
                Name = "SinkAccess",
                CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert)),
                AllowedHubToSinkPaths = ["items/include/*"]
            }));

        await sink.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
        {
            Database = hub.Database,
            Name = "HubConnection",
            TopologyDiscoveryUrls = hub.Urls
        }));

        await sink.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(new PullReplicationAsSink
        {
            ConnectionStringName = "HubConnection",
            CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx)),
            HubName = pullName,
            Mode = PullReplicationMode.HubToSink
        }));
    }

    private static async Task StoreTestItemAsync(DocumentStore store, string id, string name)
    {
        using var session = store.OpenAsyncSession();
        await session.StoreAsync(new TestItem { Name = name }, id);
        await session.SaveChangesAsync();
    }

    private static string GetChangeVectorFor(DocumentStore store, string id)
    {
        using var session = store.OpenSession();
        var item = session.Load<TestItem>(id);
        Assert.NotNull(item);
        return session.Advanced.GetChangeVectorFor(item);
    }

    private static async Task SetPullReplicationCompositeChangeVectorsFeatureAsync(DocumentDatabase database, bool enabled)
    {
        var result = await database.ServerStore.SendToLeaderAsync(
            new SetPullReplicationCompositeChangeVectorsFeatureCommand(database.Name, enabled, RaftIdGenerator.NewId()));
        await database.RachisLogIndexNotifications.WaitForIndexNotification(result.Index, database.ServerStore.Engine.OperationTimeout);

        Assert.Equal(enabled, database.SupportedFeatures.SupportedFeatureTypes.PullReplicationCompositeChangeVectors);
    }

    private static void StripPullReplicationCompositeChangeVectorsToken(DatabaseRecord record)
    {
        record.SupportedFeatures = new List<string>
        {
            HashedRevisionPk
        };
    }

    private sealed class TestItem
    {
        public string Name { get; set; }
    }
}
